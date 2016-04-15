package ru.fj.yarnapp;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class MyAppMasterAsync implements AMRMClientAsync.CallbackHandler {

    private YarnConfiguration configuration;
    private NMClient nmClient;

    public MyAppMasterAsync(YarnConfiguration yarnConfig) {
        this.nmClient = NMClient.createNMClient();
        this.configuration = yarnConfig;
        this.nmClient.init(configuration);
        this.nmClient.start();
        System.out.println("[AM] NMClient started: " + nmClient.getName());
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus containerStatus : statuses) {
            System.out.println("[AM] Container completed: " + containerStatus.getContainerId() + " state = "
                    + containerStatus.getState() + " diagnostig = " + containerStatus.getDiagnostics()
                    + " exitStatus = " + containerStatus.getExitStatus());
        }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            System.out.println("[AM] try to launch container:" + container + " " + nmClient.getName());
            try {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

                // Setup jar for Application
                Path jarPath = new Path(
                        "hdfs://sandbox.hortonworks.com:8020/hw5/ru.fj.yarnapp-0.0.1-SNAPSHOT-jar-with-dependencies.jar");
                ctx.setCommands(Collections.singletonList(
                        "java ru.fj.yarnapp.Calculator" + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                                + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
                System.out.println("[AM] Launching container " + container.getId());

                // Setup jar for Application
                LocalResource appJarResources = Records.newRecord(LocalResource.class);
                setupAppJar(jarPath, appJarResources);
                System.out.println("[AM] setupAppJar");
                ctx.setLocalResources(Collections.singletonMap("calculator.jar", appJarResources));

                // Setup CLASSPATH for Application
                Map<String, String> appEnv = new HashMap<String, String>();
                setupAppEnv(appEnv);
                ctx.setEnvironment(appEnv);
                System.out.println("[AM] setEnvironment(appEnv)");

                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    private void setupAppJar(Path jarPath, LocalResource appJarResources) throws IOException {
        FileStatus jarStat = FileSystem.get(configuration).getFileStatus(jarPath);
        appJarResources.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appJarResources.setSize(jarStat.getLen());
        appJarResources.setTimestamp(jarStat.getModificationTime());
        appJarResources.setType(LocalResourceType.FILE);
        appJarResources.setShouldBeUploadedToSharedCache(true);
        appJarResources.setVisibility(LocalResourceVisibility.APPLICATION);
    }

    private void setupAppEnv(Map<String, String> appMasterEnv) {
        for (String c : configuration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            System.out.println("[AM] classpath: " + c);
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim()/* , File.separator */);
        }
        System.out.println("[AM] classpath: " + Environment.PWD.$() + File.separator + "*");
        Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*"/*
                                                           * , File.separator
                                                           */);
    }

    @Override
    public void onShutdownRequest() {
        System.out.println("[AM] onShutdownRequest()");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        for (NodeReport nodeReport : updatedNodes) {
            System.out.println("[AM] Node updated: " + nodeReport.getHealthReport());
        }
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {
        e.printStackTrace(System.err);
    }
}
