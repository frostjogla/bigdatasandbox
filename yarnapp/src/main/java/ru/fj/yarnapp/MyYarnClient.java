package ru.fj.yarnapp;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class MyYarnClient {

    Configuration conf = new YarnConfiguration();

    public void run(String[] args) throws YarnException, IOException, InterruptedException {

        Path jarPath = FileSystem.get(conf).makeQualified(new Path(args[0]));

        // Create yarnClient
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        System.out.println("init");
        yarnClient.start();
        System.out.println("start");

        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();
        System.out.println("createApplication");

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainerLaunchCtx = Records.newRecord(ContainerLaunchContext.class);

        amContainerLaunchCtx.setCommands(Collections
                .singletonList("java ru.fj.yarnapp.MyAppMaster" + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                        + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
        System.out.println("setCommands");

        // Setup jar for ApplicationMaster
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        setupAppMasterJar(jarPath, appMasterJar);
        System.out.println("setupAppMasterJar");
        amContainerLaunchCtx.setLocalResources(Collections.singletonMap("appmaster.jar", appMasterJar));

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        amContainerLaunchCtx.setEnvironment(appMasterEnv);
        System.out.println("setEnvironment(appMasterEnv)");

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(512);
        capability.setVirtualCores(2);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName("simple-app-master"); // application name
        appContext.setAMContainerSpec(amContainerLaunchCtx);
        appContext.setResource(capability);
        appContext.setQueue("default"); // queue

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);
        System.out.println("submitApplication(appContext)");

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED
                && appState != YarnApplicationState.FAILED) {
            Thread.sleep(400);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
            System.out.println("[CLIENT] status: " + appState);
        }

        System.out.println(
                "Application " + appId + " finished with" + " state " + appState + " at " + appReport.getFinishTime());

    }

    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        Path makeQualified = fileSystem.makeQualified(jarPath);
        FileStatus jarStat = fileSystem.getFileStatus(makeQualified);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(makeQualified));
        appMasterJar.setShouldBeUploadedToSharedCache(true);
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            System.out.println("[CLIENT] classpath: " + c);
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim()/* , File.separator */);
        }
        System.out.println("[CLIENT] classpath: " + Environment.PWD.$() + File.separator + "*");
        Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*"/*
                                                           * , File.separator
                                                           */);
    }

    public static void main(String[] args) throws Exception {
        MyYarnClient c = new MyYarnClient();
        c.run(args);
    }
}
