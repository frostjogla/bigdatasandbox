package ru.fj.yarnapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class EmbeddedJettyStarter {

    private Server server;
    private CallbackHandler appMasterHandler;
    private AMRMClientAsync<ContainerRequest> rmClient;
    private YarnConfiguration configuration;

    public EmbeddedJettyStarter() {
        this.configuration = new YarnConfiguration();
        this.appMasterHandler = new MyAppMasterAsync(configuration);
        this.rmClient = AMRMClientAsync.createAMRMClientAsync(200, appMasterHandler);
        this.rmClient.init(configuration);
        this.server = new Server(1235);

        ServletContextHandler contextHandler = new ServletContextHandler(server, "/appmaster",
                ServletContextHandler.NO_SESSIONS);
        contextHandler.addServlet(new ServletHolder(new AppMasterServlet()), "/default");
        contextHandler.addServlet(new ServletHolder(new ConfigAppServlet(this.rmClient)), "/config");
        contextHandler.addServlet(new ServletHolder(new AppMasterStopper(this.rmClient, this.server)),
                "/stopappmaster");
        contextHandler.setServer(server);
    }

    public void start() throws Exception {
        rmClient.start();
        server.start();

        // Register with ResourceManager
        System.out.println("[AM] registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("[AM] registerApplicationMaster 1");

        server.join();
    }

    private static class AppMasterServlet extends HttpServlet {

        private static final long serialVersionUID = 1L;

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            resp.setContentType("text/html");
            PrintWriter writer = resp.getWriter();
            writer.println("<h1>Hello, I am App Master</h1>");
            resp.setStatus(HttpServletResponse.SC_OK);
        }
    }

    private static class ConfigAppServlet extends HttpServlet {

        private static final String CONTAINERS = "containers";

        private static final long serialVersionUID = 1L;

        private static final String PRIORITY = "priority";
        private static final String MEMORY = "memory";
        private static final String CPU = "cpu";

        private final AMRMClientAsync<ContainerRequest> rmClient;

        public ConfigAppServlet(AMRMClientAsync<ContainerRequest> rmClient) {
            this.rmClient = rmClient;
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            printResponse(resp, "");
            resp.setStatus(HttpServletResponse.SC_OK);
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            String cpuValue = req.getParameter(CPU);
            String memoryValue = req.getParameter(MEMORY);
            String priorityValue = req.getParameter(PRIORITY);
            String containersValue = req.getParameter(CONTAINERS);
            if (cpuValue == null || cpuValue.isEmpty()) {
                cpuValue = "1";
            }
            if (memoryValue == null || memoryValue.isEmpty()) {
                memoryValue = "256";
            }
            if (priorityValue == null || priorityValue.isEmpty()) {
                priorityValue = "0";
            }
            if (containersValue == null || containersValue.isEmpty()) {
                containersValue = "1";
            }
            System.out.println("cpu = " + cpuValue);
            System.out.println("memory = " + memoryValue);
            System.out.println("priority = " + priorityValue);
            System.out.println("containers = " + containersValue);

            // Priority for worker containers - priorities are intra-application
            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(Integer.valueOf(priorityValue));

            // Resource requirements for worker containers
            Resource capability = Records.newRecord(Resource.class);
            capability.setMemory(Integer.valueOf(memoryValue));
            capability.setVirtualCores(Integer.valueOf(cpuValue));

            int numContainers = Integer.valueOf(containersValue);

            for (int i = 0; i < numContainers; i++) {
                ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
                rmClient.addContainerRequest(containerAsk);
            }
            printResponse(resp, "Container requsted" + System.currentTimeMillis());
        }

        private void printResponse(HttpServletResponse resp, String string) throws IOException {
            resp.setContentType("text/html");
            PrintWriter writer = resp.getWriter();
            writer.println("<html>");
            writer.println("<head>");
            writer.println("<title>Application configuration</title>");
            writer.println("</head>");
            writer.println("<body>");
            writer.println("<h1>Application configuration</h1>");
            writer.println("<form id='form_id'  method='post'>");
            writer.println("<p>Memory: <input name='memory' placeholder='256' type='text'></input></p>");
            writer.println("<p>CPU: <input name='cpu' placeholder='1' type='text'></input></p>");
            writer.println("<p>Priority: <input name='priority' placeholder='0' type='text'></input></p>");
            writer.println("<p>Containers: <input name='containers' placeholder='1' type='text'></input></p>");
            writer.println("<p><input type='submit' value='Start application'></input></p>");
            writer.println("</form>");
            writer.println("<form method='post' action='stopappmaster'>");
            writer.println("<p><input type='submit' value='Stop AppMaster'></input></p>");
            writer.println("</form>");
            if (string != null && !string.isEmpty()) {
                writer.println("<p>");
                writer.println(string);
                writer.println("</p>");
            }
            writer.println("</body>");
            writer.println("</html>");
        }
    }

    private static final class AppMasterStopper extends HttpServlet {

        private static final long serialVersionUID = 1L;

        private final AMRMClientAsync<ContainerRequest> rmClient;
        private final Server server;

        public AppMasterStopper(AMRMClientAsync<ContainerRequest> rmClient, Server server) {
            this.rmClient = rmClient;
            this.server = server;
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
            singleThreadExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        System.out.println("[AM] unregisterApplicationMaster 0");
                        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
                        System.out.println("[AM] unregisterApplicationMaster 1");
                        rmClient.close();
                    } catch (Exception e) {
                        System.err.println("[AM] Can't close RMClient");
                        e.printStackTrace(System.err);
                    }
                    try {
                        server.stop();
                    } catch (Exception e) {
                        System.err.println("[AM] Can't stop Jetty");
                        e.printStackTrace(System.err);
                    }
                    System.out.println("[AM] AppMaster is stopped");
                }
            });
            singleThreadExecutor.shutdown();
        }
    }
}
