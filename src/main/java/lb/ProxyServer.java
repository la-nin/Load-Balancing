package lb;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyServer {
    private static final int DEFAULT_PROXY_PORT = 8080;
    private static final int PROXY_THREADS = 20;
    private static final int HEALTH_CHECK_MS = 1000;
    private static final int HEALTH_CHECK_TIMEOUT_MS = 500;

    private static final List<Backend> backends = new ArrayList<>();
    private static int nextBackendIndex = 0;

    private enum Policy {
        ROUND_ROBIN,
        LEAST_CONN,
        RANDOM
    }

    private static Policy policy = Policy.ROUND_ROBIN;

    public static void main(String[] args) throws IOException {
        int proxyPort = DEFAULT_PROXY_PORT;

        if (args.length > 0) {
            policy = parsePolicy(args[0]);
        }

        System.out.println("Load balancing policy: " + policy);

        backends.add(new Backend("worker1","worker1", 8081));
        backends.add(new Backend("worker2","worker2", 8081));
        backends.add(new Backend("worker3","worker3", 8081));

        startHealthChecks();

        ExecutorService threadPool = Executors.newFixedThreadPool(PROXY_THREADS);

        try (ServerSocket serverSocket = new ServerSocket(proxyPort)) {
            System.out.println("Proxy Server port: " + proxyPort);
            for (Backend backend : backends ) {
                System.out.println(backend.host + ":" + backend.port);
            }

            while (true) {
                Socket clientSocket =  serverSocket.accept();

                threadPool.submit(() -> {
                    handleClient(clientSocket);
                });
            }
        }
    }

    private static Policy parsePolicy(String value) {
        return switch (value.toLowerCase()) {
            case "round_robin" -> Policy.ROUND_ROBIN;
            case "least_conn" -> Policy.LEAST_CONN;
            case "random" -> Policy.RANDOM;
            default -> {
                System.out.println("No policy specified: " + value + " choose round robin");
                yield Policy.ROUND_ROBIN;
            }
        };
    }
    private static void startHealthChecks() {
        Thread healthCheckThread = new Thread(() -> {
            while (true) {
                for(Backend backend : backends) {
                    boolean healthy = checkHealth(backend);
                    backend.healthy = healthy;
                }

                printHealthStatus();

                try {
                    Thread.sleep(HEALTH_CHECK_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });

        healthCheckThread.setDaemon(true);
        healthCheckThread.start();
    }

    private static boolean checkHealth(Backend backend) {
        try (Socket socket = new Socket(backend.host, backend.port)) {
            socket.setSoTimeout(HEALTH_CHECK_TIMEOUT_MS);

            OutputStream output = socket.getOutputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            String request =
                    "GET /health HTTP/1.1\r\n" +
                            "Host: " + backend.host + ":" + backend.port + "\r\n" +
                            "Connection: close\r\n" +
                            "\r\n";

            output.write(request.getBytes(StandardCharsets.UTF_8));
            output.flush();

            String statusLine = reader.readLine();

            return statusLine != null && statusLine.equals("HTTP/1.1 200 OK");

        } catch (IOException e) {
            return false;
        }
    }

    private static void printHealthStatus() {
        StringBuilder status = new StringBuilder("Health: \r\n");
        int healthyCount = 0;


        for (Backend backend : backends) {
            if (backend.healthy) {
                healthyCount++;
            }


            status.append(backend.name)
                    .append(" ")
                    .append(backend.port)
                    .append(" ")
                    .append(backend.host)
                    .append(" ")
                    .append(backend.healthy ? "UP" : "DOWN");
            status.append("\r\n");
        }

        status.append("healthy workers: ")
                .append(healthyCount)
                .append("/")
                .append(backends.size());

        System.out.println(status);
    }

    private static void  handleClient(Socket clientSocket) {
        try(clientSocket;
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
            OutputStream clientOutput = clientSocket.getOutputStream()) {

                String RequestLine = reader.readLine();

                if (RequestLine == null || RequestLine.isBlank()){
                    sendResponse(clientOutput, 400, "Bad Request", "Bad Request");
                    return;
                }

                System.out.println("Received Request: " + RequestLine);

                String[] parts = RequestLine.split(" ");

                if(parts.length != 3) {
                    sendResponse(clientOutput, 400, "Bad Request", "Bad Request");
                    return;
                }

                String method = parts[0];
                String path = parts[1];
                String version = parts[2];

                if(!method.equals("GET") || !version.equals("HTTP/1.1")) {
                    sendResponse(clientOutput, 400, "Bad Request", "Bad Request");
                    return;
                }

                if (path.equals("/health")) {
                    handleProxyHealth(clientOutput);
                } else if (path.equals("/work")) {
                    forwardToBackend(clientOutput, path);
                } else {
                    sendResponse(clientOutput, 404, "Not Found", "NotFound");
                }

        }catch (IOException e){
            System.out.println("Proxy server failed " + e.getMessage() );
        }
    }

    private static void handleProxyHealth(OutputStream clientOutput) throws IOException {
        if(hasHealthyBackend()) {
            sendResponse(clientOutput, 200, "OK", "OK");
        }else {
            sendResponse(clientOutput, 503, "No healthy workers", "No workers are alive");
        }
    }

    private static boolean hasHealthyBackend() {
        for (Backend backend : backends) {
                if (backend.healthy) {
                    return true;
                }
            }

            return false;
        }


    private static void forwardToBackend(OutputStream clientOutput, String path) throws IOException {
        Backend backend = chooseBackend();

        if (backend == null) {
            sendResponse(clientOutput, 503, "No healthy workers", "No workers are alive");
            return;
        }

        backend.inFlightRequests.incrementAndGet();

        System.out.println("Forward to backend: " + path + "to " + backend.host + " port: " + backend.port);

        try (Socket backendSocket = new Socket(backend.host, backend.port)) {
            backendSocket.setSoTimeout(10000);

             OutputStream backendOutput =  backendSocket.getOutputStream();
             InputStream backendInput = backendSocket.getInputStream();

             String RequestLine =
                     "GET " + path + " HTTP/1.1\r\n" +
                     "Host: " + backend.host + ":" + backend.port + "\r\n" +
                     "Connection: close\r\n" +
                     "\r\n";

             backendOutput.write(RequestLine.getBytes(StandardCharsets.UTF_8));
             backendOutput.flush();

             byte[] responseBytes = new byte[4096];
             int bytesRead;

             while ((bytesRead = backendInput.read(responseBytes)) != -1) {
                 clientOutput.write(responseBytes, 0, bytesRead);
             }

             clientOutput.flush();

        } catch (SocketTimeoutException e) {
            sendResponse(clientOutput, 504, "Timeout", "Backend Timeout");
        } catch (IOException e) {
            backend.healthy = false;
            sendResponse(clientOutput, 502, "Backend request failed", "Backend request failed");
        }finally {
            backend.inFlightRequests.decrementAndGet();
        }
    }

    private static Backend chooseBackend () {
        return switch (policy) {
            case ROUND_ROBIN -> chooseHealthyRoundRobin();
            case LEAST_CONN -> chooseHealthyLeastConn();
            case RANDOM -> chooseRandom();
        };
    }

    private static synchronized Backend chooseHealthyLeastConn() {
        Backend bestBackend = null;

        for (Backend backend : backends) {
            if (!backend.healthy) {
                continue;
            }

            if (bestBackend == null || backend.inFlightRequests.get() < bestBackend.inFlightRequests.get()) {
                bestBackend = backend;
            }
        }

        return bestBackend;
    }

    private static synchronized Backend chooseRandom() {

        List<Backend> healthyBackends = new ArrayList<>();

        for (Backend backend : backends) {
            if (backend.healthy) {
                healthyBackends.add(backend);
            }
        }

        if(healthyBackends.isEmpty()) {
            return null;
        }

        int index = ThreadLocalRandom.current().nextInt(healthyBackends.size());
        return healthyBackends.get(index);
    }



    private static synchronized Backend chooseHealthyRoundRobin() {
        if (!hasHealthyBackend()) {
            return null;
        }

        int checkedBackends = 0;

        while (checkedBackends < backends.size()) {
            Backend backend = backends.get(nextBackendIndex);
            nextBackendIndex = (nextBackendIndex + 1) % backends.size();

            if(backend.healthy) {
                return backend;
            }

            checkedBackends++;
        }

        return null;
    }


        private static void sendResponse(
                OutputStream output,
                int statusCode,
                String statusText,
                String body
        ) throws IOException {
            byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);

            String response =
                    "HTTP/1.1 " + statusCode + " " + statusText + "\r\n" +
                            "Content-Type: text/plain\r\n" +
                            "Content-Length: " + bodyBytes.length + "\r\n" +
                            "Connection: close\r\n" +
                            "\r\n";

            output.write(response.getBytes(StandardCharsets.UTF_8));
            output.write(bodyBytes);
            output.flush();
        }

    private static class Backend {
        private final String name;
        private final String host;
        private final int port;
        private volatile boolean healthy; //when a change happens in one thread it should let the other threads know to update and not use an old value
        private final AtomicInteger inFlightRequests;
        private Backend(String name, String host, int port) {
            this.name = name;
            this.host = host;
            this.port = port;
            this.healthy = false;
            this.inFlightRequests = new AtomicInteger(0);
        }

    }
}


