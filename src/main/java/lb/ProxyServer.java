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

public class ProxyServer {
    private static final int DEFAULT_PROXY_PORT = 8080;
    private static final int PROXY_THREADS = 20;
    private static final int HEALTH_CHECK_MS = 1000;
    private static final int HEALTH_CHECK_TIMEOUT_MS = 500;

    private static final List<Backend> backends = new ArrayList<>();
    private static int nextBackendIndex = 0;

    public static void main(String[] args) throws IOException {
        int proxyPort = DEFAULT_PROXY_PORT;

        backends.add(new Backend("localhost", 8081));
        backends.add(new Backend("localhost", 8082));
        backends.add(new Backend("localhost", 8083));

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

        for (Backend backend : backends) {
            status.append(backend.port + " ")
                    .append(backend.healthy ? "UP" : "DOWN")
                    .append("\r\n");
        }

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
        Backend backend = chooseHealthyRoundRobin();

        if (backend == null) {
            sendResponse(clientOutput, 503, "No healthy workers", "No workers are alive");
            return;
        }

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
        }
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

        // SHOULD I WRITE AGAIN ALL THESE FUNCTIONS OR REUSE THEM (LIKE sendResponse or handleClient)


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
        private final String host;
        private final int port;
        private volatile boolean healthy; //when a change happens in one thread it should let the other threads know to update and not use an old value
        private Backend(String host, int port) {
            this.host = host;
            this.port = port;
            this.healthy = false;
        }

    }
}


