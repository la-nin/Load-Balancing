package lb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProxyServer {
    private static final int DEFAULT_PROXY_PORT = 8080;
    private static final int PROXY_THREADS = 20;

    private static final List<Backend> backends = new ArrayList<>();
    private static int nextBackendIndex = 0;

    public static void main(String[] args) throws IOException {
        int proxyPort = DEFAULT_PROXY_PORT;

        backends.add(new Backend("localhost", 8081));
        backends.add(new Backend("localhost", 8082));
        backends.add(new Backend("localhost", 8083));

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
                    sendResponse(clientOutput, 200, "OK", "OK");
                } else if (path.equals("/work")) {
                    forwardToBackend(clientOutput, path);
                } else {
                    sendResponse(clientOutput, 404, "Not Found", "NotFound");
                }

        }catch (IOException e){
            System.out.println("Proxy server failed " + e.getMessage() );
        }
    }

    private static void forwardToBackend(OutputStream clientOutput, String path) throws IOException {
        Backend backend = chooseRoundRobin();

        System.out.println("Forward to backend: " + path + "to " + backend.host + " port: " + backend.port);

        try (Socket backendSocket = new Socket(backend.host, backend.port);
             BufferedReader reader = new BufferedReader(new InputStreamReader(backendSocket.getInputStream(), StandardCharsets.UTF_8));
             OutputStream backendOutput =  backendSocket.getOutputStream() ) {

            String RequestLine =
                    "GET " + path + " HTTP/1.1\r\n" +
                    "Host: " + backend.host + ":" + backend.port + "\r\n" +
                    "Connection: close\r\n" +
                    "\r\n";

            backendOutput.write(RequestLine.getBytes(StandardCharsets.UTF_8));
            backendOutput.flush();

            String line;
            while ((line = reader.readLine()) != null) {
                clientOutput.write((line + "\r\n").getBytes(StandardCharsets.UTF_8));
            }

            clientOutput.flush();
        }
    }

    private static synchronized Backend chooseRoundRobin() {
        Backend backend = backends.get(nextBackendIndex);
        nextBackendIndex = (nextBackendIndex + 1) % backends.size();
        return backend;
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
        private Backend(String host, int port) {
            this.host = host;
            this.port = port;
        }

    }

}


