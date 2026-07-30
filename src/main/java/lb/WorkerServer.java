package lb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkerServer {
    private static final int DEFAULT_PORT = 8081;
    private static final int THREADS = 3;

    private static final Random RAND = new Random();

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        ExecutorService threadPool = Executors.newFixedThreadPool(THREADS);

        try(ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Worker listening on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();

                threadPool.submit(() -> {
                    handleClient(clientSocket);
                });
            }
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (clientSocket;
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
             OutputStream output = clientSocket.getOutputStream()) {

            String requestLine = reader.readLine();

            if (requestLine == null || requestLine.isBlank()) {
                sendResponse(output, 400, "Bad Request", "Bad Request");
                return;
            }

            System.out.println("Request: " + requestLine);

            String[] parts = requestLine.split(" ");

            if (parts.length != THREADS) {
                sendResponse(output, 400, "Bad Request", "Bad Request");
                return;
            }

            String method = parts[0];
            String path = parts[1];
            String version = parts[2];

            if (!method.equals("GET") || !version.equals("HTTP/1.1"))
             {
                 sendResponse(output, 400, "Bad Request", "Bad Request");
                 return;
             }

            if(path.equals("/health")) {
                sendResponse(output, 200, "OK", "OK");
                return;
            } else if(path.equals("/work")) {
                handleWork(output);
            }else {
                sendResponse(output, 404, "Not Found", "Not Found" );
            }

        }catch (IOException e){
            System.out.println("Falied to send request" + e.getMessage());
        }
    }

    public static void handleWork(OutputStream output) throws IOException {
        int n = RAND.nextInt(50_000_000) + 1;

        long sum = 0;

        for (int i = 1; i <= n; i++) {
            sum += i;
        }

        String body = "N= " + n + "\nSum = " + sum + "\n";

        sendResponse(output, 200, "OK", body);
    }

    public static void sendResponse(OutputStream output, int status, String statusText, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

        String response =
                "HTTP/1.1 " + status + " " + statusText + "\r\n" +
                "Content-Type: text/plain\r\n" +
                "Content-Length: " + bytes.length + "\r\n" +
                "Connection: close\r\n" +
                "\r\n";

        output.write(response.getBytes(StandardCharsets.UTF_8));
        output.write(bytes);
        output.flush();
    }
}
