package lb;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Benchmark {
    private static final String DEFAULT_URL = "http://localhost:8080/work";
    private static final int DEFAULT_CONCURRENCY = 1;
    private static final int DEFAULT_REQUESTS_PER_CLIENT = 1000;

    public static void main(String[] args) throws InterruptedException {
        String url = args.length > 0 ? args[0] : DEFAULT_URL;
        int concurrency = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_CONCURRENCY;
        int requestsPerClient = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_REQUESTS_PER_CLIENT;

        int totalRequests = concurrency * requestsPerClient;

        System.out.println("Benchmark: " + url);
        System.out.println("Concurrency: " + concurrency);
        System.out.println("Requests per client: " + requestsPerClient);
        System.out.println("Total requests: " + totalRequests);

        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        ExecutorService  executor = Executors.newFixedThreadPool(concurrency);

        AtomicInteger successfullReq = new AtomicInteger(0);
        AtomicInteger failedReq = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        List<Future<?>> futures = new ArrayList<>();

        for (int clientId = 1; clientId <= concurrency; clientId++){
            int currentClientId = clientId;

            Future<?> future = executor.submit(() -> {
                runClient(
                        currentClientId,
                        httpClient,
                        url,
                        requestsPerClient,
                        successfullReq,
                        failedReq
                );
            });

            futures.add(future);

        }

        for (Future<?> future : futures) {
            try {
                future.get();
            }catch (ExecutionException e){
                failedReq.incrementAndGet();
                System.out.println("Client failed " + e.getMessage());
            }
        }

        long endTime = System.currentTimeMillis();

        executor.shutdown();

        double durationSeconds= (endTime - startTime) / 1000.0;
        double throughput = totalRequests / durationSeconds;

        System.out.println("Benchmark finished: ");
        System.out.println("Successful requests: " +  successfullReq.get());
        System.out.println("Failed requests: " +  failedReq.get());
        System.out.println("Duration: " +  durationSeconds + " s");
        System.out.println("Throughput: " +  throughput);
    }

    private static void runClient(
            int clientId,
            HttpClient httpClient,
            String url,
            int requestsPerClient,
            AtomicInteger successfullReq,
            AtomicInteger failedReq
    ) {
        for (int i = 1; i <= requestsPerClient ; i++) {
            boolean success = sendWorkReq(httpClient, url);

            if (success) {
                successfullReq.incrementAndGet();
            } else {
                failedReq.incrementAndGet();
            }

            if (i % 100 == 0) {
                System.out.println("Client id: "+ clientId+ " completed " + successfullReq.get());
            }
        }
    }

    private static boolean sendWorkReq(HttpClient httpClient, String url) {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .GET()
            .timeout(Duration.ofSeconds(30))
            .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            return response.statusCode() == 200;

        } catch (IOException | InterruptedException e  ) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
