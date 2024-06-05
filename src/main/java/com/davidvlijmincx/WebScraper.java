package com.davidvlijmincx;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class WebScraper {

    public static void main(String[] args) {
        final var queue = new LinkedBlockingQueue<String>(2000);
        Set<String> visited = ConcurrentHashMap.newKeySet(3000);

        // Configurations
        final var PLATFORM_THREAD_FACTORY = Thread.ofPlatform().factory();
        final var VIRTUAL_THREAD_FACTORY = Thread.ofVirtual().factory();
        final var THREAD_FACTORY = VIRTUAL_THREAD_FACTORY;

        //final var NUM_DELAYED_URLS = 500;
        //final var NUM_REGULAR_URLS = 500;

        //final var NUM_TASKS = NUM_DELAYED_URLS + NUM_REGULAR_URLS;
        final var NUM_TASKS = 100;

       // System.out.println("Configuration.\r\nThreads Factory: " + THREAD_FACTORY.toString()+"\r\n"+"Delayed URLs:"+NUM_DELAYED_URLS+"\r\n"+"Regular URLs:"+NUM_REGULAR_URLS+"\r\n"+"Total Tasks:"+NUM_TASKS);
        System.out.println("Configuration.\r\nThreads Factory: " + THREAD_FACTORY.toString()+"\r\n"+"Total Tasks:"+NUM_TASKS);

        // Prepare URLs to be crawled
        queue.add("http://localhost:8080/v1/crawl/delay/330/57");
/*        for(int i=0; i!=NUM_DELAYED_URLS; ++i) {
            //var delay = (int)(Math.random() * 1000);
            var delay = 300;
            queue.add("http://localhost:8080/v1/crawl/delay/" + delay + "/" + i);
        }
        for(int i=0; i!=NUM_REGULAR_URLS; ++i) {
            queue.add("http://localhost:8080/v1/crawl/" + i);
        }*/


        long startTime = System.currentTimeMillis();

        // // Without Threads
        //new Scrape(queue, visited).scrape();


        // // 2 Approaches
        // 1. Using Runnable
        //var thread = new Thread(task);
        //thread.start();

        // 2. Using ExecutorService
        // Note: If we use try with resources, we don't need to call shutdown nor awaitTermination
        //try(var executor = Executors.newFixedThreadPool(
        //        Runtime.getRuntime().availableProcessors())) {
        try(var executor = Executors.newThreadPerTaskExecutor(THREAD_FACTORY)) { // or newVirtualThreadPerTaskExecutor()
            for(int i = 0; i!=NUM_TASKS; ++i) {
                var task = new Scrape(queue, visited);
                executor.submit(task);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        measureTime(startTime, visited);

    }

    private static void measureTime(long startTime, Set<String> visited) {
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        double totalTimeInSeconds = totalTime / 1000.0;

        System.out.printf("Crawled %s web page(s)", visited.size());
        System.out.println("Total execution time: " + totalTime + "ms");

        double throughput = visited.size() / totalTimeInSeconds;
        System.out.println("Throughput: " + throughput + " pages/sec");
    }

}

class Scrape implements Runnable {

    private final LinkedBlockingQueue<String> pageQueue;

    private final Set<String> visited;

    private final HttpClient client;

    public Scrape(LinkedBlockingQueue<String> pageQueue, Set<String> visited) {
        this.pageQueue = pageQueue;
        this.visited = visited;
        this.client = createHttpClient();
    }

    private static HttpClient createHttpClient() {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();
    }

    private String getBody(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder().GET().uri(URI.create(url)).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    public void scrape() {

        try {
            String url = pageQueue.take();

            // // Problematic line that results in Virtual Threads being pinned
            //Document document = Jsoup.connect(url).get();

            Document document = Jsoup.parse(getBody(url));
            Elements linksOnPage = document.select("a[href]");

/*            Thread.startVirtualThread(new Runnable() {
                @Override
                public void run() {
                    visited.add(url);
                }
            });

            Thread.startVirtualThread(new Runnable() {
                @Override
                public void run() {
                    for (Element link : linksOnPage) {
                        String nextUrl = link.attr("abs:href");
                        if (nextUrl.contains("http")) {
                            pageQueue.add(nextUrl);
                        }
                    }
                }
            });*/

            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

                executor.submit(() -> visited.add(url));
                executor.submit(() -> {
                    for (Element link : linksOnPage) {
                        String nextUrl = link.attr("abs:href");
                        if (nextUrl.contains("http")) {
                            pageQueue.add(nextUrl);
                        }
                    }
                });
            }

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void run() {
        this.scrape();
    }
}