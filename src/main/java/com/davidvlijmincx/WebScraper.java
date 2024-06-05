package com.davidvlijmincx;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class WebScraper {

    public static void main(String[] args) {
        final var queue = new LinkedBlockingQueue<String>(2000);
        Set<String> visited = ConcurrentHashMap.newKeySet(3000);

        queue.add("http://localhost:8080/v1/crawl/delay/330/57");

        long startTime = System.currentTimeMillis();

        // // Without Threads
        //new Scrape(queue, visited).scrape();

        var task = new Scrape(queue, visited);

        // // 2 Approaches
        // 1. Using Runnable
        //var thread = new Thread(task);
        //thread.start();

        // 2. Using ExecutorService
        // Note: If we use try with resources, we don't need to call shutdown nor awaitTermination
        //try(var executor = Executors.newFixedThreadPool(
        //        Runtime.getRuntime().availableProcessors())) {
        var platformThreadFactory = Thread.ofPlatform().factory();
        var virtualThreadFactory = Thread.ofVirtual().factory();
        var threadFactory = virtualThreadFactory;
        try(var executor = Executors.newThreadPerTaskExecutor(threadFactory)) { // or newVirtualThreadPerTaskExecutor()
            executor.submit(task);
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

    public Scrape(LinkedBlockingQueue<String> pageQueue, Set<String> visited) {
        this.pageQueue = pageQueue;
        this.visited = visited;
    }

    public void scrape() {

        try {
            String url = pageQueue.take();

            Document document = Jsoup.connect(url).get();
            Elements linksOnPage = document.select("a[href]");

            visited.add(url);
            for (Element link : linksOnPage) {
                String nextUrl = link.attr("abs:href");
                if (nextUrl.contains("http")) {
                    pageQueue.add(nextUrl);
                }
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