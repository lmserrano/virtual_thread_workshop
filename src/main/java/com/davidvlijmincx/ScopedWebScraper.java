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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

public class ScopedWebScraper {

    public static final int PAGES_TO_CRAWL = 100;

    public static void main(String[] args) {
        final var queue = new LinkedBlockingQueue<String>(2000);
        Set<String> visited = ConcurrentHashMap.newKeySet(3000);

        queue.add("http://localhost:8080/v1/crawl/delay/330/57");

        long startTime = System.currentTimeMillis();

        //new ScopedScraper(queue, visited).scrape();
        HttpClient client = createHttpClient();

        var threadFactory = Thread.ofVirtual().factory();
        try(var executor = Executors.newThreadPerTaskExecutor(threadFactory);) {
            for(int i = 0; i!=PAGES_TO_CRAWL; ++i) {
                //var task = new ScopedScraper(queue, visited, client);
                //executor.submit(task);

                executor.submit(() -> ScopedValue.runWhere(ScopedSpider.URL,
                        ()-> {
                            try {
                                return queue.take();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        },
                        new ScopedSpider(queue, visited, client)));
            }
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

    private static HttpClient createHttpClient() {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();
    }

}

class ScopedScraper implements Runnable {

    final static ScopedValue<Supplier<String>> URL = ScopedValue.newInstance();

    private final LinkedBlockingQueue<String> pageQueue;

    private final Set<String> visited;

    private final HttpClient client;

    public ScopedScraper(LinkedBlockingQueue<String> pageQueue, Set<String> visited, HttpClient client) {
        this.pageQueue = pageQueue;
        this.visited = visited;
        this.client = client;
    }

    public void scrape() {

        try {
            String url = ScopedScraper.URL.get().get(); //pageQueue.take();

            Document document = Jsoup.parse(getBody(url));
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
        scrape();
    }

    private String getBody(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder().GET().uri(URI.create(url)).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
}