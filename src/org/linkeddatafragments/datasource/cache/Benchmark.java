package org.linkeddatafragments.datasource.cache;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Random;
import java.util.Scanner;

public class Benchmark {
    private Random random = new Random(8520);

    private void testCache(Cache<String, String> cache) throws FileNotFoundException, InterruptedException {
        File file = new File("prideAndPrejudice.txt");
        Scanner input = new Scanner(file);

        int limit = 1000;
        int count = 0;
        while (input.hasNext() && count < limit) {
            process(cache, input.next());
            count++;
        }
    }

    private void process(Cache<String, String> cache, String word) throws InterruptedException {
        String cached = cache.find(word);

        if (cached != null) {
            return;
        }

        // simulated hard work.
        Thread.sleep(random.nextInt(10));

        cache.insert(word, word);
    }

    private static void executeBenchmark() {
        final Cache<String, String> lruCache = new LruCache<>(8912);    // 2705ms
        final Cache<String, String> nopCache = new NopCache<>();                // 5429ms

        final Benchmark benchmark = new Benchmark();

        try {
            long startTime = System.currentTimeMillis();
            benchmark.testCache(nopCache);
            long endTime = System.currentTimeMillis();
            System.out.println("[NOP] Total execution time [ms]: " + (endTime - startTime));

            startTime = System.currentTimeMillis();
            benchmark.testCache(lruCache);
            endTime = System.currentTimeMillis();
            System.out.println("[LRU] Total execution time [ms]: " + (endTime - startTime));
        } catch (FileNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            executeBenchmark();
        }
    }
}
