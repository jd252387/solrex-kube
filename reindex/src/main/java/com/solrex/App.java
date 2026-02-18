package com.solrex;

import com.solrex.reindex.api.DefaultReindexService;
import com.solrex.reindex.model.ClusterConfig;
import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.FieldSelection;
import com.solrex.reindex.model.ReindexFilters;
import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexResult;
import com.solrex.reindex.model.ReindexTuning;
import java.time.Duration;
import java.util.List;

public final class App {
    private App() {
    }

    public static void main(String[] args) {
        var sourceUrl = argOrDefault(args, 0, "http://localhost:30983/solr");
        var sourceCollection = argOrDefault(args, 1, "source_collection");
        var targetUrl = argOrDefault(args, 2, "http://localhost:30984/solr");
        var targetCollection = argOrDefault(args, 3, "target_collection");
        var query = argOrDefault(args, 4, "*:*");
        var timeoutSeconds = intArgOrDefault(args, 5, 120);

        System.out.println("Reindex request:");
        System.out.println("source=" + sourceUrl + "/" + sourceCollection);
        System.out.println("target=" + targetUrl + "/" + targetCollection);
        System.out.println("query=" + query);
        System.out.println("timeoutSeconds=" + timeoutSeconds);

        var request = new ReindexRequest(
            new CollectionRef(new ClusterConfig(sourceUrl), sourceCollection),
            new CollectionRef(new ClusterConfig(targetUrl), targetCollection),
            new ReindexFilters(query, null, null),
            FieldSelection.fields(List.of("id", "title", "category")),
            ReindexTuning.defaults()
        );

        try {
            var result = new DefaultReindexService()
                .reindex(request)
                .await().atMost(Duration.ofSeconds(timeoutSeconds));
            printResult(result);
        } catch (RuntimeException e) {
            System.err.println("Reindex failed: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static String argOrDefault(String[] args, int index, String defaultValue) {
        if (args.length <= index || args[index] == null || args[index].isBlank()) {
            return defaultValue;
        }
        return args[index].trim();
    }

    private static int intArgOrDefault(String[] args, int index, int defaultValue) {
        var raw = argOrDefault(args, index, Integer.toString(defaultValue));
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static void printResult(ReindexResult result) {
        var stats = result.stats();
        System.out.println("Reindex complete.");
        System.out.println("docsRead=" + stats.docsRead());
        System.out.println("docsIndexed=" + stats.docsIndexed());
        System.out.println("batchesSent=" + stats.batchesSent());
        System.out.println("retries=" + stats.retries());
        System.out.println("elapsed=" + stats.elapsed());
        System.out.println("exportModeUsed=" + result.exportModeUsed());
        if (!result.warnings().isEmpty()) {
            System.out.println("warnings:");
            result.warnings().forEach(warning -> System.out.println("  - " + warning));
        }
    }
}
