package com.solrex.reindex.pipeline;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexResult;
import com.solrex.reindex.model.ReindexStats;
import com.solrex.reindex.solr.SourceDocumentStream;
import com.solrex.reindex.util.ReindexErrorClassifier;
import com.solrex.reindex.util.RetryExecutor;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.solr.common.SolrInputDocument;

@RequiredArgsConstructor
public final class ReindexPipeline {
    @NonNull
    private final Function<ReindexRequest, Uni<SourceDocumentStream>> sourceDocumentReader;
    @NonNull
    private final BiFunction<ReindexRequest, List<SolrInputDocument>, Uni<Void>> targetDocumentWriter;
    @NonNull
    private final BackpressureBatcher backpressureBatcher;

    public Uni<ReindexResult> execute(@NonNull ReindexRequest request) {
        var startedAt = System.nanoTime();
        var docsRead = new LongAdder();
        var docsIndexed = new LongAdder();
        var batchesSent = new LongAdder();
        var retries = new LongAdder();

        return sourceDocumentReader.apply(request)
            .onItem().transformToUni(sourceStream -> {
                var warnings = new ArrayList<>(sourceStream.warnings());

                var documents = sourceStream.documents()
                    .onItem().invoke(doc -> docsRead.increment());

                var batches = backpressureBatcher.batch(
                    documents,
                    request.tuning().writeBatchSize(),
                    request.tuning().maxInFlightBatches()
                );

                return batches
                    .onItem().transformToUni(batch ->
                        RetryExecutor.execute(
                            () -> targetDocumentWriter.apply(request, batch),
                            request.tuning().retryPolicy(),
                            ReindexErrorClassifier::isRetryable,
                            retries::increment
                        ).onItem().invoke(() -> {
                            batchesSent.increment();
                            docsIndexed.add(batch.size());
                        })
                    ).merge(request.tuning().writeConcurrency())
                    .collect().asList()
                    .replaceWith(() -> toResult(
                        sourceStream.exportModeUsed(),
                        warnings,
                        startedAt,
                        docsRead,
                        docsIndexed,
                        batchesSent,
                        retries
                    ));
            });
    }

    private ReindexResult toResult(
        boolean exportModeUsed,
        ArrayList<String> warnings,
        long startedAt,
        LongAdder docsRead,
        LongAdder docsIndexed,
        LongAdder batchesSent,
        LongAdder retries
    ) {
        var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
        var stats = new ReindexStats(
            docsRead.sum(),
            docsIndexed.sum(),
            batchesSent.sum(),
            retries.sum(),
            elapsed
        );
        return new ReindexResult(stats, exportModeUsed, warnings);
    }
}
