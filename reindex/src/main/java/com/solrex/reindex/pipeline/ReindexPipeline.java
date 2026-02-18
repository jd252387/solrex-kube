package com.solrex.reindex.pipeline;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexResult;
import com.solrex.reindex.model.ReindexStats;
import com.solrex.reindex.util.ReindexErrorClassifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
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
    private final Function<ReindexRequest, Uni<Multi<SolrInputDocument>>> sourceDocumentReader;
    @NonNull
    private final BiFunction<ReindexRequest, List<SolrInputDocument>, Uni<Void>> targetDocumentWriter;

    public Uni<ReindexResult> execute(@NonNull ReindexRequest request) {
        var startedAt = System.nanoTime();
        var docsRead = new LongAdder();
        var docsIndexed = new LongAdder();
        var batchesSent = new LongAdder();
        var retries = new LongAdder();
        var maxBufferedDocs = Math.max(request.tuning().writeBatchSize(), request.tuning().writeBatchSize() * 32);

        return sourceDocumentReader.apply(request)
            .onItem().transformToUni(documents ->
                documents
                    .onItem().invoke(doc -> docsRead.increment())
                    .onOverflow().buffer(maxBufferedDocs)
                    .group().intoLists().of(request.tuning().writeBatchSize())
                    .onItem().transformToUni(batch ->
                        targetDocumentWriter.apply(request, batch)
                            .onFailure(ReindexErrorClassifier::isRetryable)
                            .invoke(failure -> retries.increment())
                            .onFailure(ReindexErrorClassifier::isRetryable)
                            .retry()
                            .withBackOff(
                                request.tuning().retryPolicy().initialBackoff(),
                                request.tuning().retryPolicy().maxBackoff()
                            )
                            .atMost(request.tuning().retryPolicy().maxRetries())
                            .onItem().invoke(() -> {
                                batchesSent.increment();
                                docsIndexed.add(batch.size());
                            })
                    )
                    .merge(request.tuning().writeConcurrency())
                    .collect().asList()
                    .replaceWith(() -> toResult(startedAt, docsRead, docsIndexed, batchesSent, retries))
            );
    }

    private ReindexResult toResult(
        long startedAt,
        LongAdder docsRead,
        LongAdder docsIndexed,
        LongAdder batchesSent,
        LongAdder retries
    ) {
        return new ReindexResult(new ReindexStats(
            docsRead.sum(),
            docsIndexed.sum(),
            batchesSent.sum(),
            retries.sum(),
            Duration.ofNanos(System.nanoTime() - startedAt)
        ));
    }
}
