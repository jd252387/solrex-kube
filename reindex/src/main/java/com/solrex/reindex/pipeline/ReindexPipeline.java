package com.solrex.reindex.pipeline;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexResult;
import com.solrex.reindex.model.ReindexStats;
import com.solrex.reindex.model.RetryPolicy;
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
                        writeBatchWithRetry(request, batch, retries, 0)
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

    private Uni<Void> writeBatchWithRetry(
        ReindexRequest request,
        List<SolrInputDocument> batch,
        LongAdder retries,
        int retriesScheduled
    ) {
        return targetDocumentWriter.apply(request, batch)
            .onFailure(failure -> shouldRetry(request.tuning().retryPolicy(), failure, retriesScheduled))
            .recoverWithUni(failure -> {
                var retryAttempt = retriesScheduled + 1;
                retries.increment();
                return Uni.createFrom().voidItem()
                    .onItem().delayIt().by(request.tuning().retryPolicy().backoffForAttempt(retryAttempt))
                    .flatMap(ignored -> writeBatchWithRetry(request, batch, retries, retryAttempt));
            });
    }

    private boolean shouldRetry(RetryPolicy retryPolicy, Throwable failure, int retriesScheduled) {
        return ReindexErrorClassifier.isRetryable(failure)
            && retriesScheduled < retryPolicy.maxRetries();
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
