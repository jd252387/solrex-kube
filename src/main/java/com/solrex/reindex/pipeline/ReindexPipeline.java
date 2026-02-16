package com.solrex.reindex.pipeline;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexResult;
import com.solrex.reindex.model.ReindexStats;
import com.solrex.reindex.solr.SourceDocumentReader;
import com.solrex.reindex.solr.TargetDocumentWriter;
import com.solrex.reindex.util.ReindexErrorClassifier;
import com.solrex.reindex.util.ReindexStageException;
import com.solrex.reindex.util.RetryExecutor;
import io.smallrye.mutiny.Uni;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public final class ReindexPipeline {
    private final SourceDocumentReader sourceDocumentReader;
    private final TargetDocumentWriter targetDocumentWriter;
    private final BackpressureBatcher backpressureBatcher;

    public ReindexPipeline(
        @NotNull SourceDocumentReader sourceDocumentReader,
        @NotNull TargetDocumentWriter targetDocumentWriter,
        @NotNull BackpressureBatcher backpressureBatcher
    ) {
        this.sourceDocumentReader = Objects.requireNonNull(sourceDocumentReader, "sourceDocumentReader cannot be null");
        this.targetDocumentWriter = Objects.requireNonNull(targetDocumentWriter, "targetDocumentWriter cannot be null");
        this.backpressureBatcher = Objects.requireNonNull(backpressureBatcher, "backpressureBatcher cannot be null");
    }

    public Uni<ReindexResult> execute(@NotNull @Valid ReindexRequest request) {
        Objects.requireNonNull(request, "request cannot be null");

        var startedAt = System.nanoTime();
        var docsRead = new LongAdder();
        var docsIndexed = new LongAdder();
        var batchesSent = new LongAdder();
        var retries = new LongAdder();

        return sourceDocumentReader.streamDocuments(request)
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
                            () -> targetDocumentWriter.writeBatch(request, batch),
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
            })
            .onFailure().transform(failure -> ReindexStageException.wrap("PIPELINE", request.target().collection(), failure));
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
