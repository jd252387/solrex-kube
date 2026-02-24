package com.solrex.reindex.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record ReindexTuning(
    @Positive int readPageSize,
    @Positive int writeBatchSize,
    @Positive int writeConcurrency,
    @NotNull @Valid RetryPolicy retryPolicy
) {
    public static final ReindexTuning DEFAULT = new ReindexTuning(
        500,
        200,
        4,
        RetryPolicy.defaults()
    );

    public ReindexTuning(
        int readPageSize,
        int writeBatchSize,
        int writeConcurrency,
        RetryPolicy retryPolicy
    ) {
        this.readPageSize = readPageSize;
        this.writeBatchSize = writeBatchSize;
        this.writeConcurrency = writeConcurrency;
        this.retryPolicy = retryPolicy == null ? RetryPolicy.defaults() : retryPolicy;
    }

    public static ReindexTuning defaults() {
        return DEFAULT;
    }
}
