package com.solrex.reindex.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;

public record ReindexTuning(
    @Positive int readPageSize,
    @Positive int writeBatchSize,
    @Positive int readConcurrency,
    @Positive int writeConcurrency,
    @Positive int maxInFlightBatches,
    @NotNull Duration requestTimeout,
    @NotNull @Valid RetryPolicy retryPolicy
) {
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final ReindexTuning DEFAULT = new ReindexTuning(
        500,
        200,
        1,
        4,
        32,
        DEFAULT_REQUEST_TIMEOUT,
        RetryPolicy.defaults()
    );

    public ReindexTuning(
        int readPageSize,
        int writeBatchSize,
        int readConcurrency,
        int writeConcurrency,
        int maxInFlightBatches,
        Duration requestTimeout,
        RetryPolicy retryPolicy
    ) {
        this.readPageSize = readPageSize;
        this.writeBatchSize = writeBatchSize;
        this.readConcurrency = readConcurrency;
        this.writeConcurrency = writeConcurrency;
        this.maxInFlightBatches = maxInFlightBatches;
        this.requestTimeout = requestTimeout == null ? DEFAULT_REQUEST_TIMEOUT : requestTimeout;
        this.retryPolicy = retryPolicy == null ? RetryPolicy.defaults() : retryPolicy;
    }

    public static ReindexTuning defaults() {
        return DEFAULT;
    }

    @AssertTrue(message = "requestTimeout must be positive")
    public boolean isRequestTimeoutPositive() {
        return requestTimeout != null && requestTimeout.compareTo(Duration.ZERO) > 0;
    }
}
