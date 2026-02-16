package com.solrex.reindex.model;

import com.solrex.reindex.validation.ValidationSupport;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Duration;

public record ReindexStats(
    @PositiveOrZero long docsRead,
    @PositiveOrZero long docsIndexed,
    @PositiveOrZero long batchesSent,
    @PositiveOrZero long retries,
    @NotNull Duration elapsed
) {
    public ReindexStats(long docsRead, long docsIndexed, long batchesSent, long retries, Duration elapsed) {
        this.docsRead = docsRead;
        this.docsIndexed = docsIndexed;
        this.batchesSent = batchesSent;
        this.retries = retries;
        this.elapsed = elapsed;

        ValidationSupport.validate(this);
    }
}
