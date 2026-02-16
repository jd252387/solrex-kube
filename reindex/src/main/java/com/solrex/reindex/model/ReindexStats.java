package com.solrex.reindex.model;

import java.time.Duration;

public record ReindexStats(
    long docsRead,
    long docsIndexed,
    long batchesSent,
    long retries,
    Duration elapsed
) {}
