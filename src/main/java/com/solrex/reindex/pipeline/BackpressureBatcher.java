package com.solrex.reindex.pipeline;

import io.smallrye.mutiny.Multi;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.List;
import org.apache.solr.common.SolrInputDocument;

public final class BackpressureBatcher {
    public Multi<List<SolrInputDocument>> batch(
        @NotNull Multi<SolrInputDocument> documents,
        @Positive int batchSize,
        @Positive int maxInFlightBatches
    ) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
        if (maxInFlightBatches <= 0) {
            throw new IllegalArgumentException("maxInFlightBatches must be > 0");
        }

        var maxBufferedDocs = Math.max(batchSize, batchSize * maxInFlightBatches);

        return documents
            .onOverflow().buffer(maxBufferedDocs)
            .group().intoLists().of(batchSize);
    }
}
