package com.solrex.reindex.pipeline;

import io.smallrye.mutiny.Multi;
import java.util.List;
import org.apache.solr.common.SolrInputDocument;

public final class BackpressureBatcher {
    public Multi<List<SolrInputDocument>> batch(
        Multi<SolrInputDocument> documents,
        int batchSize,
        int maxInFlightBatches
    ) {
        var maxBufferedDocs = Math.max(batchSize, batchSize * maxInFlightBatches);

        return documents
            .onOverflow().buffer(maxBufferedDocs)
            .group().intoLists().of(batchSize);
    }
}
