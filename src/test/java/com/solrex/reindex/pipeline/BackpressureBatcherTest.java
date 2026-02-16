package com.solrex.reindex.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.mutiny.Multi;
import java.util.List;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

class BackpressureBatcherTest {
    @Test
    void shouldCreateBatchesWithTrailingPartialBatch() {
        var docs = List.of(doc(1), doc(2), doc(3), doc(4), doc(5), doc(6), doc(7));
        var batcher = new BackpressureBatcher();

        var batches = batcher.batch(Multi.createFrom().iterable(docs), 3, 2)
            .collect().asList().await().indefinitely();

        assertThat(batches).hasSize(3);
        assertThat(batches.get(0)).hasSize(3);
        assertThat(batches.get(1)).hasSize(3);
        assertThat(batches.get(2)).hasSize(1);
    }

    private SolrInputDocument doc(int id) {
        var document = new SolrInputDocument();
        document.setField("id", id);
        return document;
    }
}
