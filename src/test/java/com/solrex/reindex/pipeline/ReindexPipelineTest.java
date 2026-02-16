package com.solrex.reindex.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.solrex.reindex.model.ClusterConfig;
import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.FieldSelection;
import com.solrex.reindex.model.ReindexFilters;
import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexTuning;
import com.solrex.reindex.solr.SourceDocumentReader;
import com.solrex.reindex.solr.SourceDocumentStream;
import com.solrex.reindex.solr.TargetDocumentWriter;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

class ReindexPipelineTest {
    @Test
    void shouldIndexAllDocumentsAndTrackStats() {
        var docs = List.of(doc(1), doc(2), doc(3), doc(4), doc(5), doc(6), doc(7), doc(8), doc(9), doc(10));
        var request = request();

        var maxInFlight = new AtomicInteger();
        var inFlight = new AtomicInteger();
        var indexedBatchSizes = new CopyOnWriteArrayList<Integer>();

        SourceDocumentReader reader = ignored -> Uni.createFrom().item(
            new SourceDocumentStream(Multi.createFrom().iterable(docs), false, List.of())
        );

        TargetDocumentWriter writer = (ignored, batch) -> Uni.createFrom().item(() -> {
            var current = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(current, Math::max);
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                indexedBatchSizes.add(batch.size());
                inFlight.decrementAndGet();
            }
            return null;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool()).replaceWithVoid();

        var result = new ReindexPipeline(reader, writer, new BackpressureBatcher())
            .execute(request)
            .await().indefinitely();

        assertThat(result.stats().docsRead()).isEqualTo(10);
        assertThat(result.stats().docsIndexed()).isEqualTo(10);
        assertThat(result.stats().batchesSent()).isEqualTo(3);
        assertThat(indexedBatchSizes).containsExactlyInAnyOrder(4, 4, 2);
        assertThat(maxInFlight.get()).isGreaterThan(1);
    }

    private ReindexRequest request() {
        return new ReindexRequest(
            new CollectionRef(new ClusterConfig("http://source-solr:8983/solr"), "source_collection"),
            new CollectionRef(new ClusterConfig("http://target-solr:8983/solr"), "target_collection"),
            new ReindexFilters("*:*", List.of(), Optional.empty()),
            FieldSelection.fields(List.of("id", "title")),
            new ReindexTuning(
                200,
                4,
                1,
                4,
                16,
                ReindexTuning.defaults().requestTimeout(),
                ReindexTuning.defaults().retryPolicy()
            )
        );
    }

    private SolrInputDocument doc(int id) {
        var document = new SolrInputDocument();
        document.setField("id", id);
        document.setField("title", "doc-" + id);
        return document;
    }
}
