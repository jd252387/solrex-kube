package com.solrex.reindex.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.RetryPolicy;
import com.solrex.reindex.test.ReindexRequestFixtures;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

class ReindexPipelineTest {
    @Test
    void shouldIndexAllDocumentsAndTrackStats() {
        var docs = List.of(doc(1), doc(2), doc(3), doc(4), doc(5), doc(6), doc(7), doc(8), doc(9), doc(10));
        var request = ReindexRequestFixtures.requestWithRetryPolicy(RetryPolicy.defaults());

        var maxInFlight = new AtomicInteger();
        var inFlight = new AtomicInteger();
        var indexedBatchSizes = new CopyOnWriteArrayList<Integer>();

        Function<ReindexRequest, Uni<Multi<SolrInputDocument>>> reader = ignored -> Uni.createFrom().item(
            Multi.createFrom().iterable(docs)
        );

        BiFunction<ReindexRequest, List<SolrInputDocument>, Uni<Void>> writer = (ignored, batch) -> Uni.createFrom().item(() -> {
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

        var result = new ReindexPipeline(reader, writer)
            .execute(request)
            .await().indefinitely();

        assertThat(result.stats().docsRead()).isEqualTo(10);
        assertThat(result.stats().docsIndexed()).isEqualTo(10);
        assertThat(result.stats().batchesSent()).isEqualTo(3);
        assertThat(indexedBatchSizes).containsExactlyInAnyOrder(4, 4, 2);
        assertThat(maxInFlight.get()).isGreaterThan(1);
    }

    @Test
    void shouldCountOnlyScheduledRetriesWhenBatchEventuallySucceeds() {
        var docs = List.of(doc(1), doc(2));
        var attempts = new AtomicInteger();
        var request = ReindexRequestFixtures.requestWithRetryPolicy(
            new RetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(1), 0.0)
        );

        Function<ReindexRequest, Uni<Multi<SolrInputDocument>>> reader = ignored -> Uni.createFrom().item(
            Multi.createFrom().iterable(docs)
        );

        BiFunction<ReindexRequest, List<SolrInputDocument>, Uni<Void>> writer = (ignored, batch) -> Uni.createFrom().deferred(() -> {
            if (attempts.getAndIncrement() < 2) {
                return Uni.createFrom().failure(new IOException("temporary failure"));
            }
            return Uni.createFrom().voidItem();
        });

        var result = new ReindexPipeline(reader, writer)
            .execute(request)
            .await().indefinitely();

        assertThat(attempts.get()).isEqualTo(3);
        assertThat(result.stats().retries()).isEqualTo(2);
    }

    @Test
    void shouldStopRetryingAfterConfiguredMaxRetries() {
        var docs = List.of(doc(1), doc(2));
        var attempts = new AtomicInteger();
        var request = ReindexRequestFixtures.requestWithRetryPolicy(
            new RetryPolicy(2, Duration.ofMillis(1), Duration.ofMillis(1), 0.0)
        );

        Function<ReindexRequest, Uni<Multi<SolrInputDocument>>> reader = ignored -> Uni.createFrom().item(
            Multi.createFrom().iterable(docs)
        );

        BiFunction<ReindexRequest, List<SolrInputDocument>, Uni<Void>> writer = (ignored, batch) -> Uni.createFrom().deferred(() -> {
            attempts.incrementAndGet();
            return Uni.createFrom().failure(new IOException("temporary failure"));
        });

        assertThatThrownBy(() -> new ReindexPipeline(reader, writer)
            .execute(request)
            .await()
            .indefinitely())
                .satisfies(error -> assertThat(error instanceof IOException || error.getCause() instanceof IOException).isTrue());

        assertThat(attempts.get()).isEqualTo(3);
    }

    private SolrInputDocument doc(int id) {
        var document = new SolrInputDocument();
        document.setField("id", id);
        document.setField("title", "doc-" + id);
        return document;
    }
}
