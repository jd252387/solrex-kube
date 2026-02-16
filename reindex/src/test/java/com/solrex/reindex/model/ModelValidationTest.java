package com.solrex.reindex.model;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.solrex.reindex.api.DefaultReindexService;
import jakarta.validation.ConstraintViolationException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ModelValidationTest {
    @Test
    void shouldRejectBlankCollectionNameAtServiceBoundary() {
        var request = new ReindexRequest(
            new CollectionRef(new ClusterConfig("http://source-solr:8983/solr"), "source_collection"),
            new CollectionRef(new ClusterConfig("http://target-solr:8983/solr"), "  "),
            ReindexFilters.defaults(),
            FieldSelection.fields(List.of("id")),
            ReindexTuning.defaults()
        );

        assertThatThrownBy(() -> new DefaultReindexService().reindex(request))
            .isInstanceOf(ConstraintViolationException.class);
    }

    @Test
    void shouldRejectNonPositiveReadPageSizeAtServiceBoundary() {
        var request = new ReindexRequest(
            new CollectionRef(new ClusterConfig("http://source-solr:8983/solr"), "source_collection"),
            new CollectionRef(new ClusterConfig("http://target-solr:8983/solr"), "target_collection"),
            ReindexFilters.defaults(),
            FieldSelection.fields(List.of("id")),
            new ReindexTuning(0, 100, 1, 1, 4, Duration.ofSeconds(5), RetryPolicy.defaults())
        );

        assertThatThrownBy(() -> new DefaultReindexService().reindex(request))
            .isInstanceOf(ConstraintViolationException.class);
    }

    @Test
    void shouldRejectInconsistentBasicAuthPairAtServiceBoundary() {
        var sourceConfig = new ClusterConfig(
            "http://source-solr:8983/solr",
            Duration.ofSeconds(5),
            Optional.of("user"),
            Optional.empty()
        );
        var request = new ReindexRequest(
            new CollectionRef(sourceConfig, "source_collection"),
            new CollectionRef(new ClusterConfig("http://target-solr:8983/solr"), "target_collection"),
            ReindexFilters.defaults(),
            FieldSelection.fields(List.of("id")),
            ReindexTuning.defaults()
        );

        assertThatThrownBy(() -> new DefaultReindexService().reindex(request))
            .isInstanceOf(ConstraintViolationException.class);
    }
}
