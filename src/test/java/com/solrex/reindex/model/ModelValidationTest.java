package com.solrex.reindex.model;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.validation.ConstraintViolationException;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ModelValidationTest {
    @Test
    void shouldRejectBlankCollectionName() {
        assertThatThrownBy(() -> new CollectionRef(new ClusterConfig("http://solr:8983/solr"), "  "))
            .isInstanceOf(ConstraintViolationException.class);
    }

    @Test
    void shouldRejectNonPositiveReadPageSize() {
        assertThatThrownBy(() -> new ReindexTuning(
            0,
            100,
            1,
            1,
            4,
            Duration.ofSeconds(5),
            RetryPolicy.defaults()
        )).isInstanceOf(ConstraintViolationException.class);
    }

    @Test
    void shouldRejectInconsistentBasicAuthPair() {
        assertThatThrownBy(() -> new ClusterConfig(
            "http://solr:8983/solr",
            Duration.ofSeconds(5),
            Optional.of("user"),
            Optional.empty()
        )).isInstanceOf(ConstraintViolationException.class);
    }
}
