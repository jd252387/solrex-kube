package com.solrex.reindex.model;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.solrex.reindex.api.DefaultReindexService;
import com.solrex.reindex.test.ReindexRequestFixtures;
import jakarta.validation.ConstraintViolationException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ModelValidationTest {
    @Test
    void shouldRejectBlankCollectionNameAtServiceBoundary() {
        var request = ReindexRequestFixtures.requestWithTargetCollection("  ");

        assertThatThrownBy(() -> new DefaultReindexService().reindex(request))
            .isInstanceOf(ConstraintViolationException.class);
    }

    @Test
    void shouldRejectNonPositiveReadPageSizeAtServiceBoundary() {
        var request = ReindexRequestFixtures.requestWithTuning(new ReindexTuning(0, 100, 1, RetryPolicy.defaults()));

        assertThatThrownBy(() -> new DefaultReindexService().reindex(request))
            .isInstanceOf(ConstraintViolationException.class);
    }

    @Test
    void shouldRejectInconsistentBasicAuthPairAtServiceBoundary() {
        var sourceConfig = new ClusterConfig(
            "http://source-solr:8983/solr",
            Duration.ofSeconds(5),
            "user",
            null
        );
        var request = ReindexRequestFixtures.requestWithSourceCluster(sourceConfig);

        assertThatThrownBy(() -> new DefaultReindexService().reindex(request))
            .isInstanceOf(ConstraintViolationException.class);
    }
}
