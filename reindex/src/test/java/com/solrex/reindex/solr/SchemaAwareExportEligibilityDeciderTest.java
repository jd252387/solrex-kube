package com.solrex.reindex.solr;

import static org.assertj.core.api.Assertions.assertThat;

import com.solrex.reindex.model.ClusterConfig;
import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.FieldSelection;
import com.solrex.reindex.model.ReindexFilters;
import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexTuning;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SchemaAwareExportEligibilityDeciderTest {
    private final SchemaAwareExportEligibilityDecider decider = new SchemaAwareExportEligibilityDecider();

    @Test
    void shouldEnableExportWhenAllFieldsAreDocValuesBacked() {
        var request = requestWithFields("id", "title");
        var metadata = new SchemaMetadata("id", Map.of("id", true, "title", true));

        var decision = decider.decide(request, metadata);

        assertThat(decision.useExport()).isTrue();
        assertThat(decision.reasonsIfNotEligible()).isEmpty();
    }

    @Test
    void shouldDisableExportWhenFieldIsNotDocValuesBacked() {
        var request = requestWithFields("id", "body");
        var metadata = new SchemaMetadata("id", Map.of("id", true, "body", false));

        var decision = decider.decide(request, metadata);

        assertThat(decision.useExport()).isFalse();
        assertThat(decision.reasonsIfNotEligible()).anyMatch(reason -> reason.contains("body"));
    }

    @Test
    void shouldDisableExportWhenAllFieldsRequested() {
        var request = new ReindexRequest(
            source(),
            target(),
            ReindexFilters.defaults(),
            FieldSelection.all(),
            ReindexTuning.defaults()
        );

        var decision = decider.decide(request, new SchemaMetadata("id", Map.of("id", true)));

        assertThat(decision.useExport()).isFalse();
    }

    private ReindexRequest requestWithFields(String... fields) {
        return new ReindexRequest(
            source(),
            target(),
            new ReindexFilters("*:*", List.of(), Optional.empty()),
            FieldSelection.fields(List.of(fields)),
            ReindexTuning.defaults()
        );
    }

    private CollectionRef source() {
        return new CollectionRef(new ClusterConfig("http://source-solr:8983/solr"), "source_collection");
    }

    private CollectionRef target() {
        return new CollectionRef(new ClusterConfig("http://target-solr:8983/solr"), "target_collection");
    }
}
