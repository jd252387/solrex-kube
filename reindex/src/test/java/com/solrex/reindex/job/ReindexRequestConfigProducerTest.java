package com.solrex.reindex.job;

import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlUsingDefaults;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlWithUnsupportedProperty;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlWithoutFields;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlWithoutSource;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlWithoutTarget;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ReindexRequestConfigProducerTest {
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void shouldParseFullRequestYamlFromConfig() {
        var request = producer(fullRequestYamlWithAllSections()).reindexRequest();

        assertThat(request.source().collection()).isEqualTo("source_collection");
        assertThat(request.target().collection()).isEqualTo("target_collection");
        assertThat(request.fields()).containsExactly("id", "title");
        assertThat(request.tuning().readPageSize()).isEqualTo(100);
        assertThat(request.tuning().writeBatchSize()).isEqualTo(80);
        assertThat(request.tuning().writeConcurrency()).isEqualTo(3);
        assertThat(request.tuning().retryPolicy().maxRetries()).isEqualTo(4);
    }

    @Test
    void shouldUseRequestDefaultsWhenOptionalSectionsAreMissing() {
        var request = producer(requestYamlUsingDefaults()).reindexRequest();

        assertThat(request.fields()).containsExactly("id");
        assertThat(request.tuning().readPageSize()).isEqualTo(500);
        assertThat(request.tuning().retryPolicy().maxRetries()).isEqualTo(3);
    }

    @Test
    void shouldRejectUnknownPropertiesInRequestYaml() {
        assertThatThrownBy(() -> producer(requestYamlWithUnsupportedProperty()).reindexRequest())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Failed to parse reindex request config");
    }

    @Test
    void shouldFailWhenRequestConfigIsBlank() {
        assertThatThrownBy(() -> producer("   ").reindexRequest())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("empty");
    }

    @Test
    void shouldFailWhenSourceIsMissing() {
        assertThatThrownBy(() -> producer(requestYamlWithoutSource()).reindexRequest())
            .isInstanceOf(ConstraintViolationException.class)
            .hasMessageContaining("Request validation failed");
    }

    @Test
    void shouldFailWhenTargetIsMissing() {
        assertThatThrownBy(() -> producer(requestYamlWithoutTarget()).reindexRequest())
            .isInstanceOf(ConstraintViolationException.class)
            .hasMessageContaining("Request validation failed");
    }

    @Test
    void shouldFailWhenFieldsAreMissing() {
        assertThatThrownBy(() -> producer(requestYamlWithoutFields()).reindexRequest())
            .isInstanceOf(ConstraintViolationException.class)
            .hasMessageContaining("Request validation failed");
    }

    private ReindexRequestConfigProducer producer(String requestYaml) {
        var config = new ReindexJobConfig() {
            @Override
            public String request() {
                return requestYaml;
            }

            @Override
            public Duration timeout() {
                return Duration.ofSeconds(5);
            }
        };
        return new ReindexRequestConfigProducer(config, validator);
    }

    private String fullRequestYamlWithAllSections() {
        return """
            source:
              cluster:
                baseUrl: http://source-solr:8983/solr/
                requestTimeout: PT20S
                basicAuthUser: source-user
                basicAuthPassword: source-password
              collection: source_collection
            target:
              cluster:
                baseUrl: http://target-solr:8983/solr
                requestTimeout: PT25S
              collection: target_collection
            filters:
              - type:book
            fields:
              - id
              - title
            tuning:
              readPageSize: 100
              writeBatchSize: 80
              writeConcurrency: 3
              retryPolicy:
                maxRetries: 4
                initialBackoff: PT0.2S
                maxBackoff: PT3S
                jitterFactor: 0.1
            """;
    }
}
