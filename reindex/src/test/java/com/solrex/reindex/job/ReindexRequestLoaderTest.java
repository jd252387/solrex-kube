package com.solrex.reindex.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReindexRequestLoaderTest {
    private final ReindexRequestLoader loader = new ReindexRequestLoader();

    @TempDir
    Path tempDir;

    @Test
    void shouldParseFullRequestYaml() throws Exception {
        var requestFile = tempDir.resolve("request.yaml");
        Files.writeString(requestFile, """
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
              query: '*:*'
              fqs:
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
            """);

        var request = loader.load(requestFile);

        assertThat(request.source().cluster().baseUrl()).isEqualTo("http://source-solr:8983/solr");
        assertThat(request.source().cluster().requestTimeout()).hasToString("PT20S");
        assertThat(request.source().cluster().basicAuthUser()).isEqualTo("source-user");
        assertThat(request.source().cluster().basicAuthPassword()).isEqualTo("source-password");

        assertThat(request.filters().query()).isEqualTo("*:*");
        assertThat(request.filters().fqs()).containsExactly("type:book");

        assertThat(request.fields()).containsExactly("id", "title");

        assertThat(request.tuning().readPageSize()).isEqualTo(100);
        assertThat(request.tuning().writeBatchSize()).isEqualTo(80);
        assertThat(request.tuning().writeConcurrency()).isEqualTo(3);
        assertThat(request.tuning().retryPolicy().maxRetries()).isEqualTo(4);
        assertThat(request.tuning().retryPolicy().initialBackoff()).hasToString("PT0.2S");
    }

    @Test
    void shouldUseRequestDefaultsWhenOptionalSectionsAreMissing() throws Exception {
        var requestFile = tempDir.resolve("request-defaults.yaml");
        Files.writeString(requestFile, """
            source:
              cluster:
                baseUrl: http://source-solr:8983/solr
              collection: source_collection
            target:
              cluster:
                baseUrl: http://target-solr:8983/solr
              collection: target_collection
            fields:
              - id
            """);

        var request = loader.load(requestFile);

        assertThat(request.filters().query()).isEqualTo("*:*");
        assertThat(request.filters().fqs()).isEmpty();
        assertThat(request.fields()).containsExactly("id");
        assertThat(request.tuning().readPageSize()).isEqualTo(500);
        assertThat(request.tuning().retryPolicy().maxRetries()).isEqualTo(3);
    }

    @Test
    void shouldRejectUnknownPropertiesInRequestYaml() throws Exception {
        var requestFile = tempDir.resolve("request-invalid.yaml");
        Files.writeString(requestFile, """
            source:
              cluster:
                baseUrl: http://source-solr:8983/solr
              collection: source_collection
            target:
              cluster:
                baseUrl: http://target-solr:8983/solr
              collection: target_collection
            fields:
              - id
            unsupported: true
            """);

        assertThatThrownBy(() -> loader.load(requestFile))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Failed to parse reindex request file");
    }

    @Test
    void shouldFailWhenRequestFileIsMissing() {
        var missingFile = tempDir.resolve("missing.yaml");

        assertThatThrownBy(() -> loader.load(missingFile))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("does not exist");
    }
}
