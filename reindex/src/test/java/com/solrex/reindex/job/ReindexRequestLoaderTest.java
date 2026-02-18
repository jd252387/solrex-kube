package com.solrex.reindex.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static com.solrex.reindex.test.ReindexRequestFixtures.fullRequestYaml;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlUsingDefaults;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlWithUnsupportedProperty;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlWithoutFields;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlWithoutSource;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlWithoutTarget;

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
        var requestFile = writeRequestFile("request.yaml", fullRequestYaml());

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
        var requestFile = writeRequestFile("request-defaults.yaml", requestYamlUsingDefaults());

        var request = loader.load(requestFile);

        assertThat(request.filters().query()).isEqualTo("*:*");
        assertThat(request.filters().fqs()).isEmpty();
        assertThat(request.fields()).containsExactly("id");
        assertThat(request.tuning().readPageSize()).isEqualTo(500);
        assertThat(request.tuning().retryPolicy().maxRetries()).isEqualTo(3);
    }

    @Test
    void shouldRejectUnknownPropertiesInRequestYaml() throws Exception {
        var requestFile = writeRequestFile("request-invalid.yaml", requestYamlWithUnsupportedProperty());

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

    @Test
    void shouldFailWhenSourceIsMissing() throws Exception {
        var requestFile = writeRequestFile("missing-source.yaml", requestYamlWithoutSource());

        assertThatThrownBy(() -> loader.load(requestFile))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("missing required field 'source'");
    }

    @Test
    void shouldFailWhenTargetIsMissing() throws Exception {
        var requestFile = writeRequestFile("missing-target.yaml", requestYamlWithoutTarget());

        assertThatThrownBy(() -> loader.load(requestFile))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("missing required field 'target'");
    }

    @Test
    void shouldFailWhenFieldsAreMissing() throws Exception {
        var requestFile = writeRequestFile("missing-fields.yaml", requestYamlWithoutFields());

        assertThatThrownBy(() -> loader.load(requestFile))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("missing required field 'fields'");
    }

    private Path writeRequestFile(String fileName, String requestYaml) throws Exception {
        var requestFile = tempDir.resolve(fileName);
        Files.writeString(requestFile, requestYaml);
        return requestFile;
    }
}
