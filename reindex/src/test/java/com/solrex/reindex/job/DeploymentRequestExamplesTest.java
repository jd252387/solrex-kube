package com.solrex.reindex.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.validation.Validator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;

class DeploymentRequestExamplesTest {
    private final ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    @SuppressWarnings("unchecked")
    void shouldDeserializeKubernetesConfigMapRequestExample() throws Exception {
        var configMap = yamlObjectMapper.readValue(
            Files.readString(repositoryRoot().resolve("deploy/k8s/reindex-configmap.yaml")),
            Map.class
        );
        var data = (Map<String, Object>) configMap.get("data");
        var requestYaml = (String) data.get("reindex.job.request");

        var request = producer(requestYaml).reindexRequest();

        assertThat(request.source().collection()).isEqualTo("source_collection");
        assertThat(request.target().collection()).isEqualTo("target_collection");
        assertThat(request.fields()).containsExactly("id", "title", "category");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldDeserializeHelmValuesRequestExample() throws Exception {
        var values = yamlObjectMapper.readValue(
            Files.readString(repositoryRoot().resolve("deploy/helm/reindex-job/values.yaml")),
            Map.class
        );
        var reindex = (Map<String, Object>) values.get("reindex");
        var request = (Map<String, Object>) reindex.get("request");
        var requestYaml = yamlObjectMapper.writeValueAsString(request);

        var loadedRequest = producer(requestYaml).reindexRequest();

        assertThat(loadedRequest.source().collection()).isEqualTo("source_collection");
        assertThat(loadedRequest.target().collection()).isEqualTo("target_collection");
        assertThat(loadedRequest.fields()).containsExactly("id", "title", "category");
    }

    private ReindexRequestConfigProducer producer(String requestYaml) {
        var config = new ReindexJobConfig() {
            @Override
            public String request() {
                return requestYaml;
            }

            @Override
            public Duration timeout() {
                return Duration.ofMinutes(5);
            }
        };
        return new ReindexRequestConfigProducer(config, validator);
    }

    private static Path repositoryRoot() {
        var current = Path.of("").toAbsolutePath().normalize();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("settings.gradle.kts"))
                || Files.isRegularFile(current.resolve("settings.gradle"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Could not find repository root from current working directory.");
    }
}
