package com.solrex.reindex.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeploymentRequestExamplesTest {
    private final ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();
    private final ReindexRequestLoader requestLoader = new ReindexRequestLoader();

    @TempDir
    Path tempDir;

    @Test
    @SuppressWarnings("unchecked")
    void shouldDeserializeKubernetesConfigMapRequestExample() throws Exception {
        var configMap = yamlObjectMapper.readValue(
            Files.readString(repositoryRoot().resolve("deploy/k8s/reindex-configmap.yaml")),
            Map.class
        );
        var data = (Map<String, Object>) configMap.get("data");
        var requestYaml = (String) data.get("request.yaml");

        var request = requestLoader.load(writeRequestFile("k8s-request.yaml", requestYaml));

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

        var loadedRequest = requestLoader.load(writeRequestFile("helm-request.yaml", requestYaml));

        assertThat(loadedRequest.source().collection()).isEqualTo("source_collection");
        assertThat(loadedRequest.target().collection()).isEqualTo("target_collection");
        assertThat(loadedRequest.fields()).containsExactly("id", "title", "category");
    }

    private Path writeRequestFile(String fileName, String requestYaml) throws Exception {
        var requestFile = tempDir.resolve(fileName);
        Files.writeString(requestFile, requestYaml);
        return requestFile;
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
