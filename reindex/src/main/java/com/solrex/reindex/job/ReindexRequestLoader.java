package com.solrex.reindex.job;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.solrex.reindex.model.ReindexRequest;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

@Singleton
public final class ReindexRequestLoader {
    private final ObjectMapper yamlObjectMapper;

    public ReindexRequestLoader() {
        this.yamlObjectMapper = new ObjectMapper(new YAMLFactory())
            .findAndRegisterModules()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    }

    public ReindexRequest load(Path requestFile) {
        Objects.requireNonNull(requestFile, "requestFile must not be null");

        if (!Files.isRegularFile(requestFile)) {
            throw new IllegalStateException("Reindex request file does not exist: " + requestFile);
        }

        try (InputStream inputStream = Files.newInputStream(requestFile)) {
            var request = yamlObjectMapper.readValue(inputStream, ReindexRequest.class);
            if (request == null) {
                throw new IllegalStateException("Reindex request file produced an empty request: " + requestFile);
            }
            return validateRequiredShape(request, requestFile);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse reindex request file: " + requestFile, e);
        }
    }

    private ReindexRequest validateRequiredShape(ReindexRequest request, Path requestFile) {
        if (request.source() == null) {
            throw new IllegalStateException("Reindex request file is missing required field 'source': " + requestFile);
        }
        if (request.target() == null) {
            throw new IllegalStateException("Reindex request file is missing required field 'target': " + requestFile);
        }
        if (request.fields() == null || request.fields().isEmpty()) {
            throw new IllegalStateException("Reindex request file is missing required field 'fields': " + requestFile);
        }
        return request;
    }
}
