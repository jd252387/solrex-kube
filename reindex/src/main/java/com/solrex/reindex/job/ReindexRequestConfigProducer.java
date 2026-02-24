package com.solrex.reindex.job;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.solrex.reindex.model.ReindexRequest;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import java.io.IOException;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

@Singleton
@RequiredArgsConstructor
public final class ReindexRequestConfigProducer {
    private static final String REQUEST_PROPERTY = "reindex.job.request";

    private final ReindexJobConfig config;
    private final Validator validator;
    private final ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory())
        .findAndRegisterModules()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    @Produces
    @Singleton
    public ReindexRequest reindexRequest() {
        var requestYaml = config.request();
        if (requestYaml == null || requestYaml.isBlank()) {
            throw new IllegalStateException("Reindex request config is empty: " + REQUEST_PROPERTY);
        }

        try {
            var request = Optional.ofNullable(yamlObjectMapper.readValue(requestYaml, ReindexRequest.class))
                .orElseThrow(() ->
                    new IllegalStateException("Reindex request config produced an empty request: " + REQUEST_PROPERTY)
                );

            var violations = validator.validate(request);
            if (!violations.isEmpty()) {
                throw new ConstraintViolationException("Request validation failed", violations);
            }

            return request;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse reindex request config: " + REQUEST_PROPERTY, e);
        }
    }
}
