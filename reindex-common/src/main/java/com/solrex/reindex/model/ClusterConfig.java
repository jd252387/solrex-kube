package com.solrex.reindex.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;

@Getter
public class ClusterConfig {
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private final @NotBlank String baseUrl;
    private final @NotNull Duration requestTimeout;
    private final String basicAuthUser;
    private final String basicAuthPassword;

    public ClusterConfig(String baseUrl) {
        this(baseUrl, DEFAULT_REQUEST_TIMEOUT, null, null);
    }

    @JsonCreator
    public ClusterConfig(
        @JsonProperty("baseUrl")
        String baseUrl,
        @JsonProperty("requestTimeout")
        Duration requestTimeout,
        @JsonProperty("basicAuthUser")
        String basicAuthUser,
        @JsonProperty("basicAuthPassword")
        String basicAuthPassword
    ) {
        this.baseUrl = normalizeBaseUrl(baseUrl);
        this.requestTimeout = requestTimeout == null ? DEFAULT_REQUEST_TIMEOUT : requestTimeout;
        this.basicAuthUser = basicAuthUser;
        this.basicAuthPassword = basicAuthPassword;
    }

    private static String normalizeBaseUrl(String baseUrl) {
        if (baseUrl == null) {
            return null;
        }
        try {
            return new URI(baseUrl).normalize().toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid Solr baseUrl: " + baseUrl, e);
        }
    }

    public String baseUrl() {
        return baseUrl;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    public String basicAuthUser() {
        return basicAuthUser;
    }

    public String basicAuthPassword() {
        return basicAuthPassword;
    }

    @AssertTrue(message = "requestTimeout must be positive")
    public boolean isRequestTimeoutPositive() {
        return requestTimeout != null && requestTimeout.compareTo(Duration.ZERO) > 0;
    }

    @AssertTrue(message = "basicAuthUser and basicAuthPassword must either both be set or both be empty")
    public boolean isBasicAuthPairConsistent() {
        return (basicAuthUser == null) == (basicAuthPassword == null);
    }
}
