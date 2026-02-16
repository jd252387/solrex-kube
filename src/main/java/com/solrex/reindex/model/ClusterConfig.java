package com.solrex.reindex.model;

import com.solrex.reindex.validation.ValidationSupport;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Optional;

public record ClusterConfig(
    @NotBlank String baseUrl,
    @NotNull Duration requestTimeout,
    @NotNull Optional<String> basicAuthUser,
    @NotNull Optional<String> basicAuthPassword
) {
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);

    public ClusterConfig(String baseUrl) {
        this(baseUrl, DEFAULT_REQUEST_TIMEOUT, Optional.empty(), Optional.empty());
    }

    public ClusterConfig(
        String baseUrl,
        Duration requestTimeout,
        Optional<String> basicAuthUser,
        Optional<String> basicAuthPassword
    ) {
        this.baseUrl = normalizeBaseUrl(baseUrl);
        this.requestTimeout = requestTimeout == null ? DEFAULT_REQUEST_TIMEOUT : requestTimeout;
        this.basicAuthUser = basicAuthUser == null ? Optional.empty() : basicAuthUser;
        this.basicAuthPassword = basicAuthPassword == null ? Optional.empty() : basicAuthPassword;

        ValidationSupport.validate(this);
    }

    private static String normalizeBaseUrl(String baseUrl) {
        if (baseUrl == null) {
            return null;
        }
        var value = baseUrl.trim();
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    @AssertTrue(message = "requestTimeout must be positive")
    public boolean isRequestTimeoutPositive() {
        return requestTimeout != null && requestTimeout.compareTo(Duration.ZERO) > 0;
    }

    @AssertTrue(message = "basicAuthUser and basicAuthPassword must either both be set or both be empty")
    public boolean isBasicAuthPairConsistent() {
        if (basicAuthUser == null || basicAuthPassword == null) {
            return true;
        }
        return basicAuthUser.isPresent() == basicAuthPassword.isPresent();
    }
}
