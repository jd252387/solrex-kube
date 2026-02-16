package com.solrex.reindex.model;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public record RetryPolicy(
    @PositiveOrZero int maxRetries,
    @NotNull Duration initialBackoff,
    @NotNull Duration maxBackoff,
    @DecimalMin("0.0") @DecimalMax("1.0") double jitterFactor
) {
    private static final RetryPolicy DEFAULT = new RetryPolicy(
        3,
        Duration.ofMillis(250),
        Duration.ofSeconds(5),
        0.2
    );

    public RetryPolicy(int maxRetries, Duration initialBackoff, Duration maxBackoff, double jitterFactor) {
        this.maxRetries = maxRetries;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.jitterFactor = jitterFactor;
    }

    public static RetryPolicy defaults() {
        return DEFAULT;
    }

    public Duration backoffForAttempt(int attemptNumber) {
        var safeAttempt = Math.max(1, attemptNumber);
        var baseMillis = initialBackoff.toMillis();
        var exponent = 1L << Math.min(30, safeAttempt - 1);
        var candidate = Math.min(maxBackoff.toMillis(), baseMillis * exponent);

        if (jitterFactor == 0.0) {
            return Duration.ofMillis(candidate);
        }

        var min = Math.max(1, (long) (candidate * (1.0 - jitterFactor)));
        var max = Math.max(min, (long) (candidate * (1.0 + jitterFactor)));
        var jittered = ThreadLocalRandom.current().nextLong(min, max + 1);
        return Duration.ofMillis(jittered);
    }

    @AssertTrue(message = "initialBackoff must be positive")
    public boolean isInitialBackoffPositive() {
        return initialBackoff != null && initialBackoff.compareTo(Duration.ZERO) > 0;
    }

    @AssertTrue(message = "maxBackoff must be positive")
    public boolean isMaxBackoffPositive() {
        return maxBackoff != null && maxBackoff.compareTo(Duration.ZERO) > 0;
    }

    @AssertTrue(message = "maxBackoff must be >= initialBackoff")
    public boolean isBackoffRangeValid() {
        if (initialBackoff == null || maxBackoff == null) {
            return true;
        }
        return maxBackoff.compareTo(initialBackoff) >= 0;
    }
}
