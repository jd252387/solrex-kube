package com.solrex.reindex.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.solrex.reindex.model.RetryPolicy;
import io.smallrye.mutiny.Uni;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RetryExecutorTest {
    @Test
    void shouldRetryRetryableFailureAndEventuallySucceed() {
        var attempts = new AtomicInteger();
        var policy = new RetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(5), 0.0);

        var result = RetryExecutor.execute(
            () -> {
                var current = attempts.incrementAndGet();
                if (current < 3) {
                    return Uni.createFrom().failure(new IOException("temporary"));
                }
                return Uni.createFrom().item("ok");
            },
            policy,
            ReindexErrorClassifier::isRetryable
        ).await().indefinitely();

        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void shouldNotRetryNonRetryableFailure() {
        var attempts = new AtomicInteger();
        var policy = new RetryPolicy(5, Duration.ofMillis(1), Duration.ofMillis(5), 0.0);

        assertThatThrownBy(() -> RetryExecutor.execute(
            () -> {
                attempts.incrementAndGet();
                return Uni.createFrom().failure(new IllegalArgumentException("bad input"));
            },
            policy,
            ReindexErrorClassifier::isRetryable
        ).await().indefinitely())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("bad input");

        assertThat(attempts.get()).isEqualTo(1);
    }
}
