package com.solrex.reindex.util;

import com.solrex.reindex.model.RetryPolicy;
import io.smallrye.mutiny.Uni;
import jakarta.validation.constraints.NotNull;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class RetryExecutor {
    private RetryExecutor() {
    }

    public static <T> Uni<T> execute(
        @NotNull Supplier<Uni<T>> action,
        @NotNull RetryPolicy policy,
        @NotNull Predicate<Throwable> retryable
    ) {
        return execute(action, policy, retryable, null);
    }

    public static <T> Uni<T> execute(
        @NotNull Supplier<Uni<T>> action,
        @NotNull RetryPolicy policy,
        @NotNull Predicate<Throwable> retryable,
        Runnable onRetry
    ) {
        Objects.requireNonNull(action, "action cannot be null");
        Objects.requireNonNull(policy, "policy cannot be null");
        Objects.requireNonNull(retryable, "retryable cannot be null");

        return attempt(action, policy, retryable, onRetry, 1);
    }

    private static <T> Uni<T> attempt(
        Supplier<Uni<T>> action,
        RetryPolicy policy,
        Predicate<Throwable> retryable,
        Runnable onRetry,
        int attemptNumber
    ) {
        return action.get().onFailure(failure -> shouldRetry(policy, retryable, failure, attemptNumber))
            .recoverWithUni(failure -> {
                if (onRetry != null) {
                    onRetry.run();
                }

                var delay = policy.backoffForAttempt(attemptNumber);
                return Uni.createFrom().voidItem()
                    .onItem().delayIt().by(delay)
                    .replaceWith(attempt(action, policy, retryable, onRetry, attemptNumber + 1));
            });
    }

    private static boolean shouldRetry(
        RetryPolicy policy,
        Predicate<Throwable> retryable,
        Throwable failure,
        int attemptNumber
    ) {
        return retryable.test(failure) && attemptNumber <= policy.maxRetries();
    }
}
