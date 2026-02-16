package com.solrex.reindex.util;

import com.solrex.reindex.model.RetryPolicy;
import io.smallrye.mutiny.Uni;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.NonNull;

public final class RetryExecutor {
    private RetryExecutor() {
    }

    public static <T> Uni<T> execute(
        @NonNull Supplier<Uni<T>> action,
        @NonNull RetryPolicy policy,
        @NonNull Predicate<Throwable> retryable
    ) {
        return execute(action, policy, retryable, null);
    }

    public static <T> Uni<T> execute(
        @NonNull Supplier<Uni<T>> action,
        @NonNull RetryPolicy policy,
        @NonNull Predicate<Throwable> retryable,
        Runnable onRetry
    ) {
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
