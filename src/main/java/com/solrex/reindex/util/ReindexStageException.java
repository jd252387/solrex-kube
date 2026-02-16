package com.solrex.reindex.util;

import jakarta.validation.constraints.NotBlank;
import java.util.Objects;

public final class ReindexStageException extends RuntimeException {
    private final String stage;
    private final String collection;

    public ReindexStageException(@NotBlank String stage, String collection, String message, Throwable cause) {
        super(message, cause);
        this.stage = Objects.requireNonNull(stage, "stage cannot be null");
        this.collection = collection == null ? "" : collection;
    }

    public String stage() {
        return stage;
    }

    public String collection() {
        return collection;
    }

    public static ReindexStageException wrap(@NotBlank String stage, String collection, Throwable cause) {
        if (cause instanceof ReindexStageException existing) {
            return existing;
        }
        var message = "Reindex stage failure at " + stage + " for collection '" + collection + "'";
        return new ReindexStageException(stage, collection, message, cause);
    }
}
