package com.solrex.reindex.api;

public final class ReindexJobConflictException extends RuntimeException {
    public ReindexJobConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
