package com.solrex.reindex.api;

import java.util.List;

public final class InvalidReindexRequestException extends RuntimeException {
    private final List<String> details;

    public InvalidReindexRequestException(String message, List<String> details) {
        super(message);
        this.details = List.copyOf(details);
    }

    public List<String> details() {
        return details;
    }
}
