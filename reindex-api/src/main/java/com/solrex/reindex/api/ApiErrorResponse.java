package com.solrex.reindex.api;

import java.time.OffsetDateTime;
import java.util.List;

public record ApiErrorResponse(String error, String message, List<String> details, OffsetDateTime timestamp) {
    static ApiErrorResponse of(String error, String message, List<String> details) {
        return new ApiErrorResponse(error, message, details, OffsetDateTime.now());
    }
}
