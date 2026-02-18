package com.solrex.reindex.api;

import java.time.OffsetDateTime;
import java.util.List;

public record ApiErrorResponse(String error, String message, List<String> details, OffsetDateTime timestamp) {
    static ApiErrorResponse of(ApiErrorCode code, String message, List<String> details) {
        return new ApiErrorResponse(code.value(), message, details, OffsetDateTime.now());
    }
}
