package com.solrex.reindex.api;

enum ApiErrorCode {
    VALIDATION_ERROR("VALIDATION_ERROR"),
    REINDEX_JOB_CONFLICT("REINDEX_JOB_CONFLICT"),
    REINDEX_JOB_CREATE_FAILED("REINDEX_JOB_CREATE_FAILED"),
    INTERNAL_ERROR("INTERNAL_ERROR");

    private final String value;

    ApiErrorCode(String value) {
        this.value = value;
    }

    String value() {
        return value;
    }
}
