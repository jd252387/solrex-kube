package com.solrex.reindex.api;

import java.time.OffsetDateTime;

public record CreateReindexJobResponse(
    String status,
    String jobName,
    String jobNamespace,
    String requestConfigMapName,
    OffsetDateTime acceptedAt
) {
    public static CreateReindexJobResponse accepted(
        String jobName,
        String jobNamespace,
        String requestConfigMapName,
        OffsetDateTime acceptedAt
    ) {
        return new CreateReindexJobResponse("ACCEPTED", jobName, jobNamespace, requestConfigMapName, acceptedAt);
    }
}
