package com.solrex.reindex.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;

public record ReindexFilters(
    @NotBlank String query,
    @NotNull List<String> fqs
) {
    public static final String DEFAULT_QUERY = "*:*";

    public ReindexFilters(String query, List<String> fqs) {
        this.query = normalizeQuery(query);
        this.fqs = normalizeFilters(fqs);
    }

    public static ReindexFilters defaults() {
        return new ReindexFilters(DEFAULT_QUERY, List.of());
    }

    private static String normalizeQuery(String query) {
        if (query == null || query.isBlank()) {
            return DEFAULT_QUERY;
        }
        return query;
    }

    private static List<String> normalizeFilters(List<String> fqs) {
        if (fqs == null || fqs.isEmpty()) {
            return List.of();
        }
        return fqs.stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .toList();
    }

}
