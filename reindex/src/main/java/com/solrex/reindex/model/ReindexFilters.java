package com.solrex.reindex.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record ReindexFilters(
    @NotBlank String query,
    @NotNull List<@NotBlank String> fqs,
    @NotNull Optional<List<@NotBlank String>> sourceShards
) {
    public static final String DEFAULT_QUERY = "*:*";

    public ReindexFilters(String query, List<String> fqs, Optional<List<String>> sourceShards) {
        this.query = normalizeQuery(query);
        this.fqs = normalizeFilters(fqs);
        this.sourceShards = sourceShards == null ? Optional.empty() : sourceShards.map(ReindexFilters::normalizeShards);
    }

    public static ReindexFilters defaults() {
        return new ReindexFilters(DEFAULT_QUERY, List.of(), Optional.empty());
    }

    public Optional<String> sourceShardsCsv() {
        return sourceShards
            .filter(list -> !list.isEmpty())
            .map(list -> String.join(",", list));
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

    private static List<String> normalizeShards(List<String> shards) {
        if (shards == null || shards.isEmpty()) {
            return List.of();
        }
        return shards.stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .toList();
    }
}
