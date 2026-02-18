package com.solrex.reindex.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;

public record ReindexRequest(
    @NotNull @Valid CollectionRef source,
    @NotNull @Valid CollectionRef target,
    @NotNull @Valid ReindexFilters filters,
    @NotNull List<String> fields,
    @NotNull @Valid ReindexTuning tuning
) {
    public ReindexRequest(
        CollectionRef source,
        CollectionRef target,
        ReindexFilters filters,
        List<String> fields,
        ReindexTuning tuning
    ) {
        this.source = source;
        this.target = target;
        this.filters = filters == null ? ReindexFilters.defaults() : filters;
        this.fields = normalizeFields(fields);
        this.tuning = tuning == null ? ReindexTuning.defaults() : tuning;
    }

    private static List<String> normalizeFields(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return List.of();
        }

        return fields.stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .distinct()
            .toList();
    }
}
