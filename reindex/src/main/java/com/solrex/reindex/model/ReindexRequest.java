package com.solrex.reindex.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record ReindexRequest(
    @NotNull @Valid CollectionRef source,
    @NotNull @Valid CollectionRef target,
    @NotNull @Valid ReindexFilters filters,
    @NotNull @Valid FieldSelection fieldSelection,
    @NotNull @Valid ReindexTuning tuning
) {
    public ReindexRequest(
        CollectionRef source,
        CollectionRef target,
        ReindexFilters filters,
        FieldSelection fieldSelection,
        ReindexTuning tuning
    ) {
        this.source = source;
        this.target = target;
        this.filters = filters == null ? ReindexFilters.defaults() : filters;
        this.fieldSelection = fieldSelection == null ? FieldSelection.all() : fieldSelection;
        this.tuning = tuning == null ? ReindexTuning.defaults() : tuning;
    }
}
