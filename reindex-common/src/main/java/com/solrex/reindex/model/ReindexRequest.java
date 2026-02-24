package com.solrex.reindex.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;

public record ReindexRequest(
    @NotNull @Valid CollectionRef source,
    @NotNull @Valid CollectionRef target,
    @Valid List<String> filters,
    @NotNull List<String> fields,
    @Valid ReindexTuning tuning
) {
    public ReindexRequest(
        CollectionRef source,
        CollectionRef target,
        List<String> filters,
        List<String> fields,
        ReindexTuning tuning
    ) {
        this.source = source;
        this.target = target;
        this.filters = filters == null ? List.of() : filters;
        this.fields = fields;
        this.tuning = tuning == null ? ReindexTuning.DEFAULT : tuning;
    }
}
