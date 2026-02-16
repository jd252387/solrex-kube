package com.solrex.reindex.model;

import com.solrex.reindex.validation.ValidationSupport;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public record ReindexResult(@NotNull @Valid ReindexStats stats, boolean exportModeUsed, @NotNull List<@NotBlank String> warnings) {
    public ReindexResult(ReindexStats stats, boolean exportModeUsed, List<String> warnings) {
        this.stats = stats;
        this.exportModeUsed = exportModeUsed;
        this.warnings = warnings == null ? List.of() : List.copyOf(warnings);

        ValidationSupport.validate(this);
    }
}
