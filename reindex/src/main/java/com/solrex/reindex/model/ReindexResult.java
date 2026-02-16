package com.solrex.reindex.model;

import java.util.List;

public record ReindexResult(ReindexStats stats, boolean exportModeUsed, List<String> warnings) {
    public ReindexResult(ReindexStats stats, boolean exportModeUsed, List<String> warnings) {
        this.stats = stats;
        this.exportModeUsed = exportModeUsed;
        this.warnings = warnings == null ? List.of() : List.copyOf(warnings);
    }
}
