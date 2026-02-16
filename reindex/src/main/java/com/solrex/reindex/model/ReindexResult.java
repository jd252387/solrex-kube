package com.solrex.reindex.model;

import java.util.List;

public record ReindexResult(ReindexStats stats, boolean exportModeUsed, List<String> warnings) {}
