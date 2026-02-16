package com.solrex.reindex.solr;

import com.solrex.reindex.model.ReindexRequest;

public interface ExportEligibilityDecider {
    ExportDecision decide(ReindexRequest request, SchemaMetadata schemaMetadata);
}
