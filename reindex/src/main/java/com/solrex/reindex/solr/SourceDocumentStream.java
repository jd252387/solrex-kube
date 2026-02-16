package com.solrex.reindex.solr;

import io.smallrye.mutiny.Multi;
import java.util.List;
import org.apache.solr.common.SolrInputDocument;

public record SourceDocumentStream(Multi<SolrInputDocument> documents, boolean exportModeUsed, List<String> warnings) {
    public SourceDocumentStream {
        warnings = warnings == null ? List.of() : List.copyOf(warnings);
    }
}
