package com.solrex.reindex.solr;

import com.solrex.reindex.model.ReindexRequest;
import io.smallrye.mutiny.Uni;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public interface SourceDocumentReader {
    Uni<SourceDocumentStream> streamDocuments(@NotNull @Valid ReindexRequest request);
}
