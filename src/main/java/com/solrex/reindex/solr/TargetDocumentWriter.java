package com.solrex.reindex.solr;

import com.solrex.reindex.model.ReindexRequest;
import io.smallrye.mutiny.Uni;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.apache.solr.common.SolrInputDocument;

public interface TargetDocumentWriter {
    Uni<Void> writeBatch(@NotNull @Valid ReindexRequest request, @NotNull List<@NotNull SolrInputDocument> batch);
}
