package com.solrex.reindex.solr;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.util.ReindexStageException;
import io.smallrye.mutiny.Uni;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;

public final class SolrTargetDocumentWriter implements TargetDocumentWriter {
    private final Http2SolrClient targetClient;

    public SolrTargetDocumentWriter(@NotNull Http2SolrClient targetClient) {
        this.targetClient = Objects.requireNonNull(targetClient, "targetClient cannot be null");
    }

    @Override
    public Uni<Void> writeBatch(@NotNull @Valid ReindexRequest request, @NotNull List<@NotNull SolrInputDocument> batch) {
        Objects.requireNonNull(request, "request cannot be null");
        Objects.requireNonNull(batch, "batch cannot be null");

        if (batch.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        var updateRequest = new UpdateRequest("/update");
        updateRequest.add(batch);

        return Uni.createFrom().completionStage(() -> targetClient.requestAsync(updateRequest, request.target().collection()))
            .onItem().invoke(this::validateResponse)
            .replaceWithVoid()
            .onFailure().transform(failure -> ReindexStageException.wrap("WRITE_BATCH", request.target().collection(), failure));
    }

    private void validateResponse(NamedList<Object> response) {
        if (response == null) {
            throw new IllegalStateException("Solr update response was null");
        }

        var responseHeader = response.get("responseHeader");
        if (!(responseHeader instanceof NamedList<?> headerValues)) {
            return;
        }

        var statusValue = headerValues.get("status");
        if (!(statusValue instanceof Number status)) {
            return;
        }

        if (status.intValue() >= 400) {
            throw new IllegalStateException("Solr update request failed with status " + status.intValue());
        }
    }
}
