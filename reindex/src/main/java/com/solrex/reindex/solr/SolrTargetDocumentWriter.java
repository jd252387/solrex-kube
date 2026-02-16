package com.solrex.reindex.solr;

import com.solrex.reindex.model.ReindexRequest;
import io.smallrye.mutiny.Uni;
import java.util.List;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;

@RequiredArgsConstructor
public final class SolrTargetDocumentWriter {
    @NonNull
    private final Http2SolrClient targetClient;

    public Uni<Void> writeBatch(
        @NonNull ReindexRequest request,
        @NonNull List<SolrInputDocument> batch
    ) {
        if (batch.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        var updateRequest = new UpdateRequest("/update");
        updateRequest.add(batch);

        return Uni.createFrom().completionStage(() -> targetClient.requestAsync(updateRequest, request.target().collection()))
            .onItem().invoke(this::validateResponse)
            .replaceWithVoid();
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
