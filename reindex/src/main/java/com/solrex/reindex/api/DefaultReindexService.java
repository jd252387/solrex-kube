package com.solrex.reindex.api;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexResult;
import com.solrex.reindex.pipeline.BackpressureBatcher;
import com.solrex.reindex.pipeline.ReindexPipeline;
import com.solrex.reindex.solr.SchemaAwareExportEligibilityDecider;
import com.solrex.reindex.solr.SolrClientFactory;
import com.solrex.reindex.solr.SolrSchemaMetadataProvider;
import com.solrex.reindex.solr.SolrSourceDocumentReader;
import com.solrex.reindex.solr.SolrTargetDocumentWriter;
import com.solrex.reindex.validation.ValidationSupport;
import io.smallrye.mutiny.Uni;
import java.io.Closeable;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class DefaultReindexService {
    @NonNull
    private final SolrClientFactory solrClientFactory;
    @NonNull
    private final BackpressureBatcher backpressureBatcher;

    public DefaultReindexService() {
        this(new SolrClientFactory(), new BackpressureBatcher());
    }

    public Uni<ReindexResult> reindex(@NonNull ReindexRequest request) {
        ValidationSupport.validate(request);

        var sourceClient = solrClientFactory.create(request.source().cluster());
        var targetClient = solrClientFactory.create(request.target().cluster());

        var sourceReader = new SolrSourceDocumentReader(
            sourceClient,
            new SolrSchemaMetadataProvider(sourceClient),
            new SchemaAwareExportEligibilityDecider()
        );

        var targetWriter = new SolrTargetDocumentWriter(targetClient);
        var pipeline = new ReindexPipeline(sourceReader::streamDocuments, targetWriter::writeBatch, backpressureBatcher);

        return pipeline.execute(request)
            .eventually(() -> {
                closeQuietly(sourceClient);
                closeQuietly(targetClient);
            });
    }

    private void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException ignored) {
            // no-op
        }
    }
}
