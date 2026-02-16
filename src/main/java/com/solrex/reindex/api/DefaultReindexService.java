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
import io.smallrye.mutiny.Uni;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

public final class DefaultReindexService implements ReindexService {
    private final SolrClientFactory solrClientFactory;
    private final BackpressureBatcher backpressureBatcher;

    public DefaultReindexService() {
        this(new SolrClientFactory(), new BackpressureBatcher());
    }

    DefaultReindexService(@NotNull SolrClientFactory solrClientFactory, @NotNull BackpressureBatcher backpressureBatcher) {
        this.solrClientFactory = Objects.requireNonNull(solrClientFactory, "solrClientFactory cannot be null");
        this.backpressureBatcher = Objects.requireNonNull(backpressureBatcher, "backpressureBatcher cannot be null");
    }

    @Override
    public Uni<ReindexResult> reindex(@NotNull @Valid ReindexRequest request) {
        Objects.requireNonNull(request, "request cannot be null");

        var sourceClient = solrClientFactory.create(request.source().cluster());
        var targetClient = solrClientFactory.create(request.target().cluster());

        var sourceReader = new SolrSourceDocumentReader(
            sourceClient,
            new SolrSchemaMetadataProvider(sourceClient),
            new SchemaAwareExportEligibilityDecider()
        );

        var targetWriter = new SolrTargetDocumentWriter(targetClient);
        var pipeline = new ReindexPipeline(sourceReader, targetWriter, backpressureBatcher);

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
