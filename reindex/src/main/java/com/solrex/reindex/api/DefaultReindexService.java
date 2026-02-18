package com.solrex.reindex.api;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexResult;
import com.solrex.reindex.pipeline.ReindexPipeline;
import com.solrex.reindex.solr.SolrClientFactory;
import com.solrex.reindex.solr.SolrSourceDocumentReader;
import com.solrex.reindex.solr.SolrTargetDocumentWriter;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public final class DefaultReindexService {
    private static final Executor CLOSE_EXECUTOR = command -> Thread.ofPlatform().daemon().start(command);

    @NonNull
    private final SolrClientFactory solrClientFactory;
    @NonNull
    private final Validator validator;

    public DefaultReindexService() {
        this(new SolrClientFactory(), Validation.buildDefaultValidatorFactory().getValidator());
    }

    public DefaultReindexService(SolrClientFactory solrClientFactory) {
        this(solrClientFactory, Validation.buildDefaultValidatorFactory().getValidator());
    }

    public Uni<ReindexResult> reindex(ReindexRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        validate(request);

        var sourceClient = solrClientFactory.create(request.source().cluster());
        var targetClient = solrClientFactory.create(request.target().cluster());
        var sourceReader = new SolrSourceDocumentReader(sourceClient);
        var targetWriter = new SolrTargetDocumentWriter(targetClient);
        var pipeline = new ReindexPipeline(sourceReader::streamDocuments, targetWriter::writeBatch);

        return pipeline.execute(request)
            .eventually(() -> Uni.createFrom().voidItem()
                .runSubscriptionOn(CLOSE_EXECUTOR)
                .invoke(() -> {
                    closeQuietly(sourceClient);
                    closeQuietly(targetClient);
                }));
    }

    private void validate(ReindexRequest request) {
        Set<ConstraintViolation<ReindexRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }

    private void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException ignored) {
            // no-op
        }
    }
}
