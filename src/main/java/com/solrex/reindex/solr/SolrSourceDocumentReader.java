package com.solrex.reindex.solr;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solrex.reindex.model.FieldSelection;
import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.util.ReindexErrorClassifier;
import com.solrex.reindex.util.ReindexStageException;
import com.solrex.reindex.util.RetryExecutor;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;

public final class SolrSourceDocumentReader implements SourceDocumentReader {
    private static final String CURSOR_STAGE = "READ_CURSOR";
    private static final String EXPORT_STAGE = "READ_EXPORT";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final Http2SolrClient sourceClient;
    private final SchemaMetadataProvider schemaMetadataProvider;
    private final ExportEligibilityDecider exportEligibilityDecider;
    private final ObjectMapper objectMapper;

    public SolrSourceDocumentReader(
        @NotNull Http2SolrClient sourceClient,
        @NotNull SchemaMetadataProvider schemaMetadataProvider,
        @NotNull ExportEligibilityDecider exportEligibilityDecider
    ) {
        this(sourceClient, schemaMetadataProvider, exportEligibilityDecider, new ObjectMapper());
    }

    SolrSourceDocumentReader(
        @NotNull Http2SolrClient sourceClient,
        @NotNull SchemaMetadataProvider schemaMetadataProvider,
        @NotNull ExportEligibilityDecider exportEligibilityDecider,
        @NotNull ObjectMapper objectMapper
    ) {
        this.sourceClient = Objects.requireNonNull(sourceClient, "sourceClient cannot be null");
        this.schemaMetadataProvider = Objects.requireNonNull(schemaMetadataProvider, "schemaMetadataProvider cannot be null");
        this.exportEligibilityDecider = Objects.requireNonNull(exportEligibilityDecider, "exportEligibilityDecider cannot be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper cannot be null");
    }

    @Override
    public Uni<SourceDocumentStream> streamDocuments(@NotNull @Valid ReindexRequest request) {
        Objects.requireNonNull(request, "request cannot be null");

        if (request.fieldSelection().allFields()) {
            var warnings = List.of("/export disabled because all fields were requested");
            return Uni.createFrom().item(new SourceDocumentStream(
                streamWithCursor(request, SchemaMetadata.defaults().uniqueKeyField(), request.fieldSelection()),
                false,
                warnings
            ));
        }

        return schemaMetadataProvider.fetch(request.source(), request.tuning().retryPolicy())
            .onFailure().recoverWithItem(failure -> SchemaMetadata.defaults())
            .onItem().transform(metadata -> {
                var decision = exportEligibilityDecider.decide(request, metadata);
                var sortField = metadata.uniqueKeyField();
                var stream = decision.useExport()
                    ? streamWithExport(request, sortField, request.fieldSelection())
                    : streamWithCursor(request, sortField, request.fieldSelection());
                return new SourceDocumentStream(stream, decision.useExport(), decision.reasonsIfNotEligible());
            });
    }

    private Multi<SolrInputDocument> streamWithCursor(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection
    ) {
        return Multi.createFrom().emitter(emitter ->
            fetchCursorPage(request, sortField, fieldSelection, CursorMarkParams.CURSOR_MARK_START, emitter)
        );
    }

    private void fetchCursorPage(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection,
        String cursorMark,
        MultiEmitter<? super SolrInputDocument> emitter
    ) {
        if (emitter.isCancelled()) {
            return;
        }

        RetryExecutor.execute(
            () -> queryCursorPage(request, sortField, fieldSelection, cursorMark),
            request.tuning().retryPolicy(),
            ReindexErrorClassifier::isRetryable
        ).subscribe().with(
            page -> {
                for (var doc : page.documents()) {
                    if (emitter.isCancelled()) {
                        return;
                    }
                    emitter.emit(doc);
                }

                if (page.done() || emitter.isCancelled()) {
                    emitter.complete();
                    return;
                }

                fetchCursorPage(request, sortField, fieldSelection, page.nextCursorMark(), emitter);
            },
            failure -> emitter.fail(ReindexStageException.wrap(CURSOR_STAGE, request.source().collection(), failure))
        );
    }

    private Uni<CursorPage> queryCursorPage(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection,
        String cursorMark
    ) {
        var params = baseReadParams(request, sortField, fieldSelection);
        params.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
        params.set(CommonParams.ROWS, request.tuning().readPageSize());

        var queryRequest = new QueryRequest(params, SolrRequest.METHOD.GET);

        return requestAsync(queryRequest, request.source().collection())
            .onItem().transform(response -> {
                var queryResponse = new QueryResponse(sourceClient);
                queryResponse.setResponse(response);

                var docs = new ArrayList<SolrInputDocument>();
                var results = queryResponse.getResults();
                if (results != null) {
                    for (SolrDocument result : results) {
                        docs.add(toInputDocument(result));
                    }
                }

                var nextCursorMark = Objects.toString(response.get(CursorMarkParams.CURSOR_MARK_NEXT), cursorMark);
                var done = cursorMark.equals(nextCursorMark);
                return new CursorPage(docs, nextCursorMark, done);
            });
    }

    private Multi<SolrInputDocument> streamWithExport(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection
    ) {
        return Multi.createFrom().emitter(emitter ->
            RetryExecutor.execute(
                () -> requestExportStream(request, sortField, fieldSelection),
                request.tuning().retryPolicy(),
                ReindexErrorClassifier::isRetryable
            ).subscribe().with(
                inputStream -> {
                    try {
                        emitExportDocuments(inputStream, emitter);
                        if (!emitter.isCancelled()) {
                            emitter.complete();
                        }
                    } catch (Exception e) {
                        emitter.fail(ReindexStageException.wrap(EXPORT_STAGE, request.source().collection(), e));
                    }
                },
                failure -> emitter.fail(ReindexStageException.wrap(EXPORT_STAGE, request.source().collection(), failure))
            )
        );
    }

    private Uni<InputStream> requestExportStream(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection
    ) {
        var params = baseReadParams(request, sortField, fieldSelection);
        params.set(CommonParams.QT, "/export");
        params.set(CommonParams.WT, "json");

        var queryRequest = new QueryRequest(params, SolrRequest.METHOD.GET);
        queryRequest.setResponseParser(new InputStreamResponseParser("json"));

        return requestAsync(queryRequest, request.source().collection())
            .onItem().transform(response -> {
                var statusValue = response.get(InputStreamResponseParser.HTTP_STATUS_KEY);
                if (statusValue instanceof Number status && status.intValue() >= 400) {
                    throw new IllegalStateException("Export request returned HTTP status " + status.intValue());
                }

                var streamValue = response.get(InputStreamResponseParser.STREAM_KEY);
                if (!(streamValue instanceof InputStream inputStream)) {
                    throw new IllegalStateException("Export response did not include an InputStream");
                }

                return inputStream;
            });
    }

    private void emitExportDocuments(InputStream stream, MultiEmitter<? super SolrInputDocument> emitter) throws IOException {
        try (stream; var parser = objectMapper.getFactory().createParser(stream)) {
            while (!emitter.isCancelled() && parser.nextToken() != null) {
                if (parser.currentToken() != JsonToken.FIELD_NAME || !"docs".equals(parser.currentName())) {
                    continue;
                }

                if (parser.nextToken() != JsonToken.START_ARRAY) {
                    continue;
                }

                while (!emitter.isCancelled() && parser.nextToken() != JsonToken.END_ARRAY) {
                    var map = objectMapper.readValue(parser, MAP_TYPE);
                    emitter.emit(toInputDocument(map));
                }
            }
        }
    }

    private ModifiableSolrParams baseReadParams(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection
    ) {
        var params = new ModifiableSolrParams();
        params.set(CommonParams.Q, request.filters().query());
        params.set(CommonParams.FL, buildFieldList(fieldSelection, Optional.ofNullable(sortField)));
        params.set(CommonParams.SORT, sortField + " asc");

        for (String fq : request.filters().fqs()) {
            params.add(CommonParams.FQ, fq);
        }

        request.filters().sourceShardsCsv().ifPresent(shards -> params.set(ShardParams.SHARDS, shards));
        return params;
    }

    private String buildFieldList(FieldSelection fieldSelection, Optional<String> requiredField) {
        if (fieldSelection.allFields()) {
            return "*";
        }

        var fields = new LinkedHashSet<>(fieldSelection.effectiveFields(requiredField));
        return String.join(",", fields);
    }

    private Uni<NamedList<Object>> requestAsync(SolrRequest<?> request, String collection) {
        return Uni.createFrom().completionStage(() -> sourceClient.requestAsync(request, collection));
    }

    private SolrInputDocument toInputDocument(SolrDocument source) {
        var target = new SolrInputDocument();
        for (String fieldName : source.getFieldNames()) {
            target.setField(fieldName, source.getFieldValue(fieldName));
        }
        return target;
    }

    private SolrInputDocument toInputDocument(Map<String, Object> values) {
        var target = new SolrInputDocument();
        values.forEach(target::setField);
        return target;
    }

    private record CursorPage(List<SolrInputDocument> documents, String nextCursorMark, boolean done) {
    }
}
