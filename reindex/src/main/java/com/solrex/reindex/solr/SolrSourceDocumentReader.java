package com.solrex.reindex.solr;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solrex.reindex.model.FieldSelection;
import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.util.ReindexErrorClassifier;
import com.solrex.reindex.util.RetryExecutor;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
import org.apache.solr.common.util.NamedList;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class SolrSourceDocumentReader {
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    @NonNull
    private final Http2SolrClient sourceClient;
    @NonNull
    private final SolrSchemaMetadataProvider schemaMetadataProvider;
    @NonNull
    private final SchemaAwareExportEligibilityDecider exportEligibilityDecider;
    @NonNull
    private final SolrShardLeaderDiscovery shardLeaderDiscovery;
    @NonNull
    private final ObjectMapper objectMapper;

    public SolrSourceDocumentReader(
        Http2SolrClient sourceClient,
        SolrSchemaMetadataProvider schemaMetadataProvider,
        SchemaAwareExportEligibilityDecider exportEligibilityDecider
    ) {
        this(
            sourceClient,
            schemaMetadataProvider,
            exportEligibilityDecider,
            new SolrShardLeaderDiscovery(sourceClient),
            new ObjectMapper()
        );
    }

    SolrSourceDocumentReader(
        Http2SolrClient sourceClient,
        SolrSchemaMetadataProvider schemaMetadataProvider,
        SchemaAwareExportEligibilityDecider exportEligibilityDecider,
        SolrShardLeaderDiscovery shardLeaderDiscovery
    ) {
        this(sourceClient, schemaMetadataProvider, exportEligibilityDecider, shardLeaderDiscovery, new ObjectMapper());
    }

    public Uni<SourceDocumentStream> streamDocuments(@NonNull ReindexRequest request) {
        if (request.fieldSelection().allFields()) {
            var warnings = List.of("/export disabled because all fields were requested");
            return shardLeaderDiscovery
                .discoverLeaders(request.source(), request.filters().sourceShards(), request.tuning().retryPolicy())
                .onItem().transform(shardLeaders -> new SourceDocumentStream(
                    streamWithCursor(request, SchemaMetadata.defaults().uniqueKeyField(), request.fieldSelection(), shardLeaders),
                    false,
                    warnings
                ));
        }

        return schemaMetadataProvider.fetch(request.source(), request.tuning().retryPolicy())
            .onFailure().recoverWithItem(failure -> SchemaMetadata.defaults())
            .onItem().transformToUni(metadata -> {
                var decision = exportEligibilityDecider.decide(request, metadata);
                var sortField = metadata.uniqueKeyField();
                return shardLeaderDiscovery
                    .discoverLeaders(request.source(), request.filters().sourceShards(), request.tuning().retryPolicy())
                    .onItem().transform(shardLeaders -> {
                        var stream = decision.useExport()
                            ? streamWithExport(request, sortField, request.fieldSelection(), shardLeaders)
                            : streamWithCursor(request, sortField, request.fieldSelection(), shardLeaders);
                        return new SourceDocumentStream(stream, decision.useExport(), decision.reasonsIfNotEligible());
                    });
            });
    }

    private Multi<SolrInputDocument> streamWithCursor(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection,
        List<SolrShardLeaderDiscovery.ShardLeaderReplica> shardLeaders
    ) {
        var shardStreams = shardLeaders.stream()
            .sorted(Comparator.comparing(SolrShardLeaderDiscovery.ShardLeaderReplica::logicalShard))
            .map(shard -> Multi.createFrom().emitter((MultiEmitter<? super SolrInputDocument> emitter) ->
                fetchCursorPage(request, sortField, fieldSelection, CursorMarkParams.CURSOR_MARK_START, shard, emitter)))
            .toList();

        return mergeShardStreams(shardStreams);
    }

    private void fetchCursorPage(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection,
        String cursorMark,
        SolrShardLeaderDiscovery.ShardLeaderReplica shard,
        MultiEmitter<? super SolrInputDocument> emitter
    ) {
        if (emitter.isCancelled()) {
            return;
        }

        RetryExecutor.execute(
            () -> queryCursorPage(request, sortField, fieldSelection, cursorMark, shard),
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

                fetchCursorPage(request, sortField, fieldSelection, page.nextCursorMark(), shard, emitter);
            },
            emitter::fail
        );
    }

    @SuppressWarnings("deprecation")
    private Uni<CursorPage> queryCursorPage(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection,
        String cursorMark,
        SolrShardLeaderDiscovery.ShardLeaderReplica shard
    ) {
        var params = baseReadParams(request, sortField, fieldSelection);
        params.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
        params.set(CommonParams.ROWS, request.tuning().readPageSize());

        var queryRequest = new QueryRequest(params, SolrRequest.METHOD.GET);
        queryRequest.setBasePath(shard.baseUrl());

        return requestAsync(queryRequest, shard.coreName())
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
        FieldSelection fieldSelection,
        List<SolrShardLeaderDiscovery.ShardLeaderReplica> shardLeaders
    ) {
        var shardStreams = shardLeaders.stream()
            .sorted(Comparator.comparing(SolrShardLeaderDiscovery.ShardLeaderReplica::logicalShard))
            .map(shard -> Multi.createFrom().emitter((MultiEmitter<? super SolrInputDocument> emitter) ->
                RetryExecutor.execute(
                    () -> requestExportStream(request, sortField, fieldSelection, shard),
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
                            emitter.fail(e);
                        }
                    },
                    emitter::fail
                )
            ))
            .toList();

        return mergeShardStreams(shardStreams);
    }

    @SuppressWarnings("deprecation")
    private Uni<InputStream> requestExportStream(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection,
        SolrShardLeaderDiscovery.ShardLeaderReplica shard
    ) {
        var params = baseReadParams(request, sortField, fieldSelection);
        params.set(CommonParams.QT, "/export");
        params.set(CommonParams.WT, "json");

        var queryRequest = new QueryRequest(params, SolrRequest.METHOD.GET);
        queryRequest.setResponseParser(new InputStreamResponseParser("json"));
        queryRequest.setBasePath(shard.baseUrl());

        return requestAsync(queryRequest, shard.coreName())
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

    ModifiableSolrParams baseReadParams(
        ReindexRequest request,
        String sortField,
        FieldSelection fieldSelection
    ) {
        var params = new ModifiableSolrParams();
        params.set(CommonParams.Q, request.filters().query());
        params.set(CommonParams.FL, buildFieldList(fieldSelection, sortField));
        params.set(CommonParams.SORT, sortField + " asc");
        params.set(CommonParams.DISTRIB, false);

        for (String fq : request.filters().fqs()) {
            params.add(CommonParams.FQ, fq);
        }

        return params;
    }

    private Multi<SolrInputDocument> mergeShardStreams(List<Multi<SolrInputDocument>> shardStreams) {
        if (shardStreams.isEmpty()) {
            return Multi.createFrom().empty();
        }

        return Multi.createBy().merging()
            .withConcurrency(shardStreams.size())
            .streams(shardStreams);
    }

    private String buildFieldList(FieldSelection fieldSelection, String requiredField) {
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
