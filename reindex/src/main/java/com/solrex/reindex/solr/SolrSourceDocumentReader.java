package com.solrex.reindex.solr;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.RetryPolicy;
import com.solrex.reindex.util.ReindexErrorClassifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import lombok.NonNull;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

public final class SolrSourceDocumentReader {
    private static final String DEFAULT_SORT_FIELD = "id";

    private final Http2SolrClient sourceClient;
    private final SolrShardLeaderDiscovery shardLeaderDiscovery;

    public SolrSourceDocumentReader(@NonNull Http2SolrClient sourceClient) {
        this.sourceClient = sourceClient;
        this.shardLeaderDiscovery = new SolrShardLeaderDiscovery(sourceClient);
    }

    public Uni<Multi<SolrInputDocument>> streamDocuments(@NonNull ReindexRequest request) {
        return shardLeaderDiscovery
            .discoverLeaders(request.source(), request.tuning().retryPolicy())
            .onItem().transform(shardLeaders -> streamWithCursor(request, DEFAULT_SORT_FIELD, shardLeaders));
    }

    private Multi<SolrInputDocument> streamWithCursor(
        ReindexRequest request,
        String sortField,
        List<SolrShardLeaderDiscovery.ShardLeaderReplica> shardLeaders
    ) {
        var shardStreams = shardLeaders.stream()
            .sorted(Comparator.comparing(SolrShardLeaderDiscovery.ShardLeaderReplica::logicalShard))
            .map(shard -> Multi.createFrom().emitter((MultiEmitter<? super SolrInputDocument> emitter) ->
                fetchCursorPage(request, sortField, CursorMarkParams.CURSOR_MARK_START, shard, emitter)))
            .toList();

        if (shardStreams.isEmpty()) {
            return Multi.createFrom().empty();
        }

        return Multi.createBy().merging()
            .withConcurrency(shardStreams.size())
            .streams(shardStreams);
    }

    private void fetchCursorPage(
        ReindexRequest request,
        String sortField,
        String cursorMark,
        SolrShardLeaderDiscovery.ShardLeaderReplica shard,
        MultiEmitter<? super SolrInputDocument> emitter
    ) {
        if (emitter.isCancelled()) {
            return;
        }

        queryCursorPage(request, sortField, cursorMark, shard, request.tuning().retryPolicy())
            .subscribe().with(
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

                    fetchCursorPage(request, sortField, page.nextCursorMark(), shard, emitter);
                },
                emitter::fail
            );
    }

    @SuppressWarnings("deprecation")
    private Uni<CursorPage> queryCursorPage(
        ReindexRequest request,
        String sortField,
        String cursorMark,
        SolrShardLeaderDiscovery.ShardLeaderReplica shard,
        RetryPolicy retryPolicy
    ) {
        var params = baseReadParams(request, sortField);
        params.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
        params.set(CommonParams.ROWS, request.tuning().readPageSize());

        var queryRequest = new QueryRequest(params, SolrRequest.METHOD.GET);

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
                return new CursorPage(docs, nextCursorMark, cursorMark.equals(nextCursorMark));
            })
            .onFailure(ReindexErrorClassifier::isRetryable)
            .retry()
            .withBackOff(retryPolicy.initialBackoff(), retryPolicy.maxBackoff())
            .atMost(retryPolicy.maxRetries());
    }

    ModifiableSolrParams baseReadParams(ReindexRequest request, String sortField) {
        var params = new ModifiableSolrParams();
        params.set(CommonParams.Q, request.filters().query());
        params.set(CommonParams.FL, buildFieldList(request.fields(), sortField));
        params.set(CommonParams.SORT, sortField + " asc");
        params.set(CommonParams.DISTRIB, false);

        for (String fq : request.filters().fqs()) {
            params.add(CommonParams.FQ, fq);
        }

        return params;
    }

    private String buildFieldList(List<String> selectedFields, String requiredField) {
        var fields = new LinkedHashSet<>(selectedFields);
        if (requiredField != null && !requiredField.isBlank()) {
            fields.add(requiredField);
        }
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

    private record CursorPage(List<SolrInputDocument> documents, String nextCursorMark, boolean done) {
    }
}
