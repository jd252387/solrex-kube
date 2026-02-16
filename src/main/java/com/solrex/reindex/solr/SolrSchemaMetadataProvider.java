package com.solrex.reindex.solr;

import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.RetryPolicy;
import com.solrex.reindex.util.ReindexErrorClassifier;
import com.solrex.reindex.util.RetryExecutor;
import io.smallrye.mutiny.Uni;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

public final class SolrSchemaMetadataProvider implements SchemaMetadataProvider {
    private final Http2SolrClient sourceClient;

    public SolrSchemaMetadataProvider(Http2SolrClient sourceClient) {
        this.sourceClient = sourceClient;
    }

    @Override
    public Uni<SchemaMetadata> fetch(CollectionRef source, RetryPolicy retryPolicy) {
        return RetryExecutor.execute(
            () -> Uni.combine().all().unis(
                fetchUniqueKey(source.collection()),
                fetchFieldDocValues(source.collection())
            ).asTuple().map(tuple -> new SchemaMetadata(tuple.getItem1(), tuple.getItem2())),
            retryPolicy,
            ReindexErrorClassifier::isRetryable
        );
    }

    private Uni<String> fetchUniqueKey(String collection) {
        var request = new GenericSolrRequest(SolrRequest.METHOD.GET, "/schema/uniquekey").setRequiresCollection(true);
        return requestAsync(request, collection)
            .onItem().transform(response -> {
                var uniqueKey = response.get("uniqueKey");
                if (uniqueKey == null || uniqueKey.toString().isBlank()) {
                    return "id";
                }
                return uniqueKey.toString();
            });
    }

    private Uni<Map<String, Boolean>> fetchFieldDocValues(String collection) {
        var params = new ModifiableSolrParams();
        params.set("showDefaults", true);

        var request = new GenericSolrRequest(SolrRequest.METHOD.GET, "/schema/fields", params)
            .setRequiresCollection(true);

        return requestAsync(request, collection)
            .onItem().transform(this::extractDocValuesMap);
    }

    private Map<String, Boolean> extractDocValuesMap(NamedList<Object> response) {
        var results = new HashMap<String, Boolean>();
        var fields = response.get("fields");

        if (!(fields instanceof Iterable<?> iterable)) {
            return results;
        }

        for (Object item : iterable) {
            if (!(item instanceof Map<?, ?> fieldMap)) {
                continue;
            }

            var rawName = fieldMap.get("name");
            if (rawName == null) {
                continue;
            }

            var rawDocValues = fieldMap.get("docValues");
            var docValuesEnabled = switch (rawDocValues) {
                case Boolean b -> b;
                case String s -> Boolean.parseBoolean(s);
                case Number n -> n.intValue() != 0;
                default -> false;
            };

            results.put(rawName.toString(), docValuesEnabled);
        }

        return Map.copyOf(results);
    }

    private Uni<NamedList<Object>> requestAsync(SolrRequest<?> request, String collection) {
        return Uni.createFrom().completionStage(() -> sourceClient.requestAsync(request, collection));
    }
}
