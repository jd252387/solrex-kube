package com.solrex.reindex.solr;

import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.RetryPolicy;
import com.solrex.reindex.util.ReindexErrorClassifier;
import com.solrex.reindex.util.RetryExecutor;
import io.smallrye.mutiny.Uni;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

@RequiredArgsConstructor
public final class SolrShardLeaderDiscovery {
    @NonNull
    private final Http2SolrClient sourceClient;

    public Uni<List<ShardLeaderReplica>> discoverLeaders(
        CollectionRef source,
        List<String> sourceShards,
        RetryPolicy retryPolicy
    ) {
        return RetryExecutor.execute(
            () -> requestClusterStatus(source.collection())
                .onItem().transform(response -> extractShardLeaders(response, source.collection(), sourceShards)),
            retryPolicy,
            ReindexErrorClassifier::isRetryable
        );
    }

    static List<ShardLeaderReplica> extractShardLeaders(
        NamedList<Object> response,
        String collection,
        List<String> sourceShards
    ) {
        var collections = requiredObject(requiredObject(response, "cluster"), "collections");
        var collectionStatus = requiredObject(collections, collection);
        var shards = requiredObject(collectionStatus, "shards");

        var discoveredLeaders = new ArrayList<ShardLeaderReplica>();
        for (var shard : entries(shards)) {
            var leader = leaderReplicaForShard(shard.key(), shard.value());
            discoveredLeaders.add(leader);
        }

        discoveredLeaders.sort(Comparator.comparing(ShardLeaderReplica::logicalShard));
        if (discoveredLeaders.isEmpty()) {
            throw new IllegalStateException("No active shard leaders were discovered for collection '" + collection + "'");
        }

        var requested = sourceShards == null
            ? new LinkedHashSet<String>()
            : sourceShards.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(shard -> !shard.isEmpty())
                .collect(LinkedHashSet::new, LinkedHashSet::add, LinkedHashSet::addAll);
        if (requested.isEmpty()) {
            return List.copyOf(discoveredLeaders);
        }
        var discoveredNames = discoveredLeaders.stream()
            .map(ShardLeaderReplica::logicalShard)
            .collect(LinkedHashSet::new, LinkedHashSet::add, LinkedHashSet::addAll);

        var unknownShards = requested.stream()
            .filter(shard -> !discoveredNames.contains(shard))
            .toList();
        if (!unknownShards.isEmpty()) {
            throw new IllegalArgumentException(
                "Requested source shards were not found in source collection '" + collection + "': " + unknownShards
            );
        }

        return discoveredLeaders.stream()
            .filter(leader -> requested.contains(leader.logicalShard()))
            .toList();
    }

    private Uni<NamedList<Object>> requestClusterStatus(String collection) {
        var params = new ModifiableSolrParams();
        params.set("action", "CLUSTERSTATUS");
        params.set("collection", collection);

        var request = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/collections", params)
            .setRequiresCollection(false);

        return Uni.createFrom().completionStage(() -> sourceClient.requestAsync(request));
    }

    private static ShardLeaderReplica leaderReplicaForShard(String shardName, Object shardData) {
        var replicas = requiredObject(shardData, "replicas");

        for (var replica : entries(replicas)) {
            if (!isActiveReplica(replica.value()) || !isLeaderReplica(replica.value())) {
                continue;
            }

            var baseUrl = requiredString(replica.value(), "base_url");
            var coreName = firstNonBlank(
                optionalString(replica.value(), "core"),
                optionalString(replica.value(), "core_name"),
                optionalString(replica.value(), "coreName")
            );

            if (coreName == null) {
                throw new IllegalStateException(
                    "Active leader replica for shard '" + shardName + "' is missing core name metadata"
                );
            }

            return new ShardLeaderReplica(shardName, normalizeBaseUrl(baseUrl), coreName);
        }

        throw new IllegalStateException("No ACTIVE leader replica found for shard '" + shardName + "'");
    }

    private static boolean isActiveReplica(Object replicaData) {
        var state = optionalString(replicaData, "state");
        return state != null && "active".equalsIgnoreCase(state);
    }

    private static boolean isLeaderReplica(Object replicaData) {
        var value = valueOf(replicaData, "leader");
        return switch (value) {
            case Boolean b -> b;
            case String s -> Boolean.parseBoolean(s);
            default -> false;
        };
    }

    private static String requiredString(Object object, String key) {
        var value = optionalString(object, key);
        if (value == null) {
            throw new IllegalStateException("Required value '" + key + "' was missing from cluster status response");
        }
        return value;
    }

    private static String optionalString(Object object, String key) {
        var value = valueOf(object, key);
        if (value == null) {
            return null;
        }
        var text = value.toString().trim();
        return text.isEmpty() ? null : text;
    }

    private static Object requiredObject(Object object, String key) {
        var value = valueOf(object, key);
        if (value == null) {
            throw new IllegalStateException("Required object '" + key + "' was missing from cluster status response");
        }
        return value;
    }

    private static Object valueOf(Object object, String key) {
        if (object instanceof NamedList<?> namedList) {
            return namedList.get(key);
        }
        if (object instanceof Map<?, ?> map) {
            return map.get(key);
        }
        return null;
    }

    private static List<Entry> entries(Object object) {
        var entries = new ArrayList<Entry>();

        if (object instanceof NamedList<?> namedList) {
            for (int i = 0; i < namedList.size(); i++) {
                var key = namedList.getName(i);
                if (key != null) {
                    entries.add(new Entry(key, namedList.getVal(i)));
                }
            }
            return entries;
        }

        if (object instanceof Map<?, ?> map) {
            for (var entry : map.entrySet()) {
                if (entry.getKey() != null) {
                    entries.add(new Entry(entry.getKey().toString(), entry.getValue()));
                }
            }
        }

        return entries;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static String normalizeBaseUrl(String baseUrl) {
        var normalized = baseUrl;
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private record Entry(String key, Object value) {
    }

    public record ShardLeaderReplica(String logicalShard, String baseUrl, String coreName) {
    }
}
