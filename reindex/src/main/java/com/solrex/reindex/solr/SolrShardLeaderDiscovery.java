package com.solrex.reindex.solr;

import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.RetryPolicy;
import com.solrex.reindex.util.ReindexErrorClassifier;
import io.smallrye.mutiny.Uni;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

public final class SolrShardLeaderDiscovery {

    private SolrShardLeaderDiscovery() {
    }

    public static Uni<List<ShardLeaderReplica>> discoverLeaders(Http2SolrClient sourceClient, CollectionRef source,
            RetryPolicy retryPolicy) {
        return requestClusterStatus(sourceClient, source.collection())
                .onItem().transform(response -> extractShardLeaders(response, source.collection()))
                .onFailure(ReindexErrorClassifier::isRetryable)
                .retry()
                .withBackOff(retryPolicy.initialBackoff(), retryPolicy.maxBackoff())
                .atMost(retryPolicy.maxRetries());
    }

    static List<ShardLeaderReplica> extractShardLeaders(NamedList<Object> response, String collection) {
        var cluster = requireObjectMap(response.get("cluster"), "cluster");
        var collections = requireObjectMap(cluster.get("collections"), "cluster.collections");
        var collectionStatus = requireObjectMap(collections.get(collection), "cluster.collections." + collection);
        var shards = requireObjectMap(
                collectionStatus.get("shards"),
                "cluster.collections." + collection + ".shards");

        var discoveredLeaders = new ArrayList<ShardLeaderReplica>();
        for (var shard : shards.entrySet()) {
            discoveredLeaders.add(leaderReplicaForShard(collection, shard.getKey(), shard.getValue()));
        }

        discoveredLeaders.sort(Comparator.comparing(ShardLeaderReplica::logicalShard));
        if (discoveredLeaders.isEmpty()) {
            throw new IllegalStateException(
                    "No active shard leaders were discovered for collection '" + collection + "'");
        }

        return List.copyOf(discoveredLeaders);
    }

    private static Uni<NamedList<Object>> requestClusterStatus(Http2SolrClient sourceClient, String collection) {
        var params = new ModifiableSolrParams();
        params.set("action", "CLUSTERSTATUS");
        params.set("collection", collection);

        var request = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/collections", params)
                .setRequiresCollection(false);

        return Uni.createFrom().completionStage(() -> sourceClient.requestAsync(request));
    }

    private static ShardLeaderReplica leaderReplicaForShard(String collection, String shardName, Object shardData) {
        var shardPath = "cluster.collections." + collection + ".shards." + shardName;
        var shard = requireObjectMap(shardData, shardPath);
        var replicas = requireObjectMap(shard.get("replicas"), shardPath + ".replicas");

        for (var replica : replicas.values()) {
            var replicaData = requireObjectMap(replica, shardPath + ".replicas.<replica>");
            if (!isActiveReplica(replicaData) || !isLeaderReplica(replicaData)) {
                continue;
            }

            var baseUrl = requiredString(replicaData, "base_url", shardPath);
            var coreName = firstNonBlank(
                    optionalString(replicaData, "core"),
                    optionalString(replicaData, "core_name"),
                    optionalString(replicaData, "coreName"));

            if (coreName == null) {
                throw new IllegalStateException(
                        "Active leader replica for shard '" + shardName + "' is missing core name metadata");
            }

            return new ShardLeaderReplica(shardName, normalizeBaseUrl(baseUrl), coreName);
        }

        throw new IllegalStateException("No ACTIVE leader replica found for shard '" + shardName + "'");
    }

    private static boolean isActiveReplica(Map<String, Object> replicaData) {
        var state = optionalString(replicaData, "state");
        return state != null && "active".equalsIgnoreCase(state);
    }

    private static boolean isLeaderReplica(Map<String, Object> replicaData) {
        var value = replicaData.get("leader");
        return switch (value) {
            case Boolean b -> b;
            case String s -> Boolean.parseBoolean(s);
            default -> false;
        };
    }

    private static String requiredString(Map<String, Object> object, String key, String path) {
        var value = optionalString(object, key);
        if (value == null) {
            throw new IllegalStateException(
                    "Required value '" + path + "." + key + "' was missing from cluster status response");
        }
        return value;
    }

    private static String optionalString(Map<String, Object> object, String key) {
        var value = object.get(key);
        if (value == null) {
            return null;
        }
        var text = value.toString().trim();
        return text.isEmpty() ? null : text;
    }

    private static Map<String, Object> requireObjectMap(Object value, String path) {
        if (value == null) {
            throw new IllegalStateException("Required object '" + path + "' was missing from cluster status response");
        }

        if (value instanceof Map<?, ?> map) {
            var converted = new LinkedHashMap<String, Object>();
            for (var entry : map.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                converted.put(entry.getKey().toString(), entry.getValue());
            }
            return converted;
        }

        if (value instanceof NamedList<?> namedList) {
            var converted = new LinkedHashMap<String, Object>();
            for (int i = 0; i < namedList.size(); i++) {
                var key = namedList.getName(i);
                if (key != null) {
                    converted.put(key, namedList.getVal(i));
                }
            }
            return converted;
        }

        throw new IllegalStateException(
                "Object '" + path + "' in cluster status response must be map-like but was "
                        + value.getClass().getSimpleName());
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

    public record ShardLeaderReplica(String logicalShard, String baseUrl, String coreName) {
    }
}
