package com.solrex.reindex.solr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.Test;

class SolrShardLeaderDiscoveryTest {
    @Test
    void shouldExtractOneActiveLeaderReplicaPerLogicalShard() {
        var response = clusterStatus(
            Map.of(
                "shard2", shard(Map.of(
                    "core_node3",
                    replica("active", "true", "http://node2:8983/solr/", "source_collection_shard2_replica_n1")
                )),
                "shard1", shard(Map.of(
                    "core_node1",
                    replica("active", true, "http://node1:8983/solr", "source_collection_shard1_replica_n1"),
                    "core_node2",
                    replica("active", false, "http://node1:7574/solr", "source_collection_shard1_replica_n2")
                ))
            )
        );

        var leaders = SolrShardLeaderDiscovery.extractShardLeaders(response, "source_collection", List.of());

        assertThat(leaders).containsExactly(
            new SolrShardLeaderDiscovery.ShardLeaderReplica(
                "shard1",
                "http://node1:8983/solr",
                "source_collection_shard1_replica_n1"
            ),
            new SolrShardLeaderDiscovery.ShardLeaderReplica(
                "shard2",
                "http://node2:8983/solr",
                "source_collection_shard2_replica_n1"
            )
        );
    }

    @Test
    void shouldFilterDiscoveredLeadersByRequestedSourceShards() {
        var response = clusterStatus(
            Map.of(
                "shard1", shard(Map.of("core_node1", replica("active", true, "http://node1:8983/solr", "core1"))),
                "shard2", shard(Map.of("core_node2", replica("active", true, "http://node2:8983/solr", "core2")))
            )
        );

        var leaders = SolrShardLeaderDiscovery.extractShardLeaders(
            response,
            "source_collection",
            List.of("shard2")
        );

        assertThat(leaders).containsExactly(
            new SolrShardLeaderDiscovery.ShardLeaderReplica("shard2", "http://node2:8983/solr", "core2")
        );
    }

    @Test
    void shouldRejectUnknownRequestedSourceShardNames() {
        var response = clusterStatus(
            Map.of("shard1", shard(Map.of("core_node1", replica("active", true, "http://node1:8983/solr", "core1"))))
        );

        assertThatThrownBy(() ->
            SolrShardLeaderDiscovery.extractShardLeaders(response, "source_collection", List.of("shard9"))
        )
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("shard9");
    }

    @Test
    void shouldFailWhenShardHasNoActiveLeaderReplica() {
        var response = clusterStatus(
            Map.of(
                "shard1",
                shard(Map.of("core_node1", replica("active", false, "http://node1:8983/solr", "core1")))
            )
        );

        assertThatThrownBy(() -> SolrShardLeaderDiscovery.extractShardLeaders(response, "source_collection", List.of()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No ACTIVE leader replica found for shard 'shard1'");
    }

    private NamedList<Object> clusterStatus(Map<String, Object> shards) {
        var response = new NamedList<Object>();
        response.add(
            "cluster",
            Map.of(
                "collections",
                Map.of(
                    "source_collection",
                    Map.of("shards", shards)
                )
            )
        );
        return response;
    }

    private Map<String, Object> shard(Map<String, Object> replicas) {
        return Map.of("replicas", replicas);
    }

    private Map<String, Object> replica(String state, Object leader, String baseUrl, String core) {
        return Map.of(
            "state", state,
            "leader", leader,
            "base_url", baseUrl,
            "core", core
        );
    }
}
