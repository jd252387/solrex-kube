package com.solrex.reindex.solr;

import static org.assertj.core.api.Assertions.assertThat;

import com.solrex.reindex.model.ClusterConfig;
import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.FieldSelection;
import com.solrex.reindex.model.ReindexFilters;
import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexTuning;
import java.util.List;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.junit.jupiter.api.Test;

class SolrSourceDocumentReaderTest {
    @Test
    void shouldAlwaysDisableDistributedQueryAndNeverSendShardsParam() {
        try (var client = new Http2SolrClient.Builder("http://source-solr:8983/solr").build()) {
            var reader = new SolrSourceDocumentReader(
                client,
                new SolrSchemaMetadataProvider(client),
                new SchemaAwareExportEligibilityDecider(),
                new SolrShardLeaderDiscovery(client)
            );

            var request = new ReindexRequest(
                new CollectionRef(new ClusterConfig("http://source-solr:8983/solr"), "source_collection"),
                new CollectionRef(new ClusterConfig("http://target-solr:8983/solr"), "target_collection"),
                new ReindexFilters("*:*", List.of("type:book"), List.of("shard1", "shard2")),
                FieldSelection.fields(List.of("id", "title")),
                ReindexTuning.defaults()
            );

            var params = reader.baseReadParams(request, "id", request.fieldSelection());

            assertThat(params.get(CommonParams.DISTRIB)).isEqualTo("false");
            assertThat(params.get(ShardParams.SHARDS)).isNull();
            assertThat(params.get(CommonParams.Q)).isEqualTo("*:*");
            assertThat(params.getParams(CommonParams.FQ)).containsExactly("type:book");
        }
    }
}
