package com.solrex.reindex.solr;

import static org.assertj.core.api.Assertions.assertThat;

import com.solrex.reindex.model.ReindexFilters;
import com.solrex.reindex.test.ReindexRequestFixtures;
import java.util.List;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.junit.jupiter.api.Test;

class SolrSourceDocumentReaderTest {
    @Test
    void shouldAlwaysDisableDistributedQueryAndNeverSendShardsParam() {
        try (var client = new Http2SolrClient.Builder("http://source-solr:8983/solr").build()) {
            var reader = new SolrSourceDocumentReader(client);

            var request = ReindexRequestFixtures.requestWithFiltersAndFields(
                new ReindexFilters("*:*", List.of("type:book")),
                List.of("title")
            );

            var params = reader.baseReadParams(request, "id");

            assertThat(params.get(CommonParams.DISTRIB)).isEqualTo("false");
            assertThat(params.get(ShardParams.SHARDS)).isNull();
            assertThat(params.get(CommonParams.Q)).isEqualTo("*:*");
            assertThat(params.get(CommonParams.FL)).isEqualTo("title,id");
            assertThat(params.getParams(CommonParams.FQ)).containsExactly("type:book");
        }
    }
}
