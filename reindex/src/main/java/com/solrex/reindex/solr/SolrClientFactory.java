package com.solrex.reindex.solr;

import com.solrex.reindex.model.ClusterConfig;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

public final class SolrClientFactory {
    public Http2SolrClient create(ClusterConfig config) {
        var builder = new Http2SolrClient.Builder(config.baseUrl())
            .withConnectionTimeout(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS)
            .withIdleTimeout(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS)
            .withRequestTimeout(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);

        config.basicAuthUser().ifPresent(user ->
            builder.withBasicAuthCredentials(user, config.basicAuthPassword().orElseThrow()));

        return builder.build();
    }
}
