package com.solrex.reindex.solr;

import com.solrex.reindex.model.ClusterConfig;
import jakarta.inject.Singleton;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

@Singleton
public final class SolrClientFactory {
    public Http2SolrClient create(ClusterConfig config) {
        var builder = new Http2SolrClient.Builder(config.baseUrl())
            .withConnectionTimeout(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS)
            .withIdleTimeout(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS)
            .withRequestTimeout(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);

        if (config.basicAuthUser() != null && config.basicAuthPassword() != null) {
            builder.withBasicAuthCredentials(config.basicAuthUser(), config.basicAuthPassword());
        }

        return builder.build();
    }
}
