package com.solrex.reindex.solr;

import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.RetryPolicy;
import io.smallrye.mutiny.Uni;

public interface SchemaMetadataProvider {
    Uni<SchemaMetadata> fetch(CollectionRef source, RetryPolicy retryPolicy);
}
