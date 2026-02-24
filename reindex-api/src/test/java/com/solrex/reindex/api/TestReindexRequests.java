package com.solrex.reindex.api;

import com.solrex.reindex.model.ClusterConfig;
import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexTuning;
import java.util.List;

final class TestReindexRequests {
    private static final ClusterConfig SOURCE_CLUSTER = new ClusterConfig("http://solr-a:8983/solr");
    private static final ClusterConfig TARGET_CLUSTER = new ClusterConfig("http://solr-b:8983/solr");

    private TestReindexRequests() {
    }

    static ReindexRequest valid() {
        return new ReindexRequest(
            sourceCollection(),
            targetCollection(),
            List.of(),
            List.of("id", "title", "category"),
            ReindexTuning.defaults()
        );
    }

    private static CollectionRef sourceCollection() {
        return new CollectionRef(SOURCE_CLUSTER, "source_collection");
    }

    private static CollectionRef targetCollection() {
        return new CollectionRef(TARGET_CLUSTER, "target_collection");
    }
}
