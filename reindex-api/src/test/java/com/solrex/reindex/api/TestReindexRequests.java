package com.solrex.reindex.api;

import com.solrex.reindex.model.ClusterConfig;
import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.ReindexFilters;
import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexTuning;
import java.util.List;

final class TestReindexRequests {
    private TestReindexRequests() {
    }

    static ReindexRequest valid() {
        return new ReindexRequest(
            new CollectionRef(new ClusterConfig("http://solr-a:8983/solr"), "source_collection"),
            new CollectionRef(new ClusterConfig("http://solr-b:8983/solr"), "target_collection"),
            ReindexFilters.defaults(),
            List.of("id", "title", "category"),
            ReindexTuning.defaults()
        );
    }
}
