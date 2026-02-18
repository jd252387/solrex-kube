package com.solrex.reindex.test;

import com.solrex.reindex.model.ClusterConfig;
import com.solrex.reindex.model.CollectionRef;
import com.solrex.reindex.model.ReindexFilters;
import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexTuning;
import com.solrex.reindex.model.RetryPolicy;
import java.util.List;

public final class ReindexRequestFixtures {
    private static final String SOURCE_BASE_URL = "http://source-solr:8983/solr";
    private static final String TARGET_BASE_URL = "http://target-solr:8983/solr";
    private static final String SOURCE_COLLECTION = "source_collection";
    private static final String TARGET_COLLECTION = "target_collection";

    private ReindexRequestFixtures() {
    }

    public static ReindexRequest validRequest() {
        return request(
            sourceCluster(),
            SOURCE_COLLECTION,
            targetCluster(),
            TARGET_COLLECTION,
            ReindexFilters.defaults(),
            List.of("id", "title"),
            ReindexTuning.defaults()
        );
    }

    public static ReindexRequest requestWithTargetCollection(String targetCollection) {
        return request(
            sourceCluster(),
            SOURCE_COLLECTION,
            targetCluster(),
            targetCollection,
            ReindexFilters.defaults(),
            List.of("id"),
            ReindexTuning.defaults()
        );
    }

    public static ReindexRequest requestWithTuning(ReindexTuning tuning) {
        return request(
            sourceCluster(),
            SOURCE_COLLECTION,
            targetCluster(),
            TARGET_COLLECTION,
            ReindexFilters.defaults(),
            List.of("id"),
            tuning
        );
    }

    public static ReindexRequest requestWithSourceCluster(ClusterConfig sourceCluster) {
        return request(
            sourceCluster,
            SOURCE_COLLECTION,
            targetCluster(),
            TARGET_COLLECTION,
            ReindexFilters.defaults(),
            List.of("id"),
            ReindexTuning.defaults()
        );
    }

    public static ReindexRequest requestWithFiltersAndFields(ReindexFilters filters, List<String> fields) {
        return request(
            sourceCluster(),
            SOURCE_COLLECTION,
            targetCluster(),
            TARGET_COLLECTION,
            filters,
            fields,
            ReindexTuning.defaults()
        );
    }

    public static ReindexRequest requestWithRetryPolicy(RetryPolicy retryPolicy) {
        return request(
            sourceCluster(),
            SOURCE_COLLECTION,
            targetCluster(),
            TARGET_COLLECTION,
            new ReindexFilters("*:*", List.of()),
            List.of("id", "title"),
            new ReindexTuning(200, 4, 4, retryPolicy)
        );
    }

    public static String fullRequestYaml() {
        return """
            source:
              cluster:
                baseUrl: http://source-solr:8983/solr/
                requestTimeout: PT20S
                basicAuthUser: source-user
                basicAuthPassword: source-password
              collection: source_collection
            target:
              cluster:
                baseUrl: http://target-solr:8983/solr
                requestTimeout: PT25S
              collection: target_collection
            filters:
              query: '*:*'
              fqs:
                - type:book
            fields:
              - id
              - title
            tuning:
              readPageSize: 100
              writeBatchSize: 80
              writeConcurrency: 3
              retryPolicy:
                maxRetries: 4
                initialBackoff: PT0.2S
                maxBackoff: PT3S
                jitterFactor: 0.1
            """;
    }

    public static String requestYamlUsingDefaults() {
        return """
            source:
              cluster:
                baseUrl: http://source-solr:8983/solr
              collection: source_collection
            target:
              cluster:
                baseUrl: http://target-solr:8983/solr
              collection: target_collection
            fields:
              - id
            """;
    }

    public static String requestYamlWithUnsupportedProperty() {
        return """
            source:
              cluster:
                baseUrl: http://source-solr:8983/solr
              collection: source_collection
            target:
              cluster:
                baseUrl: http://target-solr:8983/solr
              collection: target_collection
            fields:
              - id
            unsupported: true
            """;
    }

    public static String requestYamlWithoutSource() {
        return """
            target:
              cluster:
                baseUrl: http://target-solr:8983/solr
              collection: target_collection
            fields:
              - id
            """;
    }

    public static String requestYamlWithoutTarget() {
        return """
            source:
              cluster:
                baseUrl: http://source-solr:8983/solr
              collection: source_collection
            fields:
              - id
            """;
    }

    public static String requestYamlWithoutFields() {
        return """
            source:
              cluster:
                baseUrl: http://source-solr:8983/solr
              collection: source_collection
            target:
              cluster:
                baseUrl: http://target-solr:8983/solr
              collection: target_collection
            """;
    }

    public static String requestYamlWithBlankTargetCollection() {
        return """
            source:
              cluster:
                baseUrl: http://source-solr:8983/solr
              collection: source_collection
            target:
              cluster:
                baseUrl: http://target-solr:8983/solr
              collection: ' '
            fields:
              - id
            """;
    }

    private static ReindexRequest request(
        ClusterConfig sourceCluster,
        String sourceCollection,
        ClusterConfig targetCluster,
        String targetCollection,
        ReindexFilters filters,
        List<String> fields,
        ReindexTuning tuning
    ) {
        return new ReindexRequest(
            new CollectionRef(sourceCluster, sourceCollection),
            new CollectionRef(targetCluster, targetCollection),
            filters,
            fields,
            tuning
        );
    }

    private static ClusterConfig sourceCluster() {
        return new ClusterConfig(SOURCE_BASE_URL);
    }

    private static ClusterConfig targetCluster() {
        return new ClusterConfig(TARGET_BASE_URL);
    }
}
