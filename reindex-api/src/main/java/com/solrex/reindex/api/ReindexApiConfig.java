package com.solrex.reindex.api;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

@ConfigMapping(prefix = "reindex.api")
public interface ReindexApiConfig {
    K8s k8s();

    Job job();

    interface K8s {
        Optional<String> namespace();
    }

    interface Job {
        @WithDefault("solrex/reindex:latest")
        String image();

        @WithDefault("reindex-job")
        String serviceAccount();

        @WithDefault("1")
        int backoffLimit();

        @WithDefault("3600")
        int ttlSecondsAfterFinished();

        @WithDefault("/etc/reindex/request.yaml")
        String requestFile();

        @WithDefault("true")
        boolean kubernetesConfigEnabled();

        @WithDefault("true")
        boolean kubernetesConfigFailOnMissingConfig();
    }
}
