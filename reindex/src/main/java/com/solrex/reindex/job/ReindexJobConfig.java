package com.solrex.reindex.job;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.nio.file.Path;
import java.time.Duration;

@ConfigMapping(prefix = "reindex.job")
public interface ReindexJobConfig {
    @WithDefault("/etc/reindex/request.yaml")
    Path requestFile();

    @WithDefault("PT15M")
    Duration timeout();
}
