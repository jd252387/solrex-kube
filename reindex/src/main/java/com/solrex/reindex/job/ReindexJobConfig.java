package com.solrex.reindex.job;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;

@ConfigMapping(prefix = "reindex.job")
public interface ReindexJobConfig {
    String request();

    @WithDefault("PT15M")
    Duration timeout();
}
