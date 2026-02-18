package com.solrex.reindex.job;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ReindexJobConfigMappingTest {
    @Inject
    ReindexJobConfig config;

    @Test
    void shouldExposeDefaultJobConfigValues() {
        assertThat(config.requestFile()).isEqualTo(Path.of("/etc/reindex/request.yaml"));
        assertThat(config.timeout()).isEqualTo(Duration.ofMinutes(15));
    }
}
