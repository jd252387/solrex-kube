package com.solrex.reindex.job;

import static org.assertj.core.api.Assertions.assertThat;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlWithBlankTargetCollection;

import com.solrex.reindex.api.DefaultReindexService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReindexJobRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void shouldReturnNonZeroWhenValidationFails() throws Exception {
        var requestFile = tempDir.resolve("invalid-request.yaml");
        Files.writeString(requestFile, requestYamlWithBlankTargetCollection());

        var config = new ReindexJobConfig() {
            @Override
            public Path requestFile() {
                return requestFile;
            }

            @Override
            public Duration timeout() {
                return Duration.ofSeconds(5);
            }
        };

        var runner = new ReindexJobRunner(
            new DefaultReindexService(),
            new ReindexRequestLoader(),
            config
        );

        assertThat(runner.run()).isEqualTo(1);
    }
}
