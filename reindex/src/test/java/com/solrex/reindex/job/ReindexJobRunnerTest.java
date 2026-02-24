package com.solrex.reindex.job;

import static org.assertj.core.api.Assertions.assertThat;
import static com.solrex.reindex.test.ReindexRequestFixtures.requestYamlUsingDefaults;

import com.solrex.reindex.solr.SolrClientFactory;
import jakarta.validation.Validation;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ReindexJobRunnerTest {
    @Test
    void shouldReturnNonZeroWhenReindexFails() {
        var config = config(requestYamlUsingDefaults());
        var validator = Validation.buildDefaultValidatorFactory().getValidator();
        var request = new ReindexRequestConfigProducer(config, validator).reindexRequest();
        var service = new ReindexService(new SolrClientFactory(), validator);

        var runner = new ReindexJobRunner(service, request, config);

        assertThat(runner.run()).isEqualTo(1);
    }

    private ReindexJobConfig config(String requestYaml) {
        return new ReindexJobConfig() {
            @Override
            public String request() {
                return requestYaml;
            }

            @Override
            public Duration timeout() {
                return Duration.ofSeconds(5);
            }
        };
    }
}
