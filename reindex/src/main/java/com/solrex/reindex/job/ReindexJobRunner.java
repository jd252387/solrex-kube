package com.solrex.reindex.job;

import com.solrex.reindex.api.DefaultReindexService;
import com.solrex.reindex.model.ReindexResult;
import jakarta.inject.Singleton;
import jakarta.validation.ConstraintViolationException;
import java.time.Duration;
import org.jboss.logging.Logger;

@Singleton
public final class ReindexJobRunner {
    private static final Logger LOG = Logger.getLogger(ReindexJobRunner.class);

    private final DefaultReindexService reindexService;
    private final ReindexRequestLoader requestLoader;
    private final ReindexJobConfig config;

    public ReindexJobRunner(
        DefaultReindexService reindexService,
        ReindexRequestLoader requestLoader,
        ReindexJobConfig config
    ) {
        this.reindexService = reindexService;
        this.requestLoader = requestLoader;
        this.config = config;
    }

    public int run() {
        try {
            var request = requestLoader.load(config.requestFile());
            LOG.infof(
                "Starting reindex. source=%s/%s target=%s/%s query=%s timeout=%s",
                request.source().cluster().baseUrl(),
                request.source().collection(),
                request.target().cluster().baseUrl(),
                request.target().collection(),
                request.filters().query(),
                config.timeout()
            );

            var result = reindexService.reindex(request)
                .await().atMost(safeTimeout(config.timeout()));

            logResult(result);

            return 0;
        } catch (ConstraintViolationException e) {
            LOG.errorf("Request validation failed: %s", e.getMessage());
            return 1;
        } catch (Exception e) {
            LOG.error("Reindex job failed.", e);
            return 1;
        }
    }

    private Duration safeTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            return Duration.ofMinutes(15);
        }
        return timeout;
    }

    private void logResult(ReindexResult result) {
        var stats = result.stats();

        LOG.infof(
            "Reindex complete. docsRead=%d docsIndexed=%d batchesSent=%d retries=%d elapsed=%s",
            stats.docsRead(),
            stats.docsIndexed(),
            stats.batchesSent(),
            stats.retries(),
            stats.elapsed()
        );
    }
}
