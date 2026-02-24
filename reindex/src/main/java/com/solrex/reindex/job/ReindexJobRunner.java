package com.solrex.reindex.job;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexStats;
import jakarta.inject.Singleton;
import jakarta.validation.ConstraintViolationException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Singleton
@RequiredArgsConstructor
@Slf4j
public final class ReindexJobRunner {
    private final ReindexService reindexService;
    private final ReindexRequest request;
    private final ReindexJobConfig config;

    public int run() {
        try {
            log.info(
                "Starting reindex. source={}/{} target={}/{} filters={} timeout={}",
                request.source().cluster().getBaseUrl(),
                request.source().collection(),
                request.target().cluster().getBaseUrl(),
                request.target().collection(),
                request.filters(),
                config.timeout()
            );

            ReindexStats stats = reindexService.reindex(request)
                .await().atMost(config.timeout()).stats();

            log.info(
                    "Reindex complete. docsRead={} docsIndexed={} batchesSent={} retries={} elapsed={}",
                    stats.docsRead(),
                    stats.docsIndexed(),
                    stats.batchesSent(),
                    stats.retries(),
                    stats.elapsed()
            );
            return 0;
        } catch (ConstraintViolationException e) {
            log.error("Request validation failed", e);
            return 1;
        } catch (Exception e) {
            log.error("Reindex job failed", e);
            return 1;
        }
    }
}
