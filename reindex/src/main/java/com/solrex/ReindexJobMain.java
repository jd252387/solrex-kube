package com.solrex;

import com.solrex.reindex.job.ReindexJobRunner;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;
import lombok.RequiredArgsConstructor;

@QuarkusMain(name = "reindex-job")
@RequiredArgsConstructor
public final class ReindexJobMain implements QuarkusApplication {
    @Inject
    ReindexJobRunner reindexJobRunner;

    @Override
    public int run(String... args) {
        return reindexJobRunner.run();
    }
}
