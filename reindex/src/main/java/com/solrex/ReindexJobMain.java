package com.solrex;

import com.solrex.reindex.job.ReindexJobRunner;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;

@QuarkusMain(name = "reindex-job")
public final class ReindexJobMain {
    private ReindexJobMain() {
    }

    public static void main(String... args) {
        Quarkus.run(ReindexApplication.class, args);
    }

    public static class ReindexApplication implements QuarkusApplication {
        @Inject
        ReindexJobRunner reindexJobRunner;

        @Override
        public int run(String... args) {
            return reindexJobRunner.run();
        }
    }
}
