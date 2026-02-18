package com.solrex.reindex.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class ReindexJobNameGeneratorTest {
    private final ReindexJobNameGenerator generator = new ReindexJobNameGenerator();

    @Test
    void createsExpectedNameFormat() {
        var clock = Clock.fixed(Instant.parse("2026-02-18T16:20:30Z"), ZoneOffset.UTC);

        var jobName = generator.newJobName(clock);

        assertThat(jobName).matches("reindex-20260218162030-[a-f0-9]{5}");
    }

    @Test
    void generatesUniqueNamesAcrossInvocations() {
        var clock = Clock.fixed(Instant.parse("2026-02-18T16:20:30Z"), ZoneOffset.UTC);

        var first = generator.newJobName(clock);
        var second = generator.newJobName(clock);

        assertThat(second).isNotEqualTo(first);
    }
}
