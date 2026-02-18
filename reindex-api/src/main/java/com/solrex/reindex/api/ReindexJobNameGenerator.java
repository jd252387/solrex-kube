package com.solrex.reindex.api;

import jakarta.inject.Singleton;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.UUID;

@Singleton
public final class ReindexJobNameGenerator {
    private static final DateTimeFormatter NAME_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
        .withLocale(Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    public String newJobName(Clock clock) {
        var timestamp = NAME_TIME_FORMATTER.format(clock.instant());
        var suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 5);
        return "reindex-" + timestamp + "-" + suffix;
    }
}
