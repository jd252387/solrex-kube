package com.solrex.reindex.solr;

import java.util.List;

public record ExportDecision(boolean useExport, List<String> reasonsIfNotEligible) {
    public ExportDecision {
        reasonsIfNotEligible = reasonsIfNotEligible == null ? List.of() : List.copyOf(reasonsIfNotEligible);
    }

    public static ExportDecision enabled() {
        return new ExportDecision(true, List.of());
    }

    public static ExportDecision disabled(List<String> reasons) {
        return new ExportDecision(false, reasons);
    }
}
