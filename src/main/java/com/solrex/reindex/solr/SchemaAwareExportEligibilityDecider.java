package com.solrex.reindex.solr;

import com.solrex.reindex.model.ReindexRequest;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public final class SchemaAwareExportEligibilityDecider implements ExportEligibilityDecider {
    @Override
    public ExportDecision decide(ReindexRequest request, SchemaMetadata schemaMetadata) {
        if (request.fieldSelection().allFields()) {
            return ExportDecision.disabled(List.of("/export disabled because all fields were requested"));
        }

        var reasons = new ArrayList<String>();
        var requiredFields = new LinkedHashSet<>(request.fieldSelection().fields());
        requiredFields.add(schemaMetadata.uniqueKeyField());

        if (requiredFields.isEmpty()) {
            reasons.add("No fields were selected for export");
        }

        for (String field : requiredFields) {
            var docValues = schemaMetadata.docValuesByField().get(field);
            if (!Boolean.TRUE.equals(docValues)) {
                reasons.add("Field '%s' is not docValues-enabled in source schema".formatted(field));
            }
        }

        if (!reasons.isEmpty()) {
            return ExportDecision.disabled(reasons);
        }

        return ExportDecision.enabled();
    }
}
