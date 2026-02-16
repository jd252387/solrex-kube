package com.solrex.reindex.solr;

import java.util.Map;

public record SchemaMetadata(String uniqueKeyField, Map<String, Boolean> docValuesByField) {
    public static SchemaMetadata defaults() {
        return new SchemaMetadata("id", Map.of());
    }

    public SchemaMetadata {
        uniqueKeyField = (uniqueKeyField == null || uniqueKeyField.isBlank()) ? "id" : uniqueKeyField;
        docValuesByField = docValuesByField == null ? Map.of() : Map.copyOf(docValuesByField);
    }
}
