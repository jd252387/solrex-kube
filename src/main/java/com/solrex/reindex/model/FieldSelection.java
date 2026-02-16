package com.solrex.reindex.model;

import com.solrex.reindex.validation.ValidationSupport;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public record FieldSelection(boolean allFields, @NotNull List<@NotBlank String> fields) {
    public FieldSelection(boolean allFields, List<String> fields) {
        this.allFields = allFields;
        this.fields = allFields ? List.of() : normalizeFields(fields);

        ValidationSupport.validate(this);
    }

    public static FieldSelection all() {
        return new FieldSelection(true, List.of());
    }

    public static FieldSelection fields(List<String> fields) {
        return new FieldSelection(false, fields);
    }

    public Set<String> effectiveFields(Optional<String> requiredField) {
        if (allFields) {
            return Set.of("*");
        }

        var values = new LinkedHashSet<>(fields);
        requiredField.ifPresent(values::add);
        return values;
    }

    private static List<String> normalizeFields(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return List.of();
        }
        return fields.stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .distinct()
            .toList();
    }

    @AssertTrue(message = "fields cannot be empty when allFields=false")
    public boolean isSelectionConsistent() {
        return allFields || !fields.isEmpty();
    }
}
