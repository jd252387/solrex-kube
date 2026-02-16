package com.solrex.reindex.model;

import com.solrex.reindex.validation.ValidationSupport;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CollectionRef(@NotNull @Valid ClusterConfig cluster, @NotBlank String collection) {
    public CollectionRef(ClusterConfig cluster, String collection) {
        this.cluster = cluster;
        this.collection = collection == null ? null : collection.trim();

        ValidationSupport.validate(this);
    }
}
