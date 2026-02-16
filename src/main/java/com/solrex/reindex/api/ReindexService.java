package com.solrex.reindex.api;

import com.solrex.reindex.model.ReindexRequest;
import com.solrex.reindex.model.ReindexResult;
import io.smallrye.mutiny.Uni;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public interface ReindexService {
    Uni<ReindexResult> reindex(@NotNull @Valid ReindexRequest request);
}
