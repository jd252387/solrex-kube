package com.solrex.reindex.api;

import com.solrex.reindex.model.ReindexRequest;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

@Path("/api/v1/reindex/jobs")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public final class ReindexJobResource {
    private final ReindexJobService reindexJobService;
    private final Validator validator;

    public ReindexJobResource(ReindexJobService reindexJobService, Validator validator) {
        this.reindexJobService = reindexJobService;
        this.validator = validator;
    }

    @POST
    public Response create(ReindexRequest request) {
        validateRequiredFields(request);

        var violations = validator.validate(request);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
        return Response.accepted(reindexJobService.create(request)).build();
    }

    private static void validateRequiredFields(ReindexRequest request) {
        List<String> details = new ArrayList<>();

        if (request == null) {
            details.add("request body must not be null");
        } else {
            if (request.source() == null) {
                details.add("source must not be null");
            }
            if (request.target() == null) {
                details.add("target must not be null");
            }
            if (request.fields() == null || request.fields().isEmpty()) {
                details.add("fields must contain at least one value");
            }
        }

        if (!details.isEmpty()) {
            throw new InvalidReindexRequestException("Invalid request payload.", details);
        }
    }
}
