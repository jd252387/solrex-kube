package com.solrex.reindex.api;

import jakarta.validation.ConstraintViolationException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.Comparator;
import java.util.List;

@Provider
public final class ReindexApiExceptionMapper implements ExceptionMapper<Throwable> {
    @Override
    public Response toResponse(Throwable exception) {
        if (exception instanceof WebApplicationException webApplicationException) {
            return webApplicationException.getResponse();
        }

        if (exception instanceof ConstraintViolationException violationException) {
            var details = violationException.getConstraintViolations()
                .stream()
                .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                .sorted(Comparator.naturalOrder())
                .toList();
            return error(Response.Status.BAD_REQUEST, ApiErrorCode.VALIDATION_ERROR, "Invalid request payload.", details);
        }

        if (exception instanceof InvalidReindexRequestException invalidRequestException) {
            return error(
                Response.Status.BAD_REQUEST,
                ApiErrorCode.VALIDATION_ERROR,
                invalidRequestException.getMessage(),
                invalidRequestException.details()
            );
        }

        if (exception instanceof ReindexJobConflictException conflictException) {
            return error(
                Response.Status.CONFLICT,
                ApiErrorCode.REINDEX_JOB_CONFLICT,
                conflictException.getMessage(),
                List.of("A job with the generated name already exists.")
            );
        }

        if (exception instanceof ReindexJobCreationException creationException) {
            return error(
                Response.Status.INTERNAL_SERVER_ERROR,
                ApiErrorCode.REINDEX_JOB_CREATE_FAILED,
                creationException.getMessage(),
                List.of("Failed to create one or more Kubernetes resources.")
            );
        }

        return error(
            Response.Status.INTERNAL_SERVER_ERROR,
            ApiErrorCode.INTERNAL_ERROR,
            "Unexpected server error.",
            List.of()
        );
    }

    private Response error(Response.Status status, ApiErrorCode code, String message, List<String> details) {
        return Response.status(status)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .entity(ApiErrorResponse.of(code, message, details))
            .build();
    }
}
