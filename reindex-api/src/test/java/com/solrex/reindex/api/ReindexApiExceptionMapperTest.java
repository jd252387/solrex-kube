package com.solrex.reindex.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Path;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReindexApiExceptionMapperTest {
    private final ReindexApiExceptionMapper mapper = new ReindexApiExceptionMapper();

    @Test
    void shouldPassThroughWebApplicationExceptions() {
        var response = Response.status(418).build();
        var exception = new WebApplicationException(response);

        var mapped = mapper.toResponse(exception);

        assertThat(mapped).isSameAs(response);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldMapConstraintViolationExceptionToValidationError() {
        ConstraintViolation<Object> violation = mock(ConstraintViolation.class);
        Path path = mock(Path.class);
        when(path.toString()).thenReturn("request.source.collection");
        when(violation.getPropertyPath()).thenReturn(path);
        when(violation.getMessage()).thenReturn("must not be blank");

        var response = mapper.toResponse(new ConstraintViolationException(Set.of(violation)));
        var error = (ApiErrorResponse) response.getEntity();

        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(error.error()).isEqualTo("VALIDATION_ERROR");
        assertThat(error.message()).isEqualTo("Invalid request payload.");
        assertThat(error.details()).containsExactly("request.source.collection must not be blank");
    }

    @Test
    void shouldMapInvalidReindexRequestExceptionToValidationError() {
        var response = mapper.toResponse(new InvalidReindexRequestException("Invalid payload", List.of("source missing")));
        var error = (ApiErrorResponse) response.getEntity();

        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(error.error()).isEqualTo("VALIDATION_ERROR");
        assertThat(error.message()).isEqualTo("Invalid payload");
        assertThat(error.details()).containsExactly("source missing");
    }

    @Test
    void shouldMapConflictExceptionToConflictErrorCode() {
        var response = mapper.toResponse(new ReindexJobConflictException("Conflict", null));
        var error = (ApiErrorResponse) response.getEntity();

        assertThat(response.getStatus()).isEqualTo(409);
        assertThat(error.error()).isEqualTo("REINDEX_JOB_CONFLICT");
    }

    @Test
    void shouldMapCreationExceptionToCreationFailedErrorCode() {
        var response = mapper.toResponse(new ReindexJobCreationException("Create failed", null));
        var error = (ApiErrorResponse) response.getEntity();

        assertThat(response.getStatus()).isEqualTo(500);
        assertThat(error.error()).isEqualTo("REINDEX_JOB_CREATE_FAILED");
    }

    @Test
    void shouldMapUnexpectedExceptionsToInternalErrorCode() {
        var response = mapper.toResponse(new RuntimeException("boom"));
        var error = (ApiErrorResponse) response.getEntity();

        assertThat(response.getStatus()).isEqualTo(500);
        assertThat(error.error()).isEqualTo("INTERNAL_ERROR");
        assertThat(error.message()).isEqualTo("Unexpected server error.");
    }
}
