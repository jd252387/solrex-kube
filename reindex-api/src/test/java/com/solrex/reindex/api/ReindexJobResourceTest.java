package com.solrex.reindex.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ReindexJobResourceTest {
    @InjectMock
    ReindexJobService reindexJobService;

    @BeforeEach
    void setUp() {
        reset(reindexJobService);
        when(reindexJobService.create(any())).thenReturn(
            CreateReindexJobResponse.accepted(
                "reindex-20260218162500-abcde",
                "solrex",
                "reindex-20260218162500-abcde-request",
                OffsetDateTime.parse("2026-02-18T16:25:00Z")
            )
        );
    }

    @Test
    void createReturnsAcceptedResponse() {
        given()
            .contentType("application/json")
            .body(TestReindexRequests.valid())
            .when()
            .post("/api/v1/reindex/jobs")
            .then()
            .statusCode(202)
            .body("status", equalTo("ACCEPTED"))
            .body("jobNamespace", equalTo("solrex"))
            .body("jobName", matchesPattern("reindex-[0-9]{14}-[a-f0-9]{5}"))
            .body("requestConfigMapName", matchesPattern("reindex-[0-9]{14}-[a-f0-9]{5}-request"));

        verify(reindexJobService).create(any());
    }

    @Test
    void invalidPayloadReturnsBadRequest() {
        given()
            .contentType("application/json")
            .body("{}")
            .when()
            .post("/api/v1/reindex/jobs")
            .then()
            .statusCode(400)
            .body("error", equalTo("VALIDATION_ERROR"));
    }

    @Test
    void conflictErrorReturnsConflict() {
        doThrow(new ReindexJobConflictException("Conflict", null))
            .when(reindexJobService)
            .create(any());

        given()
            .contentType("application/json")
            .body(TestReindexRequests.valid())
            .when()
            .post("/api/v1/reindex/jobs")
            .then()
            .statusCode(409)
            .body("error", equalTo("REINDEX_JOB_CONFLICT"));
    }

    @Test
    void creationErrorReturnsInternalServerError() {
        doThrow(new ReindexJobCreationException("Boom", null))
            .when(reindexJobService)
            .create(any());

        given()
            .contentType("application/json")
            .body(TestReindexRequests.valid())
            .when()
            .post("/api/v1/reindex/jobs")
            .then()
            .statusCode(500)
            .body("error", equalTo("REINDEX_JOB_CREATE_FAILED"));
    }
}
