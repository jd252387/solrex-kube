# Simplification Backlog

This backlog lists concrete simplifications intentionally deferred from the current high-impact refactor.

## 1) Split Solr response parsing into small local helpers
Status: Completed on 2026-02-18.
- Current hotspot: `reindex/src/main/java/com/solrex/reindex/solr/SolrShardLeaderDiscovery.java`
- Proposed simplification: Replace generic `requiredObject/valueOf/entries` helpers with explicit typed parsing for the exact Solr response shape used.
- Expected impact: Better readability and easier debugging in shard leader discovery; lower cognitive load.
- Prerequisite: None.
- Suggested tests: Add focused tests for malformed `cluster` and `collections` payloads with explicit error messages.

## 2) Remove static validation utility and use injected Validator
Status: Completed on 2026-02-18.
- Current hotspot: `reindex/src/main/java/com/solrex/reindex/validation/ValidationSupport.java`
- Proposed simplification: Inject `jakarta.validation.Validator` into `DefaultReindexService` and remove static global validator bootstrapping.
- Expected impact: Less hidden global state and clearer runtime wiring.
- Prerequisite: None.
- Suggested tests: Add service test that injects a validator and verifies validation failure path without static bootstrap.

## 3) Collapse request YAML loading and validation into one step
Status: Completed on 2026-02-18.
- Current hotspot: `reindex/src/main/java/com/solrex/reindex/job/ReindexRequestLoader.java`
- Proposed simplification: Load YAML and immediately validate required fields in the loader, so `ReindexJobRunner` does not need to reason about request shape assumptions.
- Expected impact: Shorter runner logic and fewer distributed validation concerns.
- Prerequisite: Decide whether loader should return typed validation errors vs generic runtime exceptions.
- Suggested tests: Add loader tests that assert clear messages for missing `source`, `target`, and `fields`.

## 4) Tighten retry metrics to count actual retries only
Status: Completed on 2026-02-18.
- Current hotspot: `reindex/src/main/java/com/solrex/reindex/pipeline/ReindexPipeline.java`
- Proposed simplification: Track retry count from a retry callback/hook that runs only when a retry is scheduled.
- Expected impact: More accurate operational metrics and easier interpretation of retry behavior.
- Prerequisite: Decide if metrics should represent retry attempts or retryable failures.
- Suggested tests: Add pipeline test asserting retry counter behavior for "eventual success" and "exhausted retries".

## 5) Move Kubernetes Job/ConfigMap labels/env names to constants
Status: Completed on 2026-02-18.
- Current hotspot: `reindex-api/src/main/java/com/solrex/reindex/api/ReindexJobService.java`
- Proposed simplification: Extract repeated label keys and env variable names into private constants.
- Expected impact: Fewer string literals, lower typo risk, easier edits.
- Prerequisite: None.
- Suggested tests: Add a service test that validates required env variables are always present in created Job spec.

## 6) Simplify error response creation around an enum code model
Status: Completed on 2026-02-18.
- Current hotspot: `reindex-api/src/main/java/com/solrex/reindex/api/ReindexApiExceptionMapper.java`
- Proposed simplification: Replace string error codes with a small enum and a single `ApiErrorResponse` factory.
- Expected impact: Prevents drift in error code naming and reduces duplicate mapper literals.
- Prerequisite: Decide whether error codes are public API or internal.
- Suggested tests: Add mapper unit tests for each exception-to-code mapping.

## 7) Remove test-only deep object builders by introducing fixture factories
Status: Completed on 2026-02-18.
- Current hotspot: `reindex/src/test/java/com/solrex/reindex/job/ReindexRequestLoaderTest.java`, `reindex-api/src/test/java/com/solrex/reindex/api/TestReindexRequests.java`
- Proposed simplification: Introduce one shared test fixture helper per module for `ReindexRequest` and YAML snippets.
- Expected impact: Less duplicated test setup and faster evolution of request schema.
- Prerequisite: Decide whether to keep module-local fixtures vs shared test-fixtures Gradle source set.
- Suggested tests: N/A (refactor tests only).

## 8) Decide whether shard-level targeting should be fully removed from deployment docs
Status: Completed on 2026-02-18.
- Current hotspot: `README.md`, `deploy/k8s/reindex-configmap.yaml`, `deploy/helm/reindex-job/values.yaml`
- Proposed simplification: Remove any residual references to advanced tuning fields and document only supported request schema.
- Expected impact: Less operator confusion and cleaner onboarding path.
- Prerequisite: Confirm this repository no longer intends to support those advanced request options.
- Suggested tests: Add a smoke test in CI that validates example request snippets deserialize into `ReindexRequest`.
