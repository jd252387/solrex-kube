# Repository Guidelines

## Project Structure & Module Organization
- `reindex/`: one-shot Quarkus job (`src/main/java/com/solrex/reindex/...`).
- `reindex-api/`: Quarkus REST API that creates Kubernetes-backed reindex jobs.
- `reindex-common/`: shared request and validation models for both modules.
- `deploy/k8s/`: raw Kubernetes manifests; `deploy/helm/reindex-job/`: Helm chart for job deployment.
- `docker-compose.yml`: local dual Solr + ZooKeeper stack for integration-style testing.
- Tests live in each module under `src/test/java`, mirroring production package paths.

## Build, Test, and Development Commands
```bash
./gradlew clean build                   # compile and test all modules
./gradlew test                          # run all unit tests
./gradlew :reindex:test                 # run reindex module tests
./gradlew :reindex-api:test             # run API module tests
./gradlew :reindex:quarkusDev           # dev mode for reindex job
./gradlew :reindex-api:quarkusDev       # dev mode for API
./gradlew :reindex:quarkusBuild         # produce reindex JVM build artifacts
docker compose up -d                    # start local Solr/ZK services
```

## Coding Style & Naming Conventions
- Java 21 toolchain, 4-space indentation, and standard Java brace/whitespace style.
- Packages use `com.solrex...`; classes `PascalCase`; methods/fields `camelCase`; constants `UPPER_SNAKE_CASE`.
- Simplicity is a hard requirement: write the least code that correctly solves the problem.
- Prefer straightforward local logic; do not add interfaces, abstractions, classes, or records without a concrete near-term need.
- Optimize for readability in the current codebase, not speculative extensibility.
- Do not reinvent the wheel; use mature third-party libraries for standard concerns.
- Keep shared DTOs and validation annotations in `reindex-common`, not duplicated in module-specific code.
- Prefer Lombok to remove boilerplate: use `@RequiredArgsConstructor` (with `final` dependencies), `@Getter`/`@Setter` where needed, plus `@NonNull` and `@Builder` when helpful.
- No formatter or lint gate is configured; use `./gradlew build` as the baseline quality check.

## Testing Guidelines
- Test stack: JUnit 5, AssertJ, Mockito, and REST Assured (API module).
- Name test classes `*Test.java`; keep tests in the same package structure as source files.
- Add tests for all behavior changes, including validation failures and retry/backpressure edge cases.
- No coverage threshold is currently enforced; cover happy-path plus key failure-path scenarios.

## Commit & Pull Request Guidelines
- Existing history mixes generic subjects (`commit`) and conventional prefixes (`feat: ...`); use clear, imperative messages, preferably `<type>: <summary>`.
- Keep PRs focused and include change rationale, impacted modules (`reindex`, `reindex-api`, `reindex-common`), verification commands run (for example, `./gradlew test`), and request/response or manifest snippets when behavior changes.
