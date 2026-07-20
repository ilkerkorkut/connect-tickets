# Task 13: Docker, Compose, and Make Workflows

## Objective

Deliver a pinned, health-gated, reproducible local/CI environment and make the Makefile the standard command surface.

## Depends on

- Task 12

## Source

- `PRODUCT_REQUIREMENTS.md`: Docker Compose and environment requirements
- `INITIAL_PLAN.md`: sections 23-25, Phase 1/9, and Prompt 11

## Docker image

- Use a multi-stage build with pinned Go/tool images, deterministic module download, generated-code validation, and a statically or correctly dynamically linked application as designed.
- Run as a non-root user in a minimal pinned runtime image with CA certificates and no build tools or secrets.
- Set explicit entrypoint/default mode and support the application healthcheck mode.

## Compose topology

- Provide `compose.yaml` and isolated `compose.test.yaml` services for PostgreSQL, Redis, single-node Kafka KRaft, idempotent `kafka-init`, one-shot migrations, API, worker, and publisher.
- Configure Kafka listeners that work from both host and containers.
- Create `github.repo.refresh.v1` with exactly five partitions and the DLQ topic; topic initialization must be repeatable and validate incompatible existing topics.
- Use real health checks. Migration waits for PostgreSQL; topic init waits for Kafka; application roles wait for healthy PostgreSQL/Redis/Kafka plus successful migrations/topic initialization.
- Use named data volumes. Normal shutdown preserves them; a clearly destructive reset removes them.
- Keep tests isolated by project name, ports, networks, volumes, databases, and topics, and clean them reliably.

## Makefile surface

- Self-documenting `help`, `.PHONY` declarations, overridable variables, consistent Compose project name, commands that fail on errors, and pinned tools in `./bin`.
- Setup/generation: `bootstrap`, `generate`, `ent-generate`, `generate-check`.
- Quality: `fmt`, `fmt-check`, `vet`, `lint`, `test`, `test-unit`, `test-race`, `test-integration`, `test-all`, `ci`.
- Build/run: `build`, `run-api`, `run-worker`, `run-publisher`, `run-all`.
- Infrastructure: `infra-up`, `infra-down`, `infra-logs`.
- Full lifecycle: `docker-up`, `docker-down`, `docker-logs`, explicit `docker-reset`.
- Database: migration create/up/down/status targets with destructive actions explicit.
- Kafka: topic describe, consumer-group inspect, safe consume, and confirmation-gated destructive reset.
- Smoke: base, concurrency, refresh-batch, and shutdown suites.
- `ci` order: generate check, format check, vet, lint, unit, race, integration, then build.

## Tests and verification

- Build and inspect the runtime image for user, certificates, binary, entrypoint, and absence of credentials/build tools.
- Start from empty volumes and prove health/dependency gating, migrations, exact partition count, and all application roles.
- Restart services to prove persistence and application tolerance of dependencies disappearing after startup.
- Verify `docker-down` preserves data and `docker-reset` is explicit/destructive.
- Verify `generate-check` detects generated drift and test Compose always cleans up.

```bash
make bootstrap
make ci
make docker-reset
make docker-up
make kafka-describe
make smoke
```

## Done when

- A clean machine with Docker, Make, and the documented prerequisites can run the complete system.
- Compose never relies only on startup order; health and one-shot completion gates are real.
- The same Make targets drive local development and CI.

