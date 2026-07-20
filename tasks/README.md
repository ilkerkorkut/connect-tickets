# Implementation Tasks

This folder converts [PRODUCT_REQUIREMENTS.md](../PRODUCT_REQUIREMENTS.md) and [INITIAL_PLAN.md](../INITIAL_PLAN.md) into an ordered execution backlog. Complete tasks in numeric order. A task is done only when its acceptance criteria and validation commands pass; do not defer its behavioral tests to the final hardening task.

## Rules that apply to every task

- Keep the locked stack: Go 1.25+, Gin, Ent, PostgreSQL, Redis, Kafka, IBM Sarama, Docker Compose, Make, and `log/slog`.
- Preserve the architecture boundaries in `AGENTS.md`.
- Propagate `context.Context` through all I/O and cancellation paths.
- Keep network calls outside PostgreSQL transactions.
- Treat PostgreSQL as authoritative and Kafka delivery as at least once.
- Use database constraints and distributed coordination for cross-replica correctness.
- Return typed errors and safe external messages; never expose credentials, raw database errors, or unrestricted upstream bodies.
- Add tests with every behavioral change, format code, run focused tests, and use the Makefile command surface.
- Do not add code comments.

## Ordered backlog

| Order | Task | Outcome | Depends on |
|---:|---|---|---|
| 01 | [Repository foundation](01-repository-foundation.md) | Buildable project layout and pinned dependencies | None |
| 02 | [Runtime configuration and bootstrap](02-runtime-configuration-and-bootstrap.md) | Validated configuration, runtime modes, dependency wiring, base logging | 01 |
| 03 | [Ent model and migrations](03-ent-model-and-migrations.md) | Correct PostgreSQL schema, constraints, indexes, and versioned migrations | 02 |
| 04 | [GitHub client](04-github-client.md) | Context-aware upstream client with typed failures | 02 |
| 05 | [Redis gateway and rate limits](05-redis-gateway-and-rate-limits.md) | Five-minute cache, distributed coalescing, and shared cooldown | 04 |
| 06 | [Persistence and application services](06-persistence-and-application-services.md) | Transactional repository use cases, SQL stats, cursor feed, and batch queries | 03, 05 |
| 07 | [Gin HTTP API](07-gin-http-api.md) | Complete REST contract with consistent errors and request IDs | 06 |
| 08 | [Kafka platform](08-kafka-platform.md) | Validated topics, producer, and manual-commit consumer group | 02 |
| 09 | [Batch and outbox publisher](09-batch-and-outbox-publisher.md) | Durable batch enqueue and recoverable PostgreSQL-to-Kafka publication | 03, 08 |
| 10 | [Refresh worker](10-refresh-worker.md) | Idempotent refresh processing and durable offset settlement | 05, 06, 08, 09 |
| 11 | [Retries, DLQ, and batch correctness](11-retries-dlq-and-batch-correctness.md) | Scheduled retries, terminal failure handling, and authoritative status | 10 |
| 12 | [Lifecycle, health, and observability](12-lifecycle-health-and-observability.md) | Graceful shutdown, readiness, correlation, and bounded resources | 07, 09, 11 |
| 13 | [Docker, Compose, and Make](13-docker-compose-and-make.md) | Reproducible local and CI workflows | 12 |
| 14 | [Acceptance tests and hardening](14-acceptance-tests-and-hardening.md) | Required concurrency and failure-path evidence | 13 |
| 15 | [README and final acceptance](15-readme-and-final-acceptance.md) | Complete documentation and verified delivery | 14 |

## Completion protocol

For each task:

1. Read the linked source sections and this task's dependencies.
2. Implement only the scoped deliverables and their tests.
3. Run the task's focused checks.
4. Run `make test-race` for concurrency-sensitive changes.
5. Run `make ci` before marking the task complete.
6. Record any conscious trade-off in the project README as soon as it is introduced.

