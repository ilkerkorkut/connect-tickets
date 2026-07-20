# AGENTS.md

## Project

Greenfield production-shaped GitHub repository tracker.

Locked stack:

- Go 1.25+
- Gin (https://gin-gonic.com/en/docs/)
- Ent
- PostgreSQL
- Redis
- Apache Kafka
- `github.com/IBM/sarama`
- Docker Compose
- Make
- `log/slog`

Do not replace locked technologies.

## Core Rules

- PostgreSQL is the source of truth.
- Kafka delivery is at least once; all worker effects must be idempotent.
- Use the transactional outbox for PostgreSQL-to-Kafka publication.
- Disable Kafka auto-commit. Commit offsets only after a durable PostgreSQL outcome.
- Never call GitHub, Redis, or Kafka inside a database transaction.
- Propagate `context.Context` through HTTP, database, Redis, Kafka, and GitHub paths.
- Use database constraints for correctness under concurrency.
- Use Redis for the five-minute GitHub cache, distributed cache-miss lock, and shared rate-limit cooldown.
- Do not use process-local locks for cross-replica guarantees.
- Do not log tokens, credentials, raw database errors, or unrestricted upstream bodies.
- Do not invent IBM Sarama or Ent APIs; verify against the pinned versions.

## Architecture Boundaries

- Gin handlers validate, call application services, and render responses.
- Handlers must not directly use Ent, Redis, Kafka, or the GitHub client.
- Ent stores own persistence details.
- The GitHub gateway owns HTTP calls, cache behavior, locking, and rate-limit classification.
- The outbox publisher owns Kafka publication.
- The worker owns job claiming, retries, DLQ decisions, and offset settlement.
- PostgreSQL batch-job rows are authoritative for batch status.

## Kafka Requirements

- Main topic: `github.repo.refresh.v1`
- DLQ topic: `github.repo.refresh.dlq.v1`
- Consumer group: `github-repo-refresh-v1`
- Main topic must have exactly five partitions.
- Use `repo_id` as the message key.
- Process one active record per partition.
- Do not sleep for long retries inside a Kafka handler.
- Schedule retries through a future PostgreSQL outbox event.
- Maximum three GitHub processing attempts.
- Duplicate delivery and duplicate publication must be harmless.

## API Requirements

Use one error shape:

```json
{
  "error": {
    "code": "error_code",
    "message": "Safe message",
    "request_id": "..."
  }
}
```

Important guarantees:

- Concurrent duplicate repository creation: one success, remaining requests receive HTTP 409.
- Refreshes preserve user notes.
- Statistics are calculated in SQL.
- Repository changes use a commit-ordered cursor and deletion tombstones.
- Request IDs and job IDs must be added to context and structured logs.

## Development Workflow

Use the Makefile as the standard command surface.

Common commands:

```bash
make bootstrap
make infra-up
make run-all
make test-unit
make test-race
make test-integration
make ci
make docker-up
make smoke
```

Before completing a change:

1. Format code.
2. Run focused tests.
3. Run `go test -race` for concurrency changes.
4. Run `make ci`.
5. Confirm Docker startup for infrastructure or lifecycle changes.
6. Review generated code manually.

## Testing Expectations

Every behavioral change requires tests.

Prioritize tests for:

- Concurrent duplicate POST
- Redis exactly-one-call cache behavior
- Lock expiry and ownership-safe release
- Kafka duplicate delivery
- Outbox duplicate publication
- Crash after database commit but before offset commit
- Retry scheduling and DLQ
- Global worker concurrency of five
- Change-feed commit ordering
- Graceful API and worker shutdown

Use deterministic synchronization instead of arbitrary `time.Sleep` where possible.

## Code Quality

- Keep interfaces narrow and use-case driven.
- Avoid generic repository/service abstractions.
- Return typed errors from lower layers.
- Configure explicit timeouts and resource limits.
- Keep database transactions short.
- Prefer small, reviewable changes.
- Do not ignore errors.
- Do not put any code comments.
- Do not leave TODOs that affect correctness, retries, shutdown, or security.

## Definition of Done

A change is complete only when:

- It compiles.
- Relevant tests pass.
- Concurrency-sensitive tests pass under the race detector.
- Error and shutdown paths are handled.
- Logs contain required correlation fields.
- Documentation or environment examples are updated when behavior changes.
- The implementation remains consistent with the project development and architecture specification.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

When the user types `/graphify`, invoke the `skill` tool with `skill: "graphify"` before doing anything else.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
