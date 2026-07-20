# Task 15: README and Final Acceptance

## Objective

Document the delivered system, its guarantees and limits, the conscious trade-offs, and the AI-assisted verification workflow; then complete the final acceptance audit.

## Depends on

- Task 14

## Source

- `PRODUCT_REQUIREMENTS.md`: Ground Rules
- `INITIAL_PLAN.md`: sections 28 and 31
- `AGENTS.md`: Definition of Done

## README content

- Service purpose, capabilities, and all runtime modes.
- Prerequisites, environment setup, optional GitHub token, Make workflows, local infrastructure, full Docker startup, migrations, tests, smoke tests, and cleanup/reset behavior.
- Architecture and request/job flows with clear Gin, service, Ent, GitHub/Redis, Kafka, outbox, and worker ownership boundaries.
- Why Kafka and IBM Sarama were selected; at-least-once delivery, manual offsets, five partitions, `repo_id` keying, consumer-group behavior, and retry scheduling outside handlers.
- PostgreSQL/Kafka dual-write problem, transactional outbox claims/leases, duplicate publication window, and worker idempotency.
- Redis cache keys/TTL, lock acquisition/recheck, ownership-safe release, lock-holder death, cross-replica guarantee scope, and Redis failure policy.
- Shared rate-limit cooldown and interaction among Retry-After, reset headers, backoff, jitter, and attempts.
- Mutable keyset pagination limitation.
- Commit-ordered change cursor, at-least-once reads, `event_id` deduplication, and deletion tombstones.
- Graceful startup/shutdown and what happens to in-flight HTTP, worker, and publisher operations.
- Test strategy and which tests use real PostgreSQL, Redis, and Kafka.
- Conscious trade-offs: queue/client choice, delivery semantics, sequence-row serialization, cache lock TTL/guarantee, pagination, conservative crash attempt accounting, and operational complexity.
- AI workflow: prompts/agents/tools used, how generated APIs were verified against pinned versions, tests/static analysis/review performed, and examples of corrected AI output. Do not claim work or validation that did not occur.

## Final acceptance audit

- Confirm locked stack, optional-token behavior, and no secrets.
- Confirm every API endpoint, status/error shape, body/page limit, request ID, SQL stats, cursor behavior, and change feed.
- Confirm database constraints, notes preservation, commit-ordered changes, and authoritative batch rows.
- Confirm Redis cache/coalescing/cooldown and exact documented guarantee boundary.
- Confirm Kafka topics/group/key/manual commits/five-partition concurrency/outbox/retry/DLQ/idempotency.
- Confirm health, graceful shutdown, Docker/Compose topology, Make targets, tests, race detector, and clean smoke runs.
- Re-read generated code, migrations, environment examples, and logs for unsafe output.

## Validation

```bash
make ci
make docker-reset
make docker-up
make smoke
make smoke-concurrency
make smoke-refresh-batch
make smoke-shutdown
```

## Done when

- A reviewer can run and understand the service using the README alone.
- Every trade-off and guarantee is stated precisely, including limitations.
- Every checkbox in `INITIAL_PLAN.md` section 31 is backed by code, automated evidence, or explicit documentation.
- The implementation compiles, required tests pass, clean Docker startup succeeds, and no required work remains.
