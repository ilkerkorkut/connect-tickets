# Task 14: Acceptance Tests and Production Hardening

## Objective

Audit the whole system against the required concurrency, crash-window, retry, shutdown, and security behaviors, filling only gaps not already proven by earlier task tests.

## Depends on

- Task 13

## Source

- `PRODUCT_REQUIREMENTS.md`: Production Requirements and Ground Rules
- `INITIAL_PLAN.md`: sections 26-27, Phase 9, Prompt 12, and Final Acceptance Checklist
- `AGENTS.md`: Testing Expectations and Definition of Done

## Required evidence matrix

- Concurrent duplicate POST: one 201, remaining 409, one row, no raw database failure.
- Cache coalescing: one GitHub call for concurrent same-key misses under the documented Redis/TTL assumptions.
- Lock-holder failure: expiry recovery and ownership-safe release.
- Kafka round trip: correct key/schema/headers and manual offset behavior.
- Duplicate Kafka delivery: no corruption, duplicate change, or batch double-count.
- Crash after PostgreSQL success but before offset commit: replay is harmless.
- Outbox duplicate publication: crash after publish, lease recovery, republish, and worker idempotency.
- Kafka unavailable during refresh-all: HTTP 202 after durable batch, pending outbox, later recovery.
- Retry scheduling: durable future outbox event, jitter/backoff, and no early publication.
- Retry exhaustion and DLQ: exactly three counted attempts, authoritative failed row, safe DLQ.
- Global worker concurrency: maximum five and one active record per partition.
- Change feed: commit ordering under interleaved transactions, repeat tolerance, and deletion tombstones.
- Graceful API shutdown: stop acceptance and drain.
- Graceful worker shutdown/rebalance: unfinished record is not lost or wrongly committed.
- SQL statistics: aggregation stays in SQL with deterministic language tie behavior.

## Hardening audit

- Run `go vet`, static analysis, race tests, dependency vulnerability checks, and committed lint configuration.
- Review every transaction boundary for forbidden GitHub/Redis/Kafka calls.
- Review all context paths, goroutine ownership, channel closing, leases, retries, error classification, log redaction, and bounded resource settings.
- Verify generated Ent output and migrations manually.
- Use deterministic synchronization and injectable clocks/randomness instead of arbitrary sleeps wherever possible.
- Confirm all errors and shutdown paths close resources and preserve redelivery.

## Validation

Run from a clean state:

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

- Every evidence item has a named automated test and passes reliably.
- `make ci` and all clean Docker smoke suites pass.
- The race detector, linter, vet, and vulnerability scan have no unexplained findings.
- No correctness requirement relies only on a mock when the behavior belongs to PostgreSQL, Redis, or Kafka semantics.

