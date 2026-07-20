# Task 11: Retry Scheduling, DLQ, and Batch Correctness

## Objective

Add durable, non-blocking retry scheduling, terminal failure publication, and idempotent batch completion on top of the refresh worker.

## Depends on

- Task 10

## Source

- `PRODUCT_REQUIREMENTS.md`: Worker requirements 2-4
- `INITIAL_PLAN.md`: sections 12, 16-18, Phase 8, and Prompt 10

## Retry classification and scheduling

- Classify GitHub 5xx, 429/rate limiting, timeout, selected network failures, and Redis coordination failure as retryable according to explicit policy.
- Treat not found, unauthorized as configured by the specification, invalid message, and retry exhaustion as terminal with safe codes/messages.
- Never sleep through a long backoff in a Kafka partition handler.
- Calculate `next_attempt_at` from exponential backoff, random jitter, `Retry-After`, rate-limit reset, and the shared Redis cooldown. It must never precede GitHub's required retry time.
- In one transaction, set the job to `retry_wait`, persist safe error data and `next_attempt_at`, finish the attempt, and insert a new main-topic outbox event whose `available_at` is the retry time.
- Commit the current Kafka record only after that transaction commits.
- A shared cooldown encountered before the GitHub path schedules a future event without incrementing attempt count.

## Terminal and dead-letter (DLQ) flow

- At a permanent failure or after the third consumed attempt, atomically mark the job failed, store a safe terminal error, finish the attempt, and create a DLQ outbox event.
- Commit the main-topic record only after the failure transaction commits.
- Include schema version, new and original event IDs, job/batch/repository IDs, attempt count, safe error, and failure time in the DLQ payload.
- Do not include credentials, raw database errors, or unrestricted GitHub bodies.
- PostgreSQL failure state remains authoritative even if DLQ publication is delayed or duplicated.

## Batch correctness

- Derive counts from job rows; never increment mutable counters in handlers.
- Set `completed_at` idempotently when every job is terminal.
- Duplicate delivery, retry publication, and DLQ publication must not double-count or reopen terminal work.

## Tests and verification

- Deterministically test backoff, jitter bounds, all rate-limit sources, and the rule that retry time never precedes the upstream requirement.
- Prove no long sleep occurs in the partition handler and delayed events are not published early.
- Test transient failure followed by success, cooldown deferral without attempt consumption, three-attempt exhaustion, immediate permanent failure, and DLQ payload safety.
- Test batch completion for mixed success/failure, duplicate terminal delivery, and concurrent final jobs.
- Test a crash after retry/failure commit but before offset commit.

```bash
make fmt
make test-unit
make test-integration
make test-race
```

## Done when

- All retries are durable PostgreSQL/outbox state, not sleeping goroutines or Kafka redelivery loops.
- No job makes more than three counted GitHub attempts.
- Batch status remains correct under duplicate delivery and concurrent terminal transitions.
