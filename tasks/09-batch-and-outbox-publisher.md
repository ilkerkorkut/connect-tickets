# Task 09: Batch Creation and Transactional Outbox Publisher

## Objective

Guarantee durable refresh-all enqueueing and PostgreSQL-to-Kafka publication without a dual-write gap, while accepting harmless duplicate publication.

## Depends on

- Task 03
- Task 08

## Source

- `INITIAL_PLAN.md`: sections 8.8, 14, Phase 7, and Prompt 9

## Batch transaction

- Snapshot all repositories visible in the refresh-all transaction.
- Insert the batch, one unique batch job per repository, and one refresh outbox event per job in the same transaction.
- Use `repo_id` as each event's partition key and stable unique event IDs.
- For an empty watchlist, create a completed zero-job batch.
- Return success as soon as PostgreSQL commits; Kafka availability must not change an already committed HTTP result.

## Publisher algorithm

1. In a short transaction, select due pending events and expired publishing leases with `FOR UPDATE SKIP LOCKED`.
2. Mark them `publishing`, set a random publisher owner and lease, increment/record the publish attempt as designed, and commit.
3. Publish outside the transaction and wait for Kafka acknowledgement.
4. Mark only acknowledged, ownership-matching events `published`.
5. Return failures to retryable state with safe error data or let the lease expire for recovery.

## Required behavior

- Honor `available_at` for initial work and delayed retries.
- Allow multiple publisher replicas without double-claiming active leases.
- Recover expired leases after process death.
- Treat an ambiguous acknowledgement as potentially published and publish again after recovery.
- Bound polling, batch size, attempts, lease, and shutdown by validated configuration.
- Stop claiming on shutdown, finish or safely abandon already-claimed work, and close Kafka after publication stops.

## Tests and verification

- Real PostgreSQL tests for atomic batch/job/event creation, empty batch, snapshot membership, and concurrent publishers using `SKIP LOCKED`.
- Kafka outage test: batch remains committed and outbox stays retryable.
- Lease-expiry and ownership tests prevent stale publishers from finalizing another publisher's claim.
- Crash-window test publishes successfully, skips the published-state update, recovers the lease, and republishes.
- Test graceful publisher cancellation without losing durable events.

```bash
make fmt
make test-unit
make test-integration
make test-race
```

## Done when

- No committed batch can lose its enqueue intent.
- Kafka is never called inside a database transaction.
- Multiple publishers are safe and duplicate publication is an explicit, tested outcome.
