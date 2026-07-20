# Task 10: Idempotent Refresh Worker

## Objective

Consume refresh records with a global maximum concurrency of five, update PostgreSQL idempotently, and settle Kafka offsets only after durable outcomes.

## Depends on

- Task 05
- Task 06
- Task 08
- Task 09

## Source

- `PRODUCT_REQUIREMENTS.md`: The Worker
- `INITIAL_PLAN.md`: section 15, Phase 8, and Prompt 10
- `AGENTS.md`: worker ownership and Kafka requirements

## Processing flow

1. Decode and validate schema/version and attach event, batch, job, repository, worker, and request correlation to context/logs.
2. Load the batch job and atomically claim it with a processing owner and lease.
3. Accept claims only for pending/queued jobs, due `retry_wait` jobs, or expired `processing` leases.
4. Ignore duplicate terminal deliveries and active leases safely; settle the record only after confirming durable database state.
5. Check shutdown and the Redis shared cooldown before consuming an attempt.
6. Load the repository and record an attempt immediately before entering the GitHub request path.
7. Invalidate/refresh through the Redis-coordinated GitHub gateway.
8. On success, transactionally update GitHub-owned fields and `fetched_at`, preserve notes, add a commit-ordered change, mark the job succeeded, and finish the attempt.
9. Only after that transaction commits, mark and commit the Kafka record.

## Concurrency and lease rules

- Main topic has five partitions and each consumer instance processes one active record per assigned partition, yielding a group-wide maximum of five.
- Database leases remain the final defense against duplicate delivery and stale workers.
- Lease completion/update must verify ownership.
- Rebalance or cancellation before a durable outcome leaves the record replayable.
- A crash after the success transaction but before offset commit must cause harmless redelivery.

## Attempt definition

- Consume an attempt only after the cooldown gate and immediately before entering the GitHub call path.
- Cooldown deferral, duplicate terminal delivery, another owner's active lease, pre-claim rebalance, pre-call Kafka/PostgreSQL/Redis failure, and shutdown cancellation do not consume attempts.
- Maximum GitHub processing attempts is three.
- Persisting attempt start before a crash may conservatively consume an attempt; document this.

## Tests and verification

- Duplicate-delivery test proves no data corruption, extra change, or double-counted batch success.
- Crash-after-database-commit/before-offset-commit test proves replay is harmless.
- Deterministic concurrency test proves no more than five GitHub calls are active across the group and no partition processes two records at once.
- Test lease expiry/reclaim, stale-owner rejection, terminal duplicate, rebalance, invalid payload, shutdown cancellation, and notes preservation.
- Run all concurrency-sensitive tests under the race detector.

```bash
make fmt
make test-unit
make test-integration
make test-race
```

## Done when

- Every committed Kafka offset corresponds to a durable PostgreSQL terminal, retry, defer, or invalid-message outcome.
- Duplicate records and crash replay are harmless.
- Worker concurrency is globally bounded by the five-partition topology.
