# Task 06: Persistence and Application Services

## Objective

Implement narrow Ent stores and application services for repository use cases, transactional change capture, SQL statistics, batch reads, and durable batch creation.

## Depends on

- Task 03
- Task 05

## Source

- `INITIAL_PLAN.md`: sections 3, 7.7, 8.2-8.11, 14.2, 18, Phase 5, Prompt 6
- `AGENTS.md`: handler/store/service boundaries and database correctness rules

## Repository use cases

- Create: validate/normalize input, call the GitHub gateway outside a transaction, then insert the repository and `created` change atomically. Map the PostgreSQL unique violation to a typed duplicate error.
- Get and list: query PostgreSQL with language filtering, deterministic keyset ordering, configured page limits, and opaque versioned cursors bound to sort/filter inputs.
- Update notes: modify only bounded notes; preserve GitHub-owned fields; create `notes_updated` in the same transaction; do not emit a change for a semantic no-op.
- Delete: persist a `deleted` tombstone in the same transaction as removal.
- Synchronous refresh: load locally, refresh through the Redis-coordinated gateway outside the transaction, then update GitHub-owned fields, preserve notes, update `fetched_at`, and write `refreshed` atomically.

## Commit-ordered changes

- For every repository mutation, lock the `repository_changes` singleton row, increment it, perform the mutation, insert the change event, and commit while holding the row lock.
- Encode an opaque, versioned change cursor based on the committed sequence.
- Query `sequence > cursor.sequence` in ascending sequence order with a maximum limit.
- Include stable `event_id` values for consumer deduplication and sufficient deletion tombstones.

## Queries

- Compute `total`, `total_stars`, and `top_language` entirely in SQL.
- Exclude null/empty languages; choose greatest repository count and break ties by language ascending; return null when absent.
- Derive batch totals and completion from `BatchJob` rows. Pending includes `pending`, `queued`, `processing`, and `retry_wait`; return safe details for failed rows.
- Create refresh-all batches as a transaction-time repository snapshot with one job and one outbox event per repository. An empty watchlist creates a completed zero-job batch.

## Constraints

- Keep transactions short and make no GitHub, Redis, or Kafka calls inside them.
- Translate Ent/PostgreSQL failures into typed application errors without exposing raw errors.
- PostgreSQL is the only source of truth for repository, change, and batch state.

## Tests and verification

- Real PostgreSQL integration tests for concurrent duplicate create, mutation/change atomicity, tombstones, cursor order under interleaved transactions, no-op notes, and notes preservation on refresh.
- Query tests for both list sorts, filter-bound cursors, invalid cursor versions, maximum limits, mutable keyset behavior, and empty pages.
- SQL stats tests including empty data and language ties.
- Batch snapshot tests including zero repositories and repositories created after the transaction.

```bash
make fmt
make test-unit
make test-integration
make test-race
```

## Done when

- All application use cases expose narrow typed contracts to handlers/workers.
- Concurrent duplicate creation produces one row and one domain success.
- Change consumers cannot miss committed changes when advancing cursors correctly and can deduplicate repeats by event ID.
