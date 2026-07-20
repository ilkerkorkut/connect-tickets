# Task 03: Ent Model and Versioned Migrations

## Objective

Implement the PostgreSQL data model whose constraints and transactions enforce repository, batch, outbox, retry, and change-feed correctness.

## Depends on

- Task 02

## Source

- `INITIAL_PLAN.md`: sections 3.1-3.3, 7.1-7.7, Phase 2, and Prompt 3

## Entities and invariants

- `Repository`: `id`, `owner`, `name`, `full_name`, unique lowercase `normalized_full_name`, `description`, non-negative `stars`, `language`, `html_url`, bounded `notes`, `fetched_at`, `created_at`, and `updated_at`.
- `Batch`: `id`, `created_at`, and `completed_at`; status is derived from jobs, not mutable counters.
- `BatchJob`: identifiers, status, attempt count, processing owner/lease, next attempt, safe last error, and lifecycle timestamps. Enforce `UNIQUE(batch_id, repo_id)` and states `pending`, `queued`, `processing`, `retry_wait`, `succeeded`, and `failed`.
- `JobAttempt`: batch job, attempt number, worker, timings, result, error code, and duration. Enforce `UNIQUE(batch_job_id, attempt_number)`.
- `OutboxEvent`: event identity/type, aggregate, partition key, payload, state, availability, publishing owner/lease, attempts, safe error, and timestamps. Enforce unique event ID and states `pending`, `publishing`, and `published`.
- `RepositoryChange`: monotonic sequence, unique event ID, repository identity, operation, snapshot/tombstone, and timestamp. Support `created`, `notes_updated`, `refreshed`, and `deleted`.
- `ChangeSequence`: singleton `repository_changes` row with `last_value`.

## Required indexes

- Batch jobs: `(batch_id, status)`, `(status, next_attempt_at)`, `(status, lease_until)`, and `(repo_id, status)`.
- Outbox: `(status, available_at)` and `(status, publishing_lease_until)`.
- Add foreign keys and query-supporting change-feed indexes without weakening deletion tombstones.

## Deliverables

- Handwritten Ent schemas and generated code.
- Reviewable, versioned PostgreSQL migrations; runtime auto-migration is not the production migration strategy.
- Migration runner for the `migrate` runtime mode.
- Seed or migration logic that safely initializes the change-sequence singleton.
- A short schema note explaining each non-obvious constraint and index.

## Tests and verification

- Run migrations up on real PostgreSQL and verify constraints, indexes, enums/checks, foreign keys, and singleton initialization.
- Prove duplicate normalized repositories and duplicate batch jobs fail at the database boundary.
- Prove negative stars, invalid states, and duplicate attempt/event identities are rejected.
- Verify a clean database can migrate from zero and the migration is repeatable in the intended workflow.

```bash
make ent-generate
make migrate-up
make generate-check
make test-integration
```

## Done when

- Generated code is reviewed and clean.
- PostgreSQL constraints are the final concurrency authority.
- All required claim/retry/publish queries are supported by indexes.
- Production startup never relies on automatic schema creation.

