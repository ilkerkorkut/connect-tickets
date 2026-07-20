# GitHub Repository Tracker
## Development and Architecture Specification

## 1. Purpose

Build a production-shaped backend service that tracks GitHub repositories and remains correct under concurrent requests, duplicate queue delivery, upstream failures, rate limiting, process restarts, and graceful shutdown.

This is a greenfield project. The implementation must begin from an empty repository and follow this document as the primary engineering specification.

The service must provide:

- A REST API for tracking GitHub repositories.
- PostgreSQL persistence through Ent.
- Redis caching and distributed coordination.
- Kafka-based asynchronous refresh processing.
- At-least-once job delivery with idempotent business effects.
- Pollable refresh-batch status.
- A no-miss, duplicate-tolerant repository change feed.
- Structured logging, request and job correlation, explicit timeouts, and graceful shutdown.
- A complete Docker Compose development environment.
- A Makefile as the standard development command surface.
- Meaningful unit, integration, concurrency, failure, and shutdown tests.
- A README documenting run instructions, architecture, trade-offs, guarantees, and AI-assisted development workflow.

---

## 2. Locked Technology Stack

The following stack is mandatory:

- Go 1.25 or newer
- Gin
- Ent
- PostgreSQL
- Redis
- Apache Kafka
- IBM Sarama: `github.com/IBM/sarama`
- Docker
- Docker Compose
- GNU Make-compatible Makefile
- Go `log/slog`
- Versioned database migrations generated from Ent/Atlas or committed SQL migrations

Do not replace any locked technology with an alternative.

The exact versions of all dependencies and container images must be pinned. Do not use floating `latest` tags.

---

## 3. Core Engineering Principles

### 3.1 PostgreSQL is the source of truth

PostgreSQL is authoritative for:

- Tracked repositories
- User notes
- Refresh batches
- Refresh jobs
- Job attempts
- Job terminal status
- Retry scheduling
- Transactional outbox state
- Repository change events
- Deletion tombstones

Redis and Kafka are coordination and delivery infrastructure. Neither is the authoritative business-state database.

### 3.2 Kafka delivery is at least once

The application must expect:

- Duplicate Kafka messages
- Redelivery after consumer crashes
- Duplicate publication from the transactional outbox
- Consumer-group rebalances
- Temporary broker unavailability
- Ambiguous producer acknowledgements

Correctness must come from idempotent processing and database constraints, not from assuming that Kafka delivers a message once.

### 3.3 No network calls inside database transactions

Do not call:

- GitHub
- Redis
- Kafka
- Any external service

while holding a database transaction open.

Database transactions must be short and limited to durable state changes.

### 3.4 Context propagation is mandatory

Every request and worker path must propagate `context.Context` through:

- HTTP handlers
- Application services
- Ent/PostgreSQL calls
- Redis calls
- GitHub HTTP requests
- Kafka publication and consumption paths
- Graceful shutdown

Do not replace a live request or worker context with `context.Background()`.

### 3.5 Cross-replica guarantees require distributed coordination

Do not use process-local mutexes or in-memory locks for guarantees that must hold across multiple API or worker replicas.

Redis must provide:

- Distributed GitHub cache-miss locking
- Shared GitHub rate-limit cooldown

PostgreSQL must provide:

- Duplicate repository protection
- Job idempotency
- Processing leases
- Outbox publication leases
- Change-sequence ordering

### 3.6 Failure classes must remain distinguishable

Do not collapse all failures into HTTP 500 or generic worker errors.

Differentiate:

- Validation errors
- Local resource not found
- GitHub repository not found
- GitHub unauthorized
- GitHub rate limited
- GitHub server failure
- Network failure
- Timeout
- Redis coordination failure
- Kafka failure
- PostgreSQL failure
- Invalid Kafka payload
- Retry exhaustion
- Shutdown cancellation

---

## 4. Required Runtime Modes

Build one application binary with explicit subcommands or modes:

```text
github-tracker api
github-tracker worker
github-tracker publisher
github-tracker all
github-tracker migrate
github-tracker healthcheck
```

### `api`

Runs only the Gin HTTP API.

### `worker`

Runs only the Kafka refresh consumer.

### `publisher`

Runs only the PostgreSQL transactional-outbox publisher.

### `all`

Runs API, worker, and publisher in one process for local host development only.

The Docker Compose topology must run API, worker, and publisher as separate containers.

### `migrate`

Applies committed database migrations and exits.

### `healthcheck`

Checks the requested service readiness or liveness condition and exits with a meaningful status code.

---

## 5. High-Level Architecture

```text
Client
  |
  v
Gin API
  |
  +--> Request ID middleware
  +--> Access logging
  +--> Error renderer
  |
  v
Application services
  |
  +--> Repository store --> Ent --> PostgreSQL
  |
  +--> GitHub gateway
  |      |
  |      +--> Redis response cache
  |      +--> Redis distributed lock
  |      +--> Redis shared rate-limit cooldown
  |      +--> GitHub HTTP API
  |
  +--> Batch service
  |      |
  |      +--> Batch jobs --> PostgreSQL
  |      +--> Outbox events --> PostgreSQL
  |
  +--> Change feed --> PostgreSQL

Outbox publisher
  |
  +--> Claims due outbox rows from PostgreSQL
  +--> Publishes to Kafka
  +--> Marks acknowledged events as published

Kafka
  |
  +--> Main refresh topic
  +--> Dead-letter topic

Worker consumer group
  |
  +--> Claims batch job in PostgreSQL
  +--> Calls GitHub through Redis-coordinated gateway
  +--> Updates repository and batch job transactionally
  +--> Schedules retries through PostgreSQL outbox
  +--> Commits Kafka offset after durable outcome
```

---

## 6. Project Structure

Use a package layout that keeps transport, infrastructure, and domain behavior separate.

```text
cmd/
  github-tracker/
    main.go

internal/
  app/
    api.go
    worker.go
    publisher.go
    all.go
    lifecycle.go

  config/
    config.go
    validation.go

  platform/
    database/
      client.go
      transaction.go
    redis/
      client.go
    kafka/
      config.go
      producer.go
      consumer.go
      admin.go
      messages.go
      topics.go
    logging/
      logger.go
      context.go
    signal/
      signal.go

  github/
    client.go
    dto.go
    errors.go
    cache.go
    lock.go
    ratelimit.go

  repository/
    model.go
    store.go
    entstore/
      store.go

  batch/
    model.go
    store.go
    service.go

  outbox/
    model.go
    store.go
    publisher.go

  worker/
    consumer.go
    refresh_handler.go
    lease.go
    retry.go
    dlq.go

  changefeed/
    cursor.go
    store.go
    service.go

  httpapi/
    router.go
    errors.go
    middleware/
      request_id.go
      access_log.go
      recovery.go
      body_limit.go
    handlers/
      repos.go
      batches.go
      stats.go
      changes.go

ent/
  schema/

migrations/

deploy/
  docker/
    kafka/
      create-topics.sh

scripts/
  smoke-test.sh
  smoke-concurrency.sh
  smoke-batch.sh
  smoke-shutdown.sh
  verify-topics.sh

Dockerfile
compose.yaml
compose.test.yaml
Makefile
.env.example
README.md
```

Avoid:

- Generic base repositories
- Generic service abstractions
- Direct Ent usage from HTTP handlers
- Direct Kafka usage from HTTP handlers
- Direct Redis usage from HTTP handlers
- Domain logic inside Gin handlers

---

## 7. Domain Model and Ent Schemas

## 7.1 Repository

Required fields:

```text
id
owner
name
full_name
normalized_full_name
description
stars
language
html_url
notes
fetched_at
created_at
updated_at
```

Requirements:

- `normalized_full_name` must be lowercase and unique.
- `full_name` preserves the canonical owner/name returned by GitHub.
- `stars` must not be negative.
- `notes` must have an explicit maximum length.
- GitHub refreshes must not overwrite `notes`.
- User operations must not modify GitHub-owned fields.

Required unique constraint:

```text
UNIQUE(normalized_full_name)
```

This database constraint is the final authority for concurrent duplicate creation.

## 7.2 Batch

Required fields:

```text
id
created_at
completed_at
```

Batch status should preferably be derived from batch-job rows instead of maintained through mutable counters.

## 7.3 BatchJob

Required fields:

```text
id
batch_id
repo_id
status
attempt_count
processing_owner
lease_until
next_attempt_at
last_error_code
last_error_message
created_at
started_at
completed_at
updated_at
```

Required constraint:

```text
UNIQUE(batch_id, repo_id)
```

Required states:

```text
pending
queued
processing
retry_wait
succeeded
failed
```

Terminal states:

```text
succeeded
failed
```

Required indexes:

```text
(batch_id, status)
(status, next_attempt_at)
(status, lease_until)
(repo_id, status)
```

## 7.4 JobAttempt

Use a separate attempt table for observability and auditability.

Required fields:

```text
id
batch_job_id
attempt_number
worker_id
started_at
finished_at
result
error_code
duration_ms
```

Required constraint:

```text
UNIQUE(batch_job_id, attempt_number)
```

## 7.5 OutboxEvent

Required fields:

```text
id
event_id
event_type
aggregate_id
partition_key
payload
status
available_at
publishing_owner
publishing_lease_until
publish_attempts
last_error
published_at
created_at
updated_at
```

Required constraint:

```text
UNIQUE(event_id)
```

Required states:

```text
pending
publishing
published
```

Required indexes:

```text
(status, available_at)
(status, publishing_lease_until)
```

## 7.6 RepositoryChange

Required fields:

```text
sequence
event_id
repo_id
operation
repository_snapshot
changed_at
```

Required operations:

```text
created
notes_updated
refreshed
deleted
```

Deletion events must include a tombstone containing enough data for polling consumers to identify the deleted repository.

## 7.7 ChangeSequence

Use a singleton row:

```text
name = repository_changes
last_value
```

Every repository mutation transaction must:

1. Lock this row with `FOR UPDATE`.
2. Increment the sequence.
3. Apply the repository mutation.
4. Insert the change event with the allocated sequence.
5. Commit while still holding the sequence-row lock.

This guarantees commit-ordered change cursors and prevents missed changes caused by transactions committing in a different order than they started.

The trade-off is brief serialization of repository mutations. Document it.

---

## 8. HTTP API Contract

Base path:

```text
/api
```

## 8.1 Error Response

Use one JSON shape everywhere:

```json
{
  "error": {
    "code": "repo_already_tracked",
    "message": "The repository is already being tracked.",
    "request_id": "..."
  }
}
```

Validation details may be included safely:

```json
{
  "error": {
    "code": "validation_failed",
    "message": "The request is invalid.",
    "request_id": "...",
    "details": [
      {
        "field": "owner",
        "reason": "required"
      }
    ]
  }
}
```

Do not return:

- Raw SQL errors
- Redis errors
- Kafka errors
- Stack traces
- Authentication tokens
- Full GitHub response bodies

## 8.2 POST `/api/repos`

Body:

```json
{
  "owner": "owner",
  "name": "repository"
}
```

Required flow:

1. Strictly decode and validate JSON.
2. Reject unknown fields.
3. Normalize owner and repository name.
4. Load repository data through the cached GitHub gateway.
5. Do not hold a database transaction during the GitHub call.
6. Insert the repository and its creation change event transactionally.
7. Let the PostgreSQL unique constraint arbitrate concurrent requests.
8. Map unique-constraint violation to HTTP 409.
9. Return the created stored record with HTTP 201.

Concurrent guarantee:

For two or more simultaneous requests for the same normalized repository:

- Exactly one insert succeeds.
- All losing requests receive a clean 409.
- None receive a database-derived 500.
- Exactly one database row exists.

## 8.3 GET `/api/repos`

Supported query parameters:

```text
language
sort=stars_desc|updated_desc
limit
cursor
```

Use opaque cursor pagination.

Sort keys:

```text
stars_desc:
  stars DESC, id DESC

updated_desc:
  updated_at DESC, id DESC
```

Cursor contents:

```text
version
sort mode
language filter
last sort value
last repository id
```

Requirements:

- Cursor must be opaque to clients.
- Reject unsupported versions.
- Reject cursors that do not match the current sort or filter.
- Enforce a configured maximum page size.
- Use deterministic ordering.

Guarantee:

This is keyset pagination over a mutable dataset, not snapshot pagination. A repository refresh can move a row between pages. Document that limitation.

## 8.4 GET `/api/repos/:id`

Return the locally stored tracked repository.

Do not call GitHub.

## 8.5 PATCH `/api/repos/:id`

Only `notes` is user-editable.

Requirements:

- Reject attempts to update any other field.
- Enforce notes length limit.
- Preserve GitHub-owned fields.
- Update notes and create a change event in one transaction.
- A semantic no-op notes update should not emit a change event.

## 8.6 DELETE `/api/repos/:id`

Required transaction:

1. Read repository data.
2. Delete the tracked repository.
3. Insert a deletion tombstone change event.
4. Commit.

Do not call GitHub.

## 8.7 POST `/api/repos/:id/refresh`

Required flow:

1. Read local repository.
2. Invalidate its Redis cache entry.
3. Use the same distributed single-fetch mechanism as a cache miss.
4. Fetch GitHub data.
5. Update only GitHub-owned fields.
6. Preserve notes.
7. Update `fetched_at`.
8. Write repository update and change event in one transaction.
9. Return updated record.

Concurrent explicit refreshes for the same repository must coalesce into one GitHub call while Redis coordination is available.

## 8.8 POST `/api/repos/refresh-all`

Required behavior:

- Do not perform refresh work in the HTTP handler.
- Create a batch snapshot from repositories that exist when the request transaction runs.
- Create one batch job and one outbox event per repository.
- Commit all rows in one PostgreSQL transaction.
- Return HTTP 202 immediately after the transaction commits.

Response:

```json
{
  "batch_id": "..."
}
```

Empty watchlist behavior:

- Create a completed batch with zero jobs.
- Return HTTP 202.

Repositories created after the batch transaction are not part of that batch.

## 8.9 GET `/api/batches/:id`

Return:

```json
{
  "id": "...",
  "total": 10,
  "pending": 2,
  "succeeded": 7,
  "failed": [
    {
      "repo_id": 42,
      "reason": {
        "code": "github_repo_not_found",
        "message": "Repository was not found on GitHub."
      }
    }
  ],
  "complete": false
}
```

Compute status from batch-job rows:

```text
total:
  all batch jobs

pending:
  pending, queued, processing, retry_wait

succeeded:
  succeeded

failed:
  failed rows
```

Do not trust Kafka offsets or Kafka topic state for batch status.

## 8.10 GET `/api/repos/stats`

Return:

```json
{
  "total": 0,
  "total_stars": 0,
  "top_language": null
}
```

Requirements:

- Compute entirely in SQL.
- Do not load repository rows into Go for aggregation.
- Exclude null and empty languages from `top_language`.
- Select language with greatest repository count.
- Resolve ties by language name ascending.
- Return `null` when no language exists.

## 8.11 GET `/api/repos/changes`

Supported parameters:

```text
since
limit
```

Query:

```text
WHERE sequence > cursor.sequence
ORDER BY sequence ASC
LIMIT requested_limit
```

Response:

```json
{
  "changes": [
    {
      "cursor": "...",
      "event_id": "...",
      "operation": "refreshed",
      "repo": {}
    }
  ],
  "next_cursor": "...",
  "has_more": true
}
```

Guarantee:

- Every committed repository mutation receives a commit-ordered sequence.
- A consumer that persists `next_cursor` after successfully processing a page will not miss a committed event.
- Reusing an older cursor may return events again.
- Consumers must deduplicate using `event_id`.
- Delivery semantics are at least once.
- Deletions are represented through tombstones.

---

## 9. HTTP Status and Error Mapping

| Condition | Status | Code |
|---|---:|---|
| Malformed JSON | 400 | `invalid_json` |
| Validation failure | 400 | `validation_failed` |
| Invalid cursor | 400 | `invalid_cursor` |
| Local tracked repository absent | 404 | `tracked_repo_not_found` |
| GitHub repository absent during create | 404 | `github_repo_not_found` |
| Concurrent duplicate repository | 409 | `repo_already_tracked` |
| GitHub unauthorized | 502 | `github_unauthorized` |
| GitHub rate limited | 503 | `github_rate_limited` |
| GitHub timeout | 504 | `github_timeout` |
| GitHub network error | 502 | `github_network_error` |
| GitHub 5xx | 502 | `github_unavailable` |
| Redis coordination unavailable | 503 | `cache_coordination_unavailable` |
| Kafka unavailable for asynchronous publication | Do not fail an already committed batch; outbox remains pending | Internal operational state |
| PostgreSQL internal failure | 500 | `internal_error` |

When GitHub provides a safe retry time, return an appropriate `Retry-After` header.

---

## 10. GitHub Client

GitHub endpoint:

```text
https://api.github.com/repos/{owner}/{name}
```

Support optional `GITHUB_TOKEN`.

## 10.1 HTTP client requirements

Use one shared `http.Client`.

Configure explicitly:

- Total request timeout
- Dial timeout
- TLS handshake timeout
- Response-header timeout
- Idle connection timeout
- Connection-pool limits
- Maximum response body size
- User-Agent
- Optional authorization header
- GitHub API version header when selected

Every GitHub method must accept `context.Context`.

Do not create a new HTTP client for each request.

## 10.2 Typed error classes

Implement typed errors for:

```text
not found
rate limited
unauthorized
server error
timeout
network error
invalid response
caller cancellation
```

Rate-limit error data should include:

```text
retry_after
rate_limit_remaining
rate_limit_reset
status_code
```

Support `Retry-After` as:

- Delta seconds
- HTTP date

Distinguish:

- GitHub 404
- GitHub 401
- GitHub 403 caused by rate limiting
- GitHub 429
- GitHub 5xx
- DNS/connectivity failures
- HTTP timeout
- Caller or shutdown cancellation
- Invalid successful JSON response

Do not implement an unbounded or hidden retry loop inside the low-level GitHub client. Worker retry policy belongs to the worker layer.

---

## 11. Redis Cache and Distributed Lock

Redis responsibilities:

1. Five-minute GitHub response cache
2. Cross-replica cache-miss lock
3. Shared GitHub rate-limit cooldown

Redis is not the durable batch or retry store.

## 11.1 Cache keys

```text
github:repo:v1:{normalized_full_name}
github:repo-lock:v1:{normalized_full_name}
github:rate-limit:cooldown-until:v1
```

## 11.2 Cache fetch algorithm

1. Read the cache.
2. Return on a valid hit.
3. On miss, try to acquire the lock with:

```text
SET lock_key ownership_token NX PX lock_ttl
```

4. If acquired:
   - Recheck the cache.
   - Call GitHub with a deadline shorter than the lock TTL.
   - Cache successful response for five minutes.
   - Release using compare-and-delete.
5. If not acquired:
   - Wait with bounded jittered polling.
   - Recheck the cache.
   - Honor context cancellation.
   - If lock expires without a cache value, compete for the lock again.

## 11.3 Ownership safety

- Lock value must be a cryptographically random ownership token.
- Lock release must use a Lua compare-and-delete script.
- A stale holder must not delete a newer holder’s lock.
- Lock TTL must exceed the GitHub call timeout plus safety margin.
- Optional lock renewal must verify ownership before extending.

## 11.4 Lock-holder failure

If the lock holder dies:

- Lock expiry allows another caller to acquire ownership.
- Waiters continue only within their own context deadlines.
- No permanent lock remains.

## 11.5 Exactly-one-call guarantee scope

Document the actual guarantee:

While Redis is available and the lock TTL is longer than the GitHub request deadline, concurrent callers across application replicas for the same uncached normalized repository result in one active GitHub call.

Do not claim unconditional distributed exactly-once behavior under arbitrary process pauses, Redis partitions, or lock-expiry races.

## 11.6 Explicit refresh

Explicit refresh must:

1. Delete the cache key.
2. Enter the same distributed lock flow.
3. Recheck the cache after acquiring the lock.
4. Avoid a duplicate upstream request when another explicit refresh has already populated the cache.

## 11.7 Redis failure behavior

Do not silently bypass distributed locking and call GitHub independently from every replica.

When Redis coordination is unavailable:

- Synchronous operations that require the one-call guarantee return a typed temporary error.
- Worker jobs schedule a retry.
- The service does not hammer GitHub.

---

## 12. Shared GitHub Rate-Limit Control

Inspect:

```text
Retry-After
X-RateLimit-Remaining
X-RateLimit-Reset
HTTP status
bounded GitHub error body
```

When a worker detects a rate limit:

1. Calculate the earliest allowed retry time.
2. Store the timestamp in Redis.
3. Atomically keep the later of the existing and new timestamp.
4. Persist retry state in PostgreSQL.
5. Create a future outbox event.
6. Commit the current Kafka message only after retry state is durable.

Before any worker starts a GitHub call:

1. Read the shared cooldown.
2. If active, schedule the batch job for that time.
3. Do not increment the attempt count.
4. Do not call GitHub.
5. Commit the current Kafka message after scheduling is durable.

Add jitter when many workers resume after the same cooldown.

---

## 13. Kafka Architecture with IBM Sarama

## 13.1 Kafka client

Use:

```text
github.com/IBM/sarama
```

Pin the exact version in `go.mod`.

All Sarama configuration must be based on the selected pinned version. Do not generate code against remembered APIs without validating the version.

## 13.2 Required topics

### Main refresh topic

```text
github.repo.refresh.v1
```

Required partitions:

```text
5
```

Purpose:

- Carries refresh jobs ready for processing.

### Dead-letter topic

```text
github.repo.refresh.dlq.v1
```

Purpose:

- Carries terminally failed or retry-exhausted job events.

PostgreSQL remains authoritative for failure state.

## 13.3 Global concurrency guarantee

The main topic must have exactly five partitions.

Workers must process each assigned partition sequentially and may process different partitions concurrently.

With one consumer group across all worker replicas:

```text
maximum global active refresh jobs <= 5
```

Requirements:

- Validate topic partition count during startup/readiness.
- Fail readiness if the main topic does not have exactly five partitions.
- Do not create additional in-handler goroutines that increase concurrency beyond one active job per partition.
- Scaling worker replicas redistributes partitions but does not increase concurrency beyond five.

## 13.4 Consumer group

Consumer group:

```text
github-repo-refresh-v1
```

Requirements:

- Disable automatic offset commit.
- Use manual offset control.
- Process records in partition order.
- Mark and commit offsets only after a durable PostgreSQL outcome.
- Handle group setup and cleanup.
- Handle partition assignment and revocation.
- Log group, topic, partition, and offset.
- Close the consumer group cleanly during shutdown.

The implementation must use the manual offset APIs supported by the pinned IBM Sarama version.

## 13.5 Message key

Use:

```text
repo_id
```

as the Kafka message key.

Benefits:

- Jobs for the same repository map to the same partition.
- Jobs for the same repository are processed in partition order.
- Two batches normally do not refresh the same repository concurrently through the main consumer group.

Database idempotency remains mandatory.

## 13.6 Message schema

Use versioned JSON:

```json
{
  "schema_version": 1,
  "event_id": "...",
  "batch_job_id": "...",
  "batch_id": "...",
  "repo_id": 42,
  "created_at": "2026-07-20T12:00:00Z",
  "correlation_id": "..."
}
```

Rules:

- `event_id` uniquely identifies the queue event.
- `batch_job_id` is the idempotency boundary.
- Mutable repository data is not embedded.
- Worker loads current state from PostgreSQL.
- Unknown schema version is a permanent failure.
- Missing required fields are permanent failures.
- Enforce maximum message size.
- Include safe correlation headers.

## 13.7 Producer requirements

Configure the Sarama producer for:

- Broker acknowledgements
- Safe retry limits
- Idempotent production when supported by the pinned version and configuration
- Bounded message size
- Explicit request timeouts
- Structured producer errors
- Graceful close and flush
- Context-aware application wrapper

Kafka producer idempotence does not remove the PostgreSQL-to-Kafka crash window. Duplicate publication remains possible and must be safe.

---

## 14. Transactional Outbox

## 14.1 Why it is required

Do not directly combine:

1. PostgreSQL transaction commit
2. Kafka publish

as though they are one atomic operation.

Kafka transactions do not make a PostgreSQL transaction atomic with Kafka.

## 14.2 Batch creation

`POST /api/repos/refresh-all` must create in one PostgreSQL transaction:

- Batch
- Batch jobs
- Main-topic outbox events

After commit, the request returns HTTP 202 even when Kafka is temporarily unavailable.

## 14.3 Outbox claim flow

The publisher must:

1. Start a short transaction.
2. Select due rows using `FOR UPDATE SKIP LOCKED`.
3. Mark rows `publishing`.
4. Assign a publisher ownership token.
5. Set a publishing lease.
6. Commit.
7. Publish to Kafka outside the transaction.
8. Wait for producer acknowledgement.
9. Mark acknowledged rows `published`.
10. Return failed rows to a retryable state or allow lease recovery.

## 14.4 Crash behavior

### Crash before publish

- Row remains pending or publishing lease expires.
- Another publisher retries.

### Crash after publish but before marking published

- Kafka event may be published twice.
- Worker idempotency prevents duplicate business effects.

### Ambiguous producer acknowledgement

- Treat event as potentially published.
- Retry publication.
- Accept possible duplicate delivery.

## 14.5 Multiple publishers

Multiple publisher replicas must be safe through:

- `FOR UPDATE SKIP LOCKED`
- Ownership tokens
- Publishing leases
- Unique outbox `event_id`

Do not keep a database transaction open during Kafka publication.

---

## 15. Kafka Worker Processing

## 15.1 Offset commit rule

Commit a Kafka record only after one of these PostgreSQL outcomes is durable:

```text
batch job succeeded
batch job permanently failed
batch job durably scheduled for retry
batch job was already terminal
invalid message was durably recorded for DLQ processing
```

Never commit before durable state.

## 15.2 Job claim

Use a conditional database processing lease.

Claimable states:

```text
pending
queued
retry_wait with next_attempt_at <= now
processing with expired lease
```

Claim operation must atomically set:

```text
status = processing
processing_owner
lease_until
started_at where appropriate
```

Only the successful claimant performs GitHub work.

## 15.3 Duplicate behavior

When a duplicate Kafka message arrives:

- `succeeded`: no-op and commit.
- `failed`: no-op and commit.
- `retry_wait` with future retry: no-op and commit; durable outbox retry remains authoritative.
- `processing` with active foreign lease: do not call GitHub.
- `processing` with expired lease: reclaim.
- Missing batch job: classify and record according to invalid-message policy.
- Deleted repository: permanently fail the batch job.

## 15.4 Processing flow

1. Decode and validate message.
2. Attach job identifiers to context and logs.
3. Load batch job.
4. Claim processing lease.
5. Check shutdown context.
6. Check Redis shared rate-limit cooldown.
7. Load repository.
8. Record processing attempt immediately before entering the GitHub call path.
9. Invalidate and refresh through the Redis-coordinated GitHub gateway.
10. On success, commit in one database transaction:
    - Repository GitHub fields
    - `fetched_at`
    - Repository change event
    - Batch job `succeeded`
    - Job attempt result
11. Mark and commit Kafka offset.
12. On retryable failure, schedule retry durably, then commit offset.
13. On permanent failure, mark failed and create DLQ outbox event, then commit offset.

## 15.5 Attempt definition

One attempt is consumed when the worker has passed the shared cooldown gate and begins the GitHub request path.

The following do not consume attempts:

- Shared rate-limit deferral
- Duplicate terminal delivery
- Active processing lease owned by another worker
- Rebalance before job claim
- Kafka failure before GitHub processing
- PostgreSQL failure before attempt start
- Redis coordination failure before GitHub processing

Maximum:

```text
3 attempts
```

A worker crash after persisting attempt start may consume an attempt even if the HTTP request did not complete. This conservative behavior must be documented.

---

## 16. Retry Scheduling

Do not sleep for long backoff periods inside Kafka partition handlers.

Long sleeps would block the partition and complicate shutdown and rebalancing.

Use PostgreSQL and the outbox as the retry scheduler.

On retryable failure:

1. Calculate `next_attempt_at`.
2. Begin a database transaction.
3. Set batch job state:

```text
status = retry_wait
next_attempt_at
last_error_code
last_error_message
```

4. Finish the attempt record.
5. Create a new main-topic outbox event:

```text
available_at = next_attempt_at
```

6. Commit.
7. Commit the current Kafka record.

The outbox publisher publishes only when:

```text
available_at <= database current time
```

Backoff must combine:

```text
exponential backoff
random jitter
Retry-After
X-RateLimit-Reset
shared Redis cooldown
```

The effective retry time must never be earlier than GitHub’s required retry time.

Default backoff values must be configuration, not hardcoded policy hidden in handlers.

---

## 17. Dead-Letter Handling

On permanent failure or exhausted attempts:

1. Mark batch job `failed`.
2. Store a safe terminal error.
3. Finish attempt record.
4. Create a DLQ outbox event.
5. Commit the database transaction.
6. Commit the main-topic Kafka record.
7. Let the publisher publish the DLQ event.

DLQ event:

```json
{
  "schema_version": 1,
  "event_id": "...",
  "original_event_id": "...",
  "batch_job_id": "...",
  "batch_id": "...",
  "repo_id": 42,
  "attempts": 3,
  "error": {
    "code": "github_rate_limited",
    "message": "GitHub rate limiting remained active after maximum attempts."
  },
  "failed_at": "..."
}
```

Do not place secrets, unrestricted upstream bodies, or raw database errors in the DLQ message.

---

## 18. Batch Status Correctness

Do not increment mutable batch counters in Kafka handlers.

Duplicate delivery could double-count them.

Calculate batch state from batch-job rows:

```text
total = count(all)
pending = count(pending, queued, processing, retry_wait)
succeeded = count(succeeded)
failed = query failed rows
```

Set `completed_at` when all jobs become terminal. The update must be idempotent.

---

## 19. Graceful Shutdown

Use `signal.NotifyContext`.

## 19.1 Shutdown order

1. Mark readiness false.
2. Stop accepting new HTTP requests.
3. Stop claiming new outbox records.
4. Stop consuming new Kafka records.
5. Drain in-flight HTTP requests within deadline.
6. Allow current worker jobs to finish within deadline.
7. For unfinished jobs:
   - Do not mark success.
   - Do not commit Kafka offsets without a durable outcome.
   - Allow Kafka redelivery or lease recovery.
8. Finish or stop current publisher operations safely.
9. Close Gin HTTP server.
10. Close Sarama consumer group.
11. Close Sarama producer.
12. Close Redis.
13. Close Ent/PostgreSQL.
14. Flush logs where applicable.

## 19.2 Worker shutdown guarantee

A job active during shutdown must result in one of:

- It finishes, commits state, and commits offset.
- It durably schedules retry and commits offset.
- It remains uncommitted and Kafka redelivers it.
- Its processing lease expires and a later worker reclaims it.

It must never disappear.

## 19.3 Kafka rebalance behavior

During partition revocation:

- Stop starting new work for the revoked partition.
- Finish or cancel current work according to the shutdown deadline.
- Commit only durable outcomes.
- Do not commit later offsets before earlier unprocessed records in the same partition.

---

## 20. Structured Logging

Use `slog`.

## 20.1 HTTP log fields

```text
request_id
method
route
path
status
duration_ms
remote_ip
user_agent
error_code
```

Prefer route templates over raw URLs for aggregation.

## 20.2 Worker fields

```text
job_id
event_id
batch_id
batch_job_id
repo_id
attempt
worker_id
consumer_group
topic
partition
offset
duration_ms
result
error_code
retry_at
```

## 20.3 Outbox fields

```text
event_id
event_type
aggregate_id
publisher_id
publish_attempt
topic
partition_key
duration_ms
result
error_code
```

## 20.4 GitHub fields

```text
github_owner
github_repo
github_status
github_duration_ms
cache_result
lock_wait_ms
rate_limit_remaining
```

Do not log:

- GitHub token
- Authorization headers
- Database credentials
- Redis password
- Kafka credentials
- Full upstream bodies
- Unbounded notes content

---

## 21. Timeouts and Resource Limits

Configure separate timeouts for:

- HTTP read header
- HTTP read
- HTTP write
- HTTP idle
- Request body size
- Application request execution
- GitHub HTTP request
- Redis dial/read/write
- Database query
- Kafka dial
- Kafka request
- Kafka producer acknowledgement
- Worker processing lease
- Outbox publishing lease
- HTTP graceful shutdown
- Worker graceful shutdown
- Publisher graceful shutdown

Do not use one global timeout for all operations.

Configure:

- PostgreSQL connection pool
- Redis pool
- Kafka fetch sizes
- Kafka message size
- GitHub transport connection pool
- API maximum body size
- Pagination maximum page size
- Outbox batch size

All limits must be environment-configurable with validated defaults.

---

## 22. Environment Variables

## 22.1 Application

```text
APP_ENV
APP_MODE
APP_VERSION
HTTP_PORT
LOG_LEVEL
SHUTDOWN_TIMEOUT
```

## 22.2 PostgreSQL

```text
DATABASE_URL
DATABASE_MAX_OPEN_CONNS
DATABASE_MAX_IDLE_CONNS
DATABASE_CONN_MAX_LIFETIME
DATABASE_QUERY_TIMEOUT
```

## 22.3 Redis

```text
REDIS_ADDR
REDIS_PASSWORD
REDIS_DB
REDIS_DIAL_TIMEOUT
REDIS_READ_TIMEOUT
REDIS_WRITE_TIMEOUT
CACHE_TTL
CACHE_LOCK_TTL
```

## 22.4 Kafka

```text
KAFKA_BROKERS
KAFKA_CLIENT_ID
KAFKA_CONSUMER_GROUP
KAFKA_REFRESH_TOPIC
KAFKA_DLQ_TOPIC
KAFKA_EXPECTED_REFRESH_PARTITIONS
KAFKA_DIAL_TIMEOUT
KAFKA_REQUEST_TIMEOUT
KAFKA_PRODUCE_TIMEOUT
KAFKA_FETCH_MAX_BYTES
KAFKA_MESSAGE_MAX_BYTES
KAFKA_SASL_MECHANISM
KAFKA_SASL_USERNAME
KAFKA_SASL_PASSWORD
KAFKA_TLS_ENABLED
```

Required local values:

```text
KAFKA_CONSUMER_GROUP=github-repo-refresh-v1
KAFKA_REFRESH_TOPIC=github.repo.refresh.v1
KAFKA_DLQ_TOPIC=github.repo.refresh.dlq.v1
KAFKA_EXPECTED_REFRESH_PARTITIONS=5
```

## 22.5 GitHub

```text
GITHUB_TOKEN
GITHUB_TIMEOUT
GITHUB_USER_AGENT
```

A custom GitHub base URL may be injectable only for tests.

## 22.6 Worker and outbox

```text
WORKER_LEASE_DURATION
WORKER_MAX_ATTEMPTS
WORKER_SHUTDOWN_TIMEOUT
OUTBOX_POLL_INTERVAL
OUTBOX_BATCH_SIZE
OUTBOX_PUBLISH_LEASE
OUTBOX_MAX_PUBLISH_ATTEMPTS
```

Do not commit real secrets.

---

## 23. Dockerfile Requirements

Use a multi-stage Dockerfile.

## 23.1 Builder stage

- Pinned Go image
- Module and build caching
- Reproducible build flags where practical
- One compiled binary
- Version injected through linker flags
- Optional test stage

## 23.2 Runtime stage

- Pinned minimal base image
- CA certificates
- Non-root user
- Binary only
- Migration files only when required
- No source tree
- No Go toolchain
- No credentials
- Correct signal propagation

Use the same image for:

- API
- Worker
- Publisher
- Migration
- Healthcheck

Include `.dockerignore`.

Do not use floating tags.

---

## 24. Docker Compose Requirements

## 24.1 Services

Required services:

```text
postgres
redis
kafka
kafka-init
migrate
api
worker
publisher
```

Optional development-only profile:

```text
kafka-ui
```

The optional UI must not be required for correctness.

## 24.2 Kafka local environment

Use Kafka in KRaft mode.

Local Compose topology:

- One combined broker/controller
- One persistent Kafka volume
- Replication factor one because the local environment has one broker
- Internal container listener
- Separate host listener for running Go processes outside Compose

Expected addresses:

```text
containers: kafka:9092
host: localhost:29092
```

This is a local development topology only. Do not present it as production infrastructure.

## 24.3 Kafka initialization

`kafka-init` must:

1. Wait for broker readiness.
2. Create main refresh topic.
3. Create DLQ topic.
4. Create main topic with exactly five partitions.
5. Apply configured retention.
6. Verify topic configuration.
7. Exit successfully.

Topic creation must be idempotent.

The application must not silently create or modify production topics.

## 24.4 Migration container

`migrate` must:

1. Wait for PostgreSQL health.
2. Apply committed migrations.
3. Exit zero only on success.

API, worker, and publisher must depend on successful migration completion.

## 24.5 Health checks

### PostgreSQL

Use `pg_isready`.

### Redis

Use `redis-cli ping`.

### Kafka

Use a broker metadata or topic-list command that proves the broker responds.

### API

Expose:

```text
GET /health/live
GET /health/ready
```

`live` verifies the process is running.

`ready` verifies:

- PostgreSQL connectivity
- Redis connectivity
- Kafka metadata access
- Required topics exist
- Main topic has exactly five partitions
- Database migrations are current

### Worker and publisher

Use the application `healthcheck` subcommand or dedicated internal endpoints.

Readiness must fail when required dependencies or topic configuration are invalid.

## 24.6 Compose dependencies

Use health and one-shot completion conditions so that:

- Migration waits for PostgreSQL.
- Kafka topic initialization waits for Kafka.
- API, worker, and publisher wait for:
  - Healthy PostgreSQL
  - Healthy Redis
  - Healthy Kafka
  - Successful migrations
  - Successful topic initialization

Application code must still tolerate dependencies disappearing after startup.

## 24.7 Volumes

Use named development volumes:

```text
postgres-data
redis-data
kafka-data
```

Ordinary shutdown must not delete volumes.

Only an explicit destructive reset target may remove them.

## 24.8 Test Compose

Create `compose.test.yaml`.

Requirements:

- Separate Compose project name
- Ephemeral data
- Test database
- Test consumer group
- Test topics
- Test runner container
- No unnecessary host ports
- Automatic teardown
- Exit code follows test runner result

---

## 25. Makefile Requirements

The Makefile is the documented developer entry point.

Use:

```makefile
SHELL := /bin/bash

APP_NAME := github-tracker
BIN_DIR := $(CURDIR)/bin
APP_BIN := $(BIN_DIR)/$(APP_NAME)

COMPOSE := docker compose
COMPOSE_PROJECT := github-tracker
GO := go

.DEFAULT_GOAL := help
```

Requirements:

- Self-documenting `help`
- `.PHONY` for command targets
- Overridable variables
- Commands fail on errors
- No silent overwrite of `.env`
- Pinned tools installed to `./bin`
- Destructive targets clearly named
- Same targets used in local development and CI
- Unit, integration, and race tests separated
- Consistent Compose project name

## 25.1 Required targets

### Help and setup

```text
make help
make bootstrap
make tools
make deps
```

`bootstrap` must:

1. Validate required commands.
2. Create `.env` from `.env.example` only when `.env` does not exist.
3. Install pinned local tools.
4. Download Go modules.

### Generation

```text
make generate
make ent-generate
make generate-check
```

`generate-check` must fail when generated output changes tracked files.

### Formatting and validation

```text
make fmt
make fmt-check
make vet
make lint
make test
make test-unit
make test-race
make test-integration
make test-all
make ci
```

Recommended `make ci` order:

```text
generate-check
fmt-check
vet
lint
test-unit
test-race
test-integration
build
```

### Build and local execution

```text
make build
make run-api
make run-worker
make run-publisher
make run-all
```

### Infrastructure-only development

```text
make infra-up
make infra-down
make infra-restart
make infra-logs
make infra-ps
```

`infra-up` should start:

```text
postgres
redis
kafka
kafka-init
migrate
```

This allows the Go application to run directly on the host.

### Full Docker environment

```text
make docker-build
make docker-up
make docker-down
make docker-restart
make docker-logs
make docker-ps
make docker-reset
```

`docker-reset` must explicitly remove volumes.

Ordinary `docker-down` must preserve volumes.

### Database migrations

```text
make migrate-up
make migrate-down
make migrate-status
make migration-new name=create_repository_changes
```

`migration-new` must fail when `name` is missing.

### Kafka inspection

```text
make kafka-topics
make kafka-describe
make kafka-groups
make kafka-lag
make kafka-dlq
make kafka-recreate-topics
```

`kafka-recreate-topics` is destructive and must require an explicit confirmation variable.

### Smoke tests

```text
make smoke
make smoke-concurrency
make smoke-refresh-batch
make smoke-shutdown
```

Scripts must have deterministic exit codes.

## 25.2 Standard developer workflows

Host application with Docker infrastructure:

```bash
make bootstrap
make infra-up
make run-all
```

Everything in Docker:

```bash
make docker-up
make smoke
```

Before commit:

```bash
make ci
```

Reset local state:

```bash
make docker-reset
make docker-up
```

---

## 26. Required Tests

The assignment requires at least two meaningful tests. The project must implement more than the minimum.

## 26.1 Concurrent duplicate POST

Use:

- Real PostgreSQL
- Real Redis
- Fake GitHub HTTP server

Procedure:

1. Start many simultaneous requests for the same repository.
2. Coordinate start using a barrier.
3. Assert:
   - Exactly one HTTP 201.
   - All others HTTP 409.
   - No HTTP 500.
   - One PostgreSQL row.
   - One GitHub request when cache coordination is available.

Run under the race detector.

## 26.2 Cache exactly-one-call

1. Empty cache.
2. Start many concurrent requests.
3. Block fake GitHub response.
4. Ensure all waiters are active.
5. Release response.
6. Assert exactly one GitHub request.
7. Assert all callers receive equivalent data.

## 26.3 Lock-holder failure

1. Simulate a lock holder that never writes cache.
2. Let the lock expire.
3. Assert another caller acquires the lock.
4. Assert a stale token cannot release the newer lock.

## 26.4 Kafka producer/consumer round trip

Use real Kafka.

Assert:

- Event publishes successfully.
- `repo_id` is message key.
- Schema decodes.
- Headers propagate.
- Manual offset commit works.

## 26.5 Kafka duplicate delivery

Deliver the same `batch_job_id` twice.

Assert:

- One GitHub refresh effect.
- One successful batch job.
- No batch double count.
- Both Kafka records can be settled safely.

## 26.6 Crash after database commit

1. Process job.
2. Commit repository and job success.
3. Simulate crash before offset commit.
4. Restart consumer.
5. Receive duplicate.
6. Assert no second GitHub call.
7. Commit duplicate record.

## 26.7 Outbox duplicate publication

1. Publish event.
2. Simulate publisher crash before marking it published.
3. Recover publishing lease.
4. Publish again.
5. Assert worker idempotency.

## 26.8 Kafka unavailable during refresh-all

Assert:

- HTTP returns 202 after PostgreSQL commit.
- Outbox rows remain pending.
- Kafka recovery allows later publication.
- Batch eventually completes.

## 26.9 Retry scheduling

Assert:

- Retryable failure creates future outbox event.
- Kafka offset is committed only after retry state is durable.
- Event is not published before `available_at`.
- Rate-limit deferral does not consume an attempt.

## 26.10 Retry exhaustion and DLQ

After three attempts:

- Batch job becomes failed.
- Safe reason is stored.
- DLQ outbox event exists.
- DLQ Kafka message is published.
- Main-topic offset is committed.
- Batch becomes complete.

## 26.11 Global maximum concurrency

1. Publish jobs across all five partitions.
2. Run multiple worker replicas.
3. Instrument active GitHub calls.
4. Assert maximum active calls never exceeds five.

## 26.12 Change-feed commit ordering

Create concurrent transactions that start and commit in different orders.

Assert:

- Cursor sequence follows commit-safe ordering.
- Advancing cursor does not miss events.
- Replaying cursor may return duplicates.
- `event_id` supports deduplication.

## 26.13 Graceful API shutdown

Start a long-running request and send SIGTERM.

Assert:

- API stops accepting new requests.
- Existing request drains within deadline.
- Connections close in order.

## 26.14 Graceful worker shutdown

Send SIGTERM while processing a job.

Assert:

- Job finishes and commits, or
- Retry is durably scheduled, or
- Offset remains uncommitted and Kafka redelivers.

The job must not be lost.

## 26.15 SQL statistics

Test:

- Empty database
- Normal values
- Null language
- Empty language
- Tied languages
- Deterministic tie-break

---

## 27. Validation Commands

Required local validation:

```bash
go fmt ./...
go vet ./...
go test ./...
go test -race ./...
staticcheck ./...
```

Use `golangci-lint` only with a committed configuration.

Required Make validation:

```bash
make ci
```

Required clean Docker validation:

```bash
make docker-reset
make docker-up
make smoke
make smoke-concurrency
make smoke-refresh-batch
make smoke-shutdown
```

Verify from an empty state.

---

## 28. README Requirements

The README must contain:

### Overview

- Service purpose
- Main capabilities
- Runtime modes

### Running locally

```bash
cp .env.example .env
make docker-up
```

Also document host application development:

```bash
make infra-up
make run-all
```

### Architecture

Describe:

- API request flow
- GitHub cache flow
- Refresh-all flow
- Transactional outbox
- Kafka consumer flow
- Retry scheduling
- DLQ
- Change feed
- Shutdown

### Kafka choice

Explain:

- Why Kafka was selected
- Why IBM Sarama was selected
- At-least-once semantics
- Manual offset control
- Five-partition global concurrency
- Message key choice
- Consumer-group behavior
- Retry scheduling outside Kafka handlers

### Transactional outbox

Explain:

- PostgreSQL/Kafka dual-write problem
- Outbox claim lease
- Duplicate publication window
- Worker idempotency

### Redis cache design

Explain:

- Five-minute TTL
- Cross-replica lock
- Ownership token
- Lock expiry
- Lock timeout relationship
- Redis failure policy
- Exact scope of one-call guarantee

### Rate-limit design

Explain:

- `Retry-After`
- Rate-limit reset
- Shared Redis cooldown
- Retry scheduling
- Attempt accounting

### Pagination

Explain:

- Composite cursor
- Tie-breaker
- Cursor validation
- Mutable dataset limitation

### Change-feed guarantee

State:

- Commit-safe ordering
- No missed committed events when cursor is advanced correctly
- Duplicate delivery is possible
- Consumer deduplicates by `event_id`
- Deletion tombstones

### Graceful shutdown

Explain:

- API shutdown order
- Worker offset behavior
- Outbox publisher recovery
- Dependency close order

### Tests

List important tests and explain why each boundary is real or mocked.

### Conscious trade-offs

Include:

- Kafka as a log rather than a native delayed-task queue
- PostgreSQL outbox for retry scheduling
- Five partitions limiting throughput
- Commit-sequence serialization
- Cursor pagination on mutable sort fields
- Single-broker local Kafka
- Hard delete with tombstones
- Redis dependency for distributed GitHub-call coordination

### AI workflow

Document:

- Tools used
- Prompts used
- Generated code that was rejected or corrected
- How generated code was validated
- Commands run after each change
- Manual review performed

“The AI wrote it” is not an acceptable explanation for any implementation decision.

---

## 29. Development Sequence

## Phase 1: Repository foundation

Deliver:

- Go module
- Package structure
- Configuration
- `slog`
- Runtime modes
- Makefile
- Dockerfile
- Compose files
- Health endpoints
- Kafka topic initializer

Gate:

```bash
make build
make test-unit
make docker-up
make kafka-describe
```

## Phase 2: Ent schemas and migrations

Deliver all required entities, constraints, indexes, and migrations.

Gate:

```bash
make ent-generate
make migrate-up
make generate-check
make test-integration
```

## Phase 3: GitHub client

Deliver:

- Shared HTTP client
- Typed errors
- Header parsing
- Timeout handling
- Test server tests

## Phase 4: Redis cache and coordination

Deliver:

- Five-minute cache
- Distributed lock
- Ownership-safe release
- Lock expiry recovery
- Shared rate-limit cooldown

## Phase 5: Repository service and API

Deliver:

- Repository CRUD endpoints
- Notes-only patch
- Synchronous refresh
- Cursor pagination
- SQL stats
- Change feed
- Consistent errors

## Phase 6: Kafka platform layer

Deliver:

- IBM Sarama configuration
- Producer
- Consumer group
- Admin/topic validation
- Message schema
- Manual offset management
- Graceful close

## Phase 7: Batch and outbox

Deliver:

- Refresh-all batch snapshot
- Batch jobs
- Outbox rows
- Multi-publisher-safe claiming
- Kafka publication
- Publishing-lease recovery

## Phase 8: Worker

Deliver:

- Five-partition processing
- Database processing lease
- Idempotent duplicate handling
- Retry scheduling
- Three-attempt limit
- DLQ
- Batch status
- Graceful shutdown

## Phase 9: Hardening and final verification

Deliver:

- Failure-path tests
- Race tests
- Docker restart tests
- Shutdown tests
- README
- AI workflow documentation
- Smoke scripts

Final gate:

```bash
make ci
make docker-reset
make docker-up
make smoke
make smoke-concurrency
make smoke-refresh-batch
make smoke-shutdown
```

---

## 30. AI Prompt Playbook

Use AI tools aggressively, but require every generated change to include tests, validation commands, and an explanation of invariants.

### Prompt 1: Project contract

```text
You are assisting with a greenfield production-shaped Go backend.

Locked stack:
- Go 1.25+
- Gin
- Ent
- PostgreSQL
- Redis
- Apache Kafka
- github.com/IBM/sarama
- Docker Compose
- Make
- slog

Do not replace any locked technology.

Engineering rules:
- PostgreSQL is authoritative.
- Kafka delivery is at least once.
- Every Kafka effect must be idempotent.
- Use a transactional outbox for PostgreSQL-to-Kafka publication.
- Disable automatic Kafka offset commits.
- Commit offsets only after durable PostgreSQL outcomes.
- Do not call network services inside database transactions.
- Propagate context through every request and job path.
- Do not use process-local locks for cross-replica guarantees.
- Do not ignore errors.
- Do not invent library APIs.
- Validate against the exact versions in go.mod.
- Add tests with every behavior.
- Run race tests for concurrency.
- Explain invariants, crash windows, and shutdown behavior.
- Do not log credentials.

For every task return:
1. Requirement implemented.
2. Invariants.
3. Files changed.
4. Implementation.
5. Tests.
6. Validation commands.
7. Self-review and remaining risks.
```

### Prompt 2: Scaffold

```text
Create the greenfield project scaffold using the locked stack.

Required:
- One binary with api, worker, publisher, all, migrate, and healthcheck modes.
- Package structure from the architecture specification.
- Environment configuration with validation.
- slog logging.
- Multi-stage Dockerfile.
- compose.yaml and compose.test.yaml.
- PostgreSQL, Redis, Kafka KRaft, kafka-init, migrate, api, worker, publisher.
- Pinned images.
- Health checks.
- Non-root runtime.
- Makefile with all required development targets.
- .env.example without secrets.

Do not add business logic yet.
Return complete files and validation commands.
```

### Prompt 3: Ent schemas

```text
Implement Ent schemas and versioned PostgreSQL migrations for:

Repository
Batch
BatchJob
JobAttempt
OutboxEvent
RepositoryChange
ChangeSequence

Implement all required unique constraints, enum validation, indexes, processing leases, publishing leases, retry dates, and timestamps.

Do not use runtime automatic migration as the production migration strategy.

Add real PostgreSQL migration tests.
Explain every index and constraint.
```

### Prompt 4: GitHub client

```text
Implement the GitHub repository client.

Requirements:
- Shared http.Client.
- Optional GITHUB_TOKEN.
- Explicit transport and total timeouts.
- Context propagation.
- Bounded response body.
- Typed errors for not found, rate limited, unauthorized, server failure, timeout, network failure, invalid response, and caller cancellation.
- Parse Retry-After in both supported forms.
- Parse X-RateLimit-Remaining and X-RateLimit-Reset.
- No internal retry loop.
- No secrets in logs.

Add table-driven httptest.Server tests.
```

### Prompt 5: Redis cache and lock

```text
Implement Redis caching and distributed request coalescing.

Requirements:
- Five-minute response cache.
- Normalized repository cache key.
- SET NX PX lock.
- Random ownership token.
- Cache recheck after lock acquisition.
- Lua compare-and-delete release.
- Bounded jittered waiter polling.
- Context cancellation.
- Lock expiry recovery.
- Explicit refresh invalidation with the same coalescing path.
- Redis failures must not silently bypass distributed coordination.
- Shared GitHub rate-limit cooldown.

Add race-enabled concurrency tests proving one upstream call.
```

### Prompt 6: Repository service

```text
Implement repository application services:

- Create from GitHub.
- Get by ID.
- Cursor-paginated list.
- Notes-only update.
- Delete with tombstone.
- Synchronous refresh.
- SQL statistics.
- Commit-ordered change events.

Requirements:
- No GitHub call inside a database transaction.
- PostgreSQL unique constraint resolves concurrent duplicates.
- Notes survive refresh.
- Every mutation and change event are one transaction.
- Change sequence is commit ordered.
- Cursor validates sort, filter, and version.

Add service and integration tests.
```

### Prompt 7: Gin API

```text
Implement the Gin API.

Requirements:
- All required endpoints.
- One error JSON shape.
- Request-ID middleware.
- slog access logs.
- Strict JSON policy.
- Body-size limits.
- Typed error-to-status mapping.
- Retry-After response header when known.
- No Ent, Redis, Kafka, or GitHub calls directly from handlers.
- Correct 201, 202, 204, 400, 404, 409, 500, 502, 503, and 504 responses.

Add table-driven handler tests.
```

### Prompt 8: IBM Sarama layer

```text
Implement the Kafka platform layer using the exact pinned github.com/IBM/sarama version.

Requirements:
- Producer.
- Consumer group.
- Admin/topic validation.
- Main topic and DLQ topic.
- Main topic partition count must equal five.
- Manual offset management.
- Automatic offset commit disabled.
- Ordered processing within each partition.
- Producer acknowledgements.
- Safe producer retries and idempotent mode when supported by the pinned version.
- Bounded fetch and message sizes.
- Dial and request timeouts.
- Context-aware wrappers.
- Partition assignment and revocation logging.
- Graceful close.

Do not invent Sarama APIs.
Add real Kafka integration tests.
```

### Prompt 9: Transactional outbox

```text
Implement the PostgreSQL transactional-outbox publisher.

Requirements:
- Refresh-all creates batch, batch jobs, and outbox events in one transaction.
- Publisher claims due events with FOR UPDATE SKIP LOCKED.
- Ownership token and publishing lease.
- Kafka publication outside database transaction.
- Mark published only after acknowledgement.
- Recover expired publishing leases.
- Support available_at for delayed retries.
- Multiple publisher replicas.
- Duplicate publication expected and safe.
- Graceful shutdown.

Test Kafka outage, duplicate publication, lease recovery, and publisher crash windows.
```

### Prompt 10: Worker

```text
Implement the Kafka refresh worker.

Requirements:
- IBM Sarama consumer group.
- Consumer group github-repo-refresh-v1.
- Main topic exactly five partitions.
- repo_id message key.
- One active handler per partition.
- Maximum global refresh concurrency five.
- Manual offset commit after durable PostgreSQL outcome.
- Batch-job processing lease.
- Duplicate terminal delivery is a no-op.
- Maximum three GitHub attempts.
- Shared Redis rate-limit gate.
- Cooldown deferrals do not consume attempts.
- Retry by future outbox event.
- No long sleep inside partition handler.
- Terminal failure creates DLQ outbox event.
- Repository update, change event, and job success are one transaction.
- Graceful rebalance and SIGTERM handling.
- No lost job.

Add tests for all crash windows.
```

### Prompt 11: Docker and Make audit

```text
Audit Dockerfile, Compose, and Makefile.

Check:
- Pinned images and dependencies.
- Kafka KRaft listeners work from containers and host.
- Main topic has exactly five partitions.
- kafka-init is idempotent.
- Migrations are a one-shot service.
- Real health checks.
- Non-root runtime.
- CA certificates.
- No baked secrets.
- Named volumes.
- docker-down preserves data.
- docker-reset is explicitly destructive.
- Make targets match CI.
- Integration tests clean up.
- generate-check detects dirty output.
- Destructive Kafka targets require confirmation.

Return concrete corrections and regression tests.
```

### Prompt 12: Production review

```text
Perform an adversarial production-readiness review.

Check:
- Kafka auto-commit accidentally enabled.
- Offset committed before durable state.
- Out-of-order partition commits.
- PostgreSQL/Kafka dual-write gaps.
- Outbox rows stuck in publishing.
- Duplicate publication.
- Duplicate worker effects.
- Processing leases that do not recover.
- Retry attempts counted during rate-limit deferral.
- Long sleeps blocking Kafka partitions.
- Main topic partition count not equal to five.
- More than five active GitHub calls.
- Rebalances during active processing.
- Shutdown before durable settlement.
- Redis lock ownership bugs.
- Lock TTL shorter than GitHub timeout.
- Context loss.
- Network calls inside database transactions.
- Missing HTTP and dependency timeouts.
- Secrets in logs.
- Cursor designs that can miss reverse-order commits.
- Nondeterministic tests using arbitrary sleeps.

Classify findings by severity and provide a regression test for every critical or high finding.
```

---

## 31. Final Acceptance Checklist

### Stack

- [ ] Go 1.25+
- [ ] Gin
- [ ] Ent
- [ ] PostgreSQL
- [ ] Redis
- [ ] Kafka
- [ ] IBM Sarama
- [ ] Docker Compose
- [ ] Makefile
- [ ] slog

### API

- [ ] All required endpoints
- [ ] Consistent errors
- [ ] Correct status codes
- [ ] Request IDs
- [ ] Body limits
- [ ] Timeouts
- [ ] Cursor pagination
- [ ] SQL statistics
- [ ] Change feed

### Repository correctness

- [ ] Case-normalized unique identity
- [ ] Concurrent duplicate POST safe
- [ ] Notes preserved
- [ ] Hard delete with tombstone
- [ ] Commit-ordered changes

### Redis

- [ ] Five-minute cache
- [ ] Cross-replica lock
- [ ] Ownership-safe release
- [ ] Lock expiry recovery
- [ ] Shared rate-limit cooldown
- [ ] No unsafe fallback during Redis outage

### Kafka

- [ ] IBM Sarama pinned
- [ ] Main topic exactly five partitions
- [ ] DLQ topic
- [ ] Manual offset commits
- [ ] Auto-commit disabled
- [ ] `repo_id` message key
- [ ] One active job per partition
- [ ] Maximum global concurrency five
- [ ] Transactional outbox
- [ ] Duplicate publication safe
- [ ] Duplicate delivery safe
- [ ] Retry through future outbox event
- [ ] DLQ after permanent failure or exhaustion

### Worker

- [ ] Processing lease
- [ ] Three-attempt maximum
- [ ] Correct attempt accounting
- [ ] Durable retry scheduling
- [ ] Atomic repository/job success
- [ ] Graceful rebalance
- [ ] Graceful SIGTERM
- [ ] No lost job

### Docker

- [ ] Multi-stage build
- [ ] Non-root runtime
- [ ] CA certificates
- [ ] Pinned images
- [ ] Kafka KRaft
- [ ] Internal and host listeners
- [ ] Health checks
- [ ] Kafka initialization
- [ ] Migration container
- [ ] Named volumes
- [ ] Clean empty-state startup

### Makefile

- [ ] Help
- [ ] Bootstrap
- [ ] Pinned tools
- [ ] Generate
- [ ] Migration
- [ ] Build
- [ ] Unit tests
- [ ] Integration tests
- [ ] Race tests
- [ ] Lint
- [ ] Docker lifecycle
- [ ] Kafka inspection
- [ ] Smoke tests
- [ ] CI target
- [ ] Explicit destructive targets

### Tests

- [ ] Concurrent duplicate POST
- [ ] Cache one-call guarantee
- [ ] Lock-holder death
- [ ] Kafka round trip
- [ ] Duplicate Kafka delivery
- [ ] Crash after DB commit
- [ ] Outbox duplicate publication
- [ ] Kafka outage during refresh-all
- [ ] Retry scheduling
- [ ] Retry exhaustion and DLQ
- [ ] Global concurrency five
- [ ] Change-feed ordering
- [ ] API shutdown
- [ ] Worker shutdown
- [ ] SQL statistics
- [ ] Race detector passes

### Documentation

- [ ] Run instructions
- [ ] Architecture
- [ ] Kafka and IBM Sarama decision
- [ ] At-least-once guarantee
- [ ] Transactional outbox
- [ ] Redis lock scope
- [ ] Rate-limit behavior
- [ ] Pagination limitation
- [ ] Change-feed guarantee
- [ ] Shutdown behavior
- [ ] Conscious trade-offs
- [ ] AI workflow
