# GitHub Repository Tracker

The service tracks GitHub repositories through a Gin API and refreshes them asynchronously with Kafka.

## Run

Copy `.env.example` to `.env`, then run:

```bash
make bootstrap
make infra-up
make run-api
make run-worker
```

`make docker-up` starts PostgreSQL, Redis, Kafka, Atlas migrations, the API, and the worker. The API listens on `http://localhost:18080`.

## Runtime layout

`cmd/api` builds the API binary. `cmd/worker` builds the worker binary. The worker consumes refresh messages and publishes the PostgreSQL transactional outbox, allowing API and worker replicas to scale independently.

```text
cmd/                 API and worker entrypoints
config/              environment configuration
internal/client/     GitHub client contract, models, errors, and Resty implementation
internal/cache/      Redis-backed GitHub cache, lock, and rate-limit coordination
internal/event/      Kafka events and consumer infrastructure
internal/producer/   transactional outbox publisher
internal/service/    repository, batch, change-feed, health, and worker services
internal/handler/    Gin handlers
internal/route/      HTTP router and server lifecycle
internal/middleware/ HTTP middleware
internal/request/    HTTP request models
internal/response/   HTTP response models
pkg/                 PostgreSQL, logging, and signal utilities
```

PostgreSQL is authoritative. Ent owns all application persistence access. Atlas applies the versioned SQL files in `migrations/`; `atlas.sum` protects their integrity. Redis holds the five-minute GitHub cache, distributed cache-miss lock, and rate-limit cooldown. Kafka provides at-least-once refresh delivery. Consumer effects are guarded by durable job claims and idempotent state transitions.

## Operational choices

Refresh work is keyed by repository ID and published through the transactional outbox. The worker processes a maximum of one active record per Kafka partition and commits only after a durable outcome. Retries create a future outbox event; terminal failures create a DLQ outbox event and persist a safe failure reason.

The GitHub gateway uses Resty with explicit response-body and request-time limits. Its distributed Redis lock ensures concurrent cache misses for one repository perform one upstream request across replicas. Refresh invalidates the cache.

Repository list pagination is cursor-based. Change-feed cursors are commit-ordered sequence values: consumers can see a repeated last record but cannot miss a committed change; deletion tombstones remain in the feed.

5 partitions are used for the Kafka topic. Each partition is consumed by one worker replica, which guarantees that only one worker processes a given repository at a time. The worker uses a durable job claim to ensure that only one replica can process a given repository at a time, even if the consumer is restarted or fails over.

## Validation

```bash
make test-unit
make test-race
make smoke-concurrency
make ci
```

The integration suite starts isolated PostgreSQL, Redis, Kafka, and an Atlas migration service through `compose.test.yaml`.

`make smoke-concurrency` starts isolated PostgreSQL, Redis, and Kafka containers through Testcontainers. It verifies both concurrency scenarios: five concurrent HTTP creates yield exactly one GitHub request, one `201 Created`, and four `409 Conflict` responses; and a GitHub `429` with `Retry-After` creates a shared Redis cooldown so five workers, consuming one message from each Kafka partition, schedule durable retries without another GitHub request. Verbose structured output identifies the scenario, synchronization gate, dependency readiness, each outcome, and the final invariant counts.
