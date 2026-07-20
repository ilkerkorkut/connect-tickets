# Task 07: Gin HTTP API

## Objective

Expose the complete REST contract through thin Gin handlers with strict input validation, consistent safe errors, request correlation, body limits, and request deadlines.

## Depends on

- Task 06

## Source

- `PRODUCT_REQUIREMENTS.md`: Endpoints and Production Requirements 1, 4, and 5
- `INITIAL_PLAN.md`: sections 8-9, 20.1, Phase 5, and Prompt 7

## Endpoints

- `POST /api/repos`: strict `{owner,name}` JSON, unknown-field rejection, normalized create, HTTP 201; concurrent losers receive 409 `repo_already_tracked`.
- `GET /api/repos`: `language`, `sort=stars_desc|updated_desc`, `limit`, and opaque `cursor`; deterministic keyset pagination.
- `GET /api/repos/:id`: stored repository or 404 `tracked_repo_not_found`.
- `PATCH /api/repos/:id`: strict notes-only body and bounded notes.
- `DELETE /api/repos/:id`: delete with the specified success response/status and a durable tombstone through the service.
- `POST /api/repos/:id/refresh`: synchronous coalesced refresh that preserves notes.
- `POST /api/repos/refresh-all`: no inline refresh work; return HTTP 202 with `{ "batch_id": "..." }` after the PostgreSQL transaction commits.
- `GET /api/batches/:id`: authoritative totals, pending count, successes, safe failed reasons, and completion flag.
- `GET /api/repos/stats`: SQL-derived aggregate response.
- `GET /api/repos/changes`: `since` and `limit`, ascending events, `next_cursor`, and `has_more`.

## Error contract

Use this shape everywhere:

```json
{"error":{"code":"error_code","message":"Safe message","request_id":"..."}}
```

Safely support validation details. Implement the status/code table from `INITIAL_PLAN.md` section 9, including 400 input/cursor errors, local and GitHub 404s, duplicate 409, GitHub 502/503/504 classifications, Redis 503, and generic internal 500. Add `Retry-After` when a safe GitHub retry time exists.

## Middleware and boundaries

- Accept or create a validated request ID, attach it to context, response headers, errors, and structured logs.
- Log method, route, status, duration, request ID, and safe error code.
- Enforce maximum request body size, server timeouts, and an application request deadline.
- Recover panics into the standard error shape without leaking stack traces to clients.
- Handlers validate, call services, and render; they never access Ent, Redis, Kafka, or the GitHub client.

## Tests and verification

- Handler tests cover every endpoint, malformed/unknown JSON, validation, bad IDs/cursors, not found, typed upstream failures, body limit, and request ID propagation.
- Real PostgreSQL concurrency test sends simultaneous duplicate POSTs and asserts one 201, all others 409, one row, and no 500s.
- Verify refresh-all returns after the database commit even when Kafka is unavailable.
- Verify safe errors never contain raw SQL/Redis/Kafka errors, tokens, stack traces, or full GitHub bodies.

```bash
make fmt
make test-unit
make test-integration
make test-race
```

## Done when

- Every required endpoint and status mapping is covered by tests.
- HTTP cancellation reaches all service I/O.
- The API contract is consistent on success, validation failure, dependency failure, panic, and timeout paths.
