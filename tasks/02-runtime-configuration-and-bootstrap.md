# Task 02: Runtime Configuration and Bootstrap

## Objective

Implement validated environment configuration, the required single-binary runtime modes, dependency construction, and baseline structured logging.

## Depends on

- Task 01

## Source

- `INITIAL_PLAN.md`: sections 4, 5, 20-22, and Prompt 2
- `AGENTS.md`: request/job context and structured logging requirements

## Deliverables

- Support the modes `api`, `worker`, `publisher`, `all`, `migrate`, and `healthcheck`.
- Make `all` run API, worker, and publisher under a shared cancellation context and return the first material runtime failure.
- Load environment configuration with validated defaults and fail fast on malformed, missing, contradictory, or unsafe values.
- Define separate settings for:
  - HTTP server and application request deadlines;
  - PostgreSQL URL, query timeout, and connection pool;
  - Redis URL, pool, dial/read/write timeouts, cache TTL, lock TTL, and waiter bounds;
  - Kafka brokers, topics, consumer group, partition expectation, auth/TLS, message/fetch bounds, and timeouts;
  - GitHub base URL, optional token, request timeout, and transport pool;
  - worker lease, maximum attempts, concurrency, and shutdown timeout;
  - outbox polling, batch size, publication lease, attempts, and shutdown timeout.
- Build a `log/slog` logger with environment-selectable level and format and safe error attributes.
- Add constructors that wire concrete dependencies only at the application boundary.
- Ensure startup failures close already-opened resources in reverse order.

## Constraints

- Do not use one global timeout for unrelated operations.
- Never log tokens, passwords, full connection URLs with credentials, or raw lower-layer errors.
- All long-running modes must accept a parent `context.Context`.
- Business packages must not read environment variables directly.

## Tests and verification

- Table-test configuration defaults, overrides, malformed durations, invalid limits, absent required values, and secret-safe error messages.
- Test each mode selection and unknown-mode failure.
- Test partial bootstrap failure cleanup deterministically.

```bash
make format
make test-unit
make build
```

## Done when

- Every required mode is selectable from the one binary.
- Invalid configuration stops before serving or consuming.
- Resource construction and cleanup are deterministic and context-aware.
- Configuration names are reflected in `.env.example`.

