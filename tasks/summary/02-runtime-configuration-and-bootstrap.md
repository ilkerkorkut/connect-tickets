# Summary — Task 02: Runtime Configuration and Bootstrap

Status: **Complete.** All task verification gates pass under the race detector. Binary bootstraps end-to-end with real config and fails fast on invalid config.

## What was delivered

### 1. Validated environment configuration (`internal/platform/config`)

- One `Config` struct with ten nested sub-structs (`App`, `Log`, `HTTP`, `Database`, `Redis`, `Cache`, `Kafka`, `GitHub`, `Worker`, `Outbox`) covering every variable in INITIAL_PLAN §22 and every separate timeout/limit in §21.
- `Load()` reads `os.Getenv` once at the app boundary; `LoadFrom(loader)` accepts a `Loader` func for hermetic tests. Business packages never touch env vars directly.
- **Parsing philosophy:** missing (empty) → use the field's validated default; present-but-malformed → typed validation error (no silent swallows). Parsers: `durationOr`, `intOr`, `int32Or`, `int64Or`, `boolOr`, `stringsCSVOr`, `stringOr`.
- `ValidationError` aggregates all field problems and lists **field names only** — never values. `errors.As(err, &ve)` works for typed handling.
- Validation covers: required values (DATABASE_URL, KAFKA_BROKERS), ranges (HTTP_PORT 1–65535), positivity (all timeouts, pool sizes, attempt counts), consistency (max idle ≤ max open for both DB and Redis pools), the locked Kafka topic/group/partition constants, and SASL mechanism whitelist (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512 with required username).

### 2. Structured logging with redaction (`internal/platform/logging`)

- `NewLogger(LogConfig, io.Writer)` builds an `slog.Logger` with level from `LOG_LEVEL` (debug/info/warn/error) and JSON or text format from `LOG_FORMAT`. Typed errors for unknown level/format.
- **Secret redaction by construction** via `HandlerOptions.ReplaceAttr`: a deny-list of attribute keys (`token`, `authorization`, `password`, `secret`, `credential`, `database_url`, `redis_password`, `kafka_sasl_password`, `api_key`, `private_key`, …) is replaced with `"[redacted]"` before serialization. Non-sensitive attributes pass through untouched.
- Context correlation helpers for every §20 field: `WithRequestID`/`RequestIDFrom`, plus job_id, batch_id, repo_id, event_id. `ScopedLogger(ctx, base)` derives a logger carrying all present correlation IDs. Empty values are never stored.

### 3. Root signal context (`internal/platform/signal`)

- `RootContext(ctx)` wraps `signal.NotifyContext(SIGINT, SIGTERM)` so the entry point stays minimal and signal handling is unit-testable.

### 4. Platform clients (lazy / non-network at bootstrap)

- **`database.NewPool`** — parses the URL via `pgxpool.ParseConfig`, sets `MaxConns`/`MinConns`/`MaxConnLifetime`/`MaxConnIdleTime`/`HealthCheckPeriod`. The pool is **lazy** (no connection opened at construction). On parse failure it returns `ErrInvalidDatabaseURL` with **no raw error text** (pgx errors can echo the URL with embedded credentials).
- **`redis.NewClient`** — builds `redis.Options` with pool size, min idle, and dial/read/write timeouts; returns `ErrMissingRedisAddr` on empty address.
- **`kafka.NewSaramaConfig`** — validates the locked topic/group/partition values, then returns a tuned `sarama.Config`: idempotent producer with `WaitForAll` acks and `Return.Successes`, **auto-commit disabled** (manual offset commit per architecture rule), sticky rebalance, SASL/TLS wiring. **No network call** — the actual `sarama.Client`/producer/consumer is deferred to the mode that uses it (Task 08/09/10), so an API-only or migrate start does not depend on Kafka being up.
- **`kafka.ValidateTopics`** + locked constants (`github.repo.refresh.v1`, `github.repo.refresh.dlq.v1`, `github-repo-refresh-v1`, 5 partitions).

### 5. Bootstrap + lifecycle (`internal/app`)

- `Resources` holds opened clients and a LIFO close chain. `Close()` iterates closers in reverse, logs each failure, and returns `errors.Join(...)` of all close errors.
- `bootstrap(ctx, cfg)` constructs logger → db pool → redis client → sarama config in order. On **any** step failure, `finalizeBootstrap` closes everything already opened (reverse order) and joins the close errors to the cause — startup failures leave no leaked resources.
- A `closerFunc` adapter bridges `pgxpool.Pool.Close()` (returns no error) to `io.Closer`.
- **Each mode** (`api`, `worker`, `publisher`, `migrate`, `healthcheck`) loads config, bootstraps resources, logs "starting mode", and returns. The long-running modes block on `<-ctx.Done()` and return the context error; `migrate` and `healthcheck` return `ErrNotImplemented` (real logic is Tasks 03/12).
- **`all`** shares one bootstrap and composes api+worker+publisher as goroutines via `golang.org/x/sync/errgroup.WithContext`. It returns the first material failure; a parent-context cancellation (SIGTERM or timeout) is treated as a clean shutdown (returns nil), not a failure. Siblings are cancelled on first failure.
- Testable seams (`runAPIFn`/`runWorkerFn`/`runPublisherFn`, `newLoggerFunc`, `kafkaNewConfig`) are swappable package vars, enabling deterministic composition tests without real infrastructure.

### 6. Entry point

- `cmd/github-tracker/main.go` uses `signal.RootContext` then dispatches via `app.Run`.

## Verification (task gate)

```bash
make format   # OK
make test-unit  # ok app, config, kafka, logging, signal (race detector)
make build    # OK — bin/github-tracker
make ci       # fmt + vet + mod-verify + build + test-unit — OK
go mod verify # all modules verified
```

End-to-end binary behavior verified:
- **Invalid/missing config → fails fast** before any work: `load configuration: configuration validation failed: DATABASE_URL is required; KAFKA_BROKERS is required` (field names only, exit 1).
- **Valid config (`.env.example`)** → bootstraps, logs structured JSON (`bootstrap complete`, `starting mode`), blocks on context until SIGTERM, then logs `shutdown requested` with reason.

## Tests (35 total)

- **`config_test.go`** — defaults applied when env unset; overrides parsed (incl. CSV brokers with whitespace); 13 invalid-value cases (port range/format, pool consistency, partitions, attempts, SASL, log level, batch size); malformed durations/integers rejected (not silently swallowed); **secret-safety** (REDIS_PASSWORD, GITHUB_TOKEN, KAFKA_SASL_PASSWORD, and a DATABASE_URL with embedded password set to a sentinel — asserted absent from every error message); typed `*config.ValidationError` with `errors.As`.
- **`logging/logger_test.go`** — level/format parsing; unknown level/format rejected; JSON structure; **secret redaction** (token/authorization/password/database_url attrs replaced with `[redacted]`, sentinel absent from output, non-sensitive attrs preserved); context correlation round-trip + missing + empty-not-stored; scoped logger carries correlation.
- **`signal/signal_test.go`** — `RootContext` cancels on real SIGTERM and SIGINT (sent to own PID); parent cancellation propagates. Bounded waits, no arbitrary sleeps.
- **`app/lifecycle_test.go`** — LIFO close ordering (deterministic with a recorder); all errors collected even when some closes fail; empty/idempotent `Close`; `finalizeBootstrap` preserves cause and closes opened resources; `waitForShutdown` returns the context error.
- **`app/all_test.go`** — `all` composes all three modes (call counters); returns the first material failure (`errors.Is`); siblings are cancelled on failure (deterministic, bounded wait). Uses swappable seams + a noop `Resources`, so no real clients or env reads.
- **`kafka/config_test.go`** — valid sarama config (idempotent producer, WaitForAll, auto-commit disabled, successes returned); rejects every topic/group/partition mismatch; rejects empty brokers; locked constants asserted.
- Task 01's `bootstrap_test.go` (dependency-graph proof) still passes.

## Constraints honored

- No env reads outside `config`; no generic config abstraction.
- No single global timeout — every operation has its own duration field, all configurable with validated defaults.
- Never logs tokens/passwords/full-URLs/raw-errors — enforced by both the redacting `ReplaceAttr` and the secret-safe validators.
- All long-running modes take a parent `context.Context`.
- sarama client deferred; pgxpool/redis lazy → no network at bootstrap.
- Partial bootstrap failure closes opened resources in reverse order.
- `.env.example` now has **complete parity** with `config.go` (0 missing, 0 extra).

## Conscious trade-offs

- **Malformed values error rather than fall back.** Earlier draft silently used defaults for unparseable durations/integers; changed because silently masking a typo'd `CACHE_TTL=5mn` to 5m hides operator error in production. Empty (unset) still falls back to the default.
- **`REDIS_ADDR`, `KAFKA_CLIENT_ID`, `APP_MODE`, `APP_ENV` have defaults** (localhost:6379, github-tracker, all, development). Only truly-deployment-specific values (DATABASE_URL, KAFKA_BROKERS) are hard-required. This lets `make run-all` work locally with minimal env.
- **`golang.org/x/sync/errgroup` promoted to a direct dependency** for `all`'s concurrent composition with first-failure semantics and automatic sibling cancellation. Already an indirect dep; promoting it is cheaper and more correct than hand-rolling an errgroup.
- **SCRAM client generator deferred to Task 08.** sarama v1.60.0 requires a caller-provided `SCRAMClientGeneratorFunc` (backed by `xdg-go/scram`) for SCRAM auth. Wiring that here would pull a new dep and is genuine Kafka-platform work. Task 02 sets the mechanism/credentials/enable flags correctly; the SCRAM client factory lands with the full Kafka layer in Task 08.
- **`migrate`/`healthcheck` return `ErrNotImplemented` after a successful bootstrap** rather than blocking on context — they are one-shot commands, not daemons. Real migration application (Task 03) and probing (Task 12) replace this stub.

## Files added/changed

New:
- `internal/platform/config/{config.go, validation.go, defaults.go, config_test.go}`
- `internal/platform/logging/{logger.go, context.go, logger_test.go}`
- `internal/platform/signal/{signal.go, signal_test.go}`
- `internal/platform/database/client.go`
- `internal/platform/redis/client.go`
- `internal/platform/kafka/{config.go, topics.go, errors.go, config_test.go}`
- `internal/app/{lifecycle.go, seams.go, lifecycle_test.go, all_test.go}`

Modified:
- `internal/app/{run.go, api.go, worker.go, publisher.go, all.go, migrate.go, healthcheck.go}` — real wiring + composition.
- `cmd/github-tracker/main.go` — `signal.RootContext`.
- `go.mod` / `go.sum` — `golang.org/x/sync` promoted to direct.
- `.env.example` — complete parity with config (added 18 resource-limit vars from §21).
