# Summary — Task 01: Repository Foundation

Status: **Complete.** All task verification gates pass from a clean build cache.

## What was delivered

### 1. Go module with pinned direct dependencies

All five locked-technology dependencies are pinned as **direct** `require` entries in `go.mod`, each at a real, resolvable, verified version (resolved from the Go module proxy, not invented):

| Dependency | Pinned version | Role |
| --- | --- | --- |
| `github.com/gin-gonic/gin` | `v1.12.0` | HTTP framework |
| `entgo.io/ent` | `v0.14.6` | ORM / code generation |
| `github.com/jackc/pgx/v5` | `v5.10.0` | PostgreSQL driver (pool + conn) |
| `github.com/redis/go-redis/v9` | `v9.21.0` | Redis client |
| `github.com/IBM/sarama` | `v1.60.0` | Apache Kafka client |

`go 1.25.0` directive is set. All transitive dependencies are pinned in `go.sum`; `go mod verify` reports "all modules verified".

### 2. Package structure (matches INITIAL_PLAN §6)

Single application binary at `cmd/github-tracker/main.go`. Package boundaries under `internal/` keep transport, infrastructure, and domain behavior separate:

- `internal/app` — runtime lifecycle and mode dispatch (`api`, `worker`, `publisher`, `all`, `migrate`, `healthcheck`).
- `internal/platform/{config,database,redis,kafka,logging,signal}` — infrastructure clients and helpers.
- `internal/github` — upstream gateway (client, dto, errors, cache, lock, ratelimit).
- `internal/repository` (+ `entstore`) — repository model, store interface, Ent-backed store.
- `internal/batch` — batch model, store, service.
- `internal/outbox` — transactional outbox model, store, publisher.
- `internal/worker` — consumer, refresh handler, lease, retry, DLQ.
- `internal/changefeed` — cursor, store, service.
- `internal/httpapi` (+ `middleware`, `handlers`) — router, error rendering, middleware, handlers.
- `ent/schema` — handwritten Ent schemas (generated code stays separate).
- `migrations/` — versioned migrations (Task 03 fills these).
- `test/integration` — build-tag-gated home for real-infrastructure tests.

No business logic was added. Each package currently holds a package-clause anchor (or the foundational mode/dispatch code in `internal/app`) so later tasks fill in behavior without reorganizing.

### 3. Single application entry point with mode selector

`cmd/github-tracker/main.go` wires `signal.NotifyContext` (SIGINT/SIGTERM) into a root context and dispatches to `app.Run`. `internal/app` exposes:

- A typed `Mode` and the six required mode constants.
- `ParseMode(args)` with clear errors for missing/empty/unknown modes.
- `Run(ctx, mode)` dispatching to per-mode runners.

Modes are selectable today but return `ErrNotImplemented` — real dependency wiring is Task 02's scope.

### 4. Bootstrap / compile test proving the dependency graph

`internal/app/bootstrap_test.go` (external `app_test` package) constructs the core type of each locked dependency **without network I/O**:

- `gin.New()` — Gin engine.
- `sarama.NewConfig()` + `cfg.Validate()` — Sarama config.
- `pgxpool.ParseConfig(...)` — PostgreSQL pool config (parsing only, no connection).
- `redis.NewClient(&redis.Options{...})` — Redis client (constructed, not dialed).

Plus table-driven coverage of mode selection (`TestParseMode`) and the full required mode set (`TestRuntimeModesAreSelectable`).

### 5. Makefile command surface

`Makefile` implements the foundational targets used across all tasks: `fmt`, `vet`, `build`, `test`/`test-unit`, `test-race`, `test-integration`, `mod-tidy`, `mod-verify`, `generate` (Ent), `ci` (fmt → vet → mod-verify → build → test-unit), `clean`, `bootstrap`. Infrastructure-dependent targets (`infra-up`, `docker-up`, `smoke`) are present as explicit placeholders that fail with a helpful message until their owning tasks implement them.

### 6. `.env.example`

Contains every variable name from INITIAL_PLAN §22 (application, PostgreSQL, Redis, Kafka, GitHub, worker, outbox) with safe local-development defaults only. Kafka constants match AGENTS.md exactly (`github-repo-refresh-v1`, `github.repo.refresh.v1`, `github.repo.refresh.dlq.v1`, 5 partitions). `GITHUB_TOKEN`, `KAFKA_SASL_*`, and `REDIS_PASSWORD` are blank. No real credentials.

## Verification (task gate)

Run from a clean build cache (`go clean -cache`, `rm -rf bin`):

```
make fmt          # OK
make build        # OK — bin/github-tracker produced
make test-unit    # ok  github.com/ilkerkorkut/connect-tickets/internal/app
go mod verify     # all modules verified
make ci           # OK (fmt + vet + mod-verify + build + test-unit)
```

Binary behavior verified manually:
- No args → usage error listing all six modes.
- Unknown mode → clear error.
- Valid mode → "runtime mode not implemented in this phase" (expected; Task 02 wires behavior).

## Constraints honored

- **No second binary** — all runtime roles are modes of `cmd/github-tracker`.
- **Generated Ent code kept separate** — handwritten schemas live in `ent/schema`; generation is driven by `make generate` / `ent/tools.go`.
- **No business logic leaks** — handlers do not import Ent/Redis/Kafka; package anchors carry no behavior.
- **No code comments, no TODOs** — per AGENTS.md.
- **No secrets committed** — `bin/` and `.env` are gitignored; `.env.example` has no real credentials.

## Conscious trade-offs

- **Pinning Ent without importing it in app code:** Ent is a code-generation dependency, not a runtime import in the foundation. It is pinned via a build-tag-gated blank import (`ent/tools.go`, `//go:build tools`) so `go mod tidy` keeps it as a direct `require`. This is the standard Go idiom for pinning codegen tool dependencies.
- **Modes return `ErrNotImplemented` rather than being fully wired:** Task 02 owns configuration, dependency construction, and real mode behavior. Wiring them here would blur the task boundary and duplicate Task 02's validated-config work.
- **pgx chosen as the PostgreSQL driver:** Ent's idiomatic Postgres driver, and the library the later Ent-generated client will build on. No `database/sql` abstraction introduced yet.

## Files added/changed

- `go.mod`, `go.sum` — module + pinned direct dependencies.
- `cmd/github-tracker/main.go` — entry point.
- `internal/app/*.go` — mode set, parser, dispatch, per-mode runners.
- `internal/app/bootstrap_test.go` — dependency-graph proof + mode tests.
- `internal/platform/**`, `internal/{github,repository,batch,outbox,worker,changefeed,httpapi}/**` — package boundary anchors per §6.
- `ent/schema/schema.go`, `ent/generate.go`, `ent/tools.go` — Ent schema home and codegen pinning.
- `migrations/.gitkeep` — migration directory anchor.
- `test/integration/bootstrap_integration_test.go` — integration test home (build-tag gated).
- `Makefile` — command surface.
- `.env.example` — environment variable names with safe defaults.
- `.gitignore` — added `bin/`.
