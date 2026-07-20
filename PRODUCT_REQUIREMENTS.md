WHAT WE WANT TO SEE

This is not a CRUD test. We're evaluating whether you can build a small but production-shaped service — the kind that survives flaky third-party APIs, concurrent users, and a 2am urgent production on call. Specifically:

1. How you work with AI tools (Claude, Cursor, Copilot, Windsurf, Aider, etc.) and how you validate what they generate
2. Senior Go fundamentals — concurrency done right, failure handling, contexts, clean boundaries
3. Distributed-systems basics — caching with Redis, async processing through a queue, at-least-once semantics
4. Production instincts — what you log, what you time out, what you protect against

With AI assistance, we expect you to ship this comfortably in a day. If you find yourself hand-writing boilerplate for hours, that itself is a signal — please use your AI tools.

Backend only — no frontend required.

---

THE TASK

Build a REST API that tracks GitHub repositories — built the way you'd build a real service.

STACK

• Go 1.25+, Gin, Ent, PostgreSQL
• Redis — required, for caching
• A message queue of your choice for async work — Redis Streams, asynq, RabbitMQ, NATS, Kafka, or anything what you know best; be ready to defend the pick.
• Everything boots with docker-compose (Postgres + Redis + your queue if separate + the app).

Integrate with the GitHub public API — https://api.github.com/repos/{owner}/{name} — no auth required for basic use. Support an optional GITHUB_TOKEN env var.

ENDPOINTS

• POST /api/repos — body { "owner": "...", "name": "..." }. Fetches from GitHub, persists, returns the stored record. Reject duplicates SAFELY UNDER CONCURRENT REQUESTS: two simultaneous POSTs for the same repo must not create two rows or return two 500s — one wins, one gets a clean 409.

• GET /api/repos — list with ?language= filter, ?sort=stars_desc|updated_desc, and cursor or offset pagination (your pick — be ready to defend it).

• GET /api/repos/:id — single repo.

• PATCH /api/repos/:id — update a user-editable notes field.

• DELETE /api/repos/:id — remove from the watchlist.

• POST /api/repos/:id/refresh — re-fetch from GitHub synchronously and update stored fields.

• POST /api/repos/refresh-all — ASYNC: enqueue one refresh job per tracked repo onto your message queue and return 202 Accepted with { "batch_id": "..." } immediately. Do NOT do the work in the request handler.

• GET /api/batches/:id — batch status: { total, pending, succeeded, failed: [{repo_id, reason}] }. Poll-able until complete.

• GET /api/repos/stats — { total, total_stars, top_language } — computed in SQL, not by loading rows into memory.

• GET /api/repos/changes?since=<cursor> — repos changed since a given point, for a hypothetical polling consumer. Design the cursor so a consumer never misses a change and can tolerate receiving the same change twice. Document the guarantee you chose.

Ent schema minimum: id, owner, name, full_name (unique), description, stars, language, html_url, notes, fetched_at, created_at, updated_at — plus whatever your batch tracking needs.

THE WORKER

A consumer (same binary with a flag, or a second binary — your call, defend it) that:

1. Consumes refresh jobs from the queue with bounded concurrency, max 5 in flight.

2. Processes each job IDEMPOTENTLY — the queue may deliver a job twice (at-least-once). Processing the same job twice must not corrupt data or double-count in the batch status.

3. Retries transient failures (GitHub 5xx / 429) via the queue's retry mechanism or your own redelivery logic: max 3 attempts, backoff with jitter, honoring Retry-After. Permanently failed jobs land in a dead-letter destination (a DLQ, a dead stream, or a failed_jobs table — your call) with the reason recorded.

4. Updates batch status as jobs complete, so GET /api/batches/:id reflects reality.

---

PRODUCTION REQUIREMENTS

1. GitHub client hygiene. Explicit timeout. Context plumbed from handler/worker to the HTTP call. Distinguish error classes: not-found / rate-limited / unauthorized / server-error / network — and map each to a sensible HTTP response or retry decision. Everything collapsing into 500 = automatic fail.

2. Redis cache for GitHub responses, 5-minute TTL. Concurrent requests for the same uncached repo must result in EXACTLY ONE GitHub call — use a Redis-based lock (SET NX with expiry) or an equivalent mechanism, and handle the edge where the lock holder dies mid-fetch. Explicit refresh invalidates the cache key. Document what happens across multiple app replicas.

3. Rate-limit awareness. Respect Retry-After and X-RateLimit-Remaining. When GitHub rate-limits you mid-batch, remaining jobs back off — not hammer.

4. Consistent error responses — one JSON shape everywhere (e.g. { "error": { "code": "...", "message": "..." } }), correct HTTP status codes.

5. Structured logging with slog. Request-ID middleware for HTTP; job-ID in every worker log line; both propagated via context.

6. Graceful shutdown — API AND worker. SIGTERM → API stops accepting requests and drains in-flight; worker stops consuming, finishes (or cleanly re-queues) in-flight jobs, closes connections. A job that was mid-processing at shutdown must not be lost — it must be redelivered. signal.NotifyContext + proper ordering.

7. Config via env vars (DB URL, Redis URL, queue config, port, optional GitHub token). .env.example is fine.

8. At least TWO meaningful tests — required, not optional. One must cover the concurrent-duplicate POST case, the worker's idempotent processing, or the cache's exactly-one-call guarantee. Testcontainers welcome; a well-mocked test is acceptable if you can defend the boundary you mocked.

---

GROUND RULES

• Use your AI tools aggressively — then own every line. In the demo you'll be asked to explain and modify anything in the repo. "The AI wrote that part" is not an accepted answer to "why is this here?"

• A README is required: how to run it, your architectural choices, WHICH TRADE-OFFS YOU CONSCIOUSLY TOOK (queue choice, delivery semantics, cache lock design, pagination), and your AI workflow.