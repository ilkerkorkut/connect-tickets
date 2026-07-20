# Task 04: GitHub Client

## Objective

Implement a bounded, context-aware client for `GET /repos/{owner}/{name}` that preserves actionable upstream failure classes.

## Depends on

- Task 02

## Source

- `PRODUCT_REQUIREMENTS.md`: GitHub integration and Production Requirement 1
- `INITIAL_PLAN.md`: sections 10.1-10.2, Phase 3, and Prompt 4

## Deliverables

- An injected `http.Client` with explicit transport, request deadline, connection limits, bounded body reading, and a configurable GitHub base URL.
- Optional `GITHUB_TOKEN` authentication and an explicit user agent without ever logging the token.
- Strict mapping of successful JSON into the GitHub-owned repository fields.
- Typed failures for not found, unauthorized, rate limited, server error, timeout, network failure, invalid response, and caller cancellation.
- Rate-limit metadata containing status, `Retry-After`, remaining quota, and reset time when safely available.
- `Retry-After` parsing for delta seconds and HTTP dates.
- Safe, bounded upstream diagnostics suitable for internal logging but not unrestricted response bodies.

## Constraints

- Do not retry invisibly in the low-level client; worker policy owns retries.
- Preserve the distinction between caller cancellation and an HTTP client timeout.
- Treat GitHub 403 as rate limiting only when headers/body classification supports it; also recognize 429.
- Never make this client responsible for Redis caching or database persistence.

## Tests and verification

- Use `httptest.Server` to cover success, 404, 401, rate-limited 403, 429, 5xx, malformed success JSON, oversized body, timeout, cancellation, and connection failure.
- Verify both `Retry-After` formats and rate-limit headers.
- Assert authentication is optional and secrets never occur in errors or captured logs.

```bash
make fmt
go test ./internal/github/... -count=1
make test-unit
```

## Done when

- Every specified failure is distinguishable with `errors.Is`/`errors.As` or an equivalent typed contract.
- All requests honor caller context and configured deadlines.
- Tests are deterministic and make no calls to the public GitHub API.
