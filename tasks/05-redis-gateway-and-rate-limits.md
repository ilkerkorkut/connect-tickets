# Task 05: Redis Cache, Distributed Coalescing, and Rate Limits

## Objective

Wrap the GitHub client with a five-minute distributed cache, ownership-safe cache-miss coalescing, explicit-refresh behavior, and a shared rate-limit cooldown.

## Depends on

- Task 04

## Source

- `PRODUCT_REQUIREMENTS.md`: Production Requirements 2-3
- `INITIAL_PLAN.md`: sections 11-12, Phase 4, and Prompt 5

## Cache and lock behavior

- Normalize owner/name and use stable versioned cache and lock keys.
- Cache valid GitHub repository responses for five minutes.
- On a miss, acquire a Redis `SET NX PX` lock with a cryptographically random ownership token.
- Recheck the cache after lock acquisition before calling GitHub.
- Release only the caller's lock with an atomic Lua compare-and-delete script.
- Waiters use bounded jittered polling, honor cancellation, recheck cache, and may acquire after lock expiry.
- Set the lock TTL longer than the GitHub request deadline and document the actual guarantee boundary.
- Explicit refresh invalidates the cache and then enters the same coalescing path; concurrent refreshes for one repository coalesce.

## Shared cooldown behavior

- Store the earliest permissible GitHub-call time in Redis.
- Interpret `Retry-After`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, and status classification from typed GitHub errors.
- Update cooldown atomically by retaining the later timestamp.
- Expose a context-aware cooldown check for workers and add jitter when many jobs resume.

## Failure policy

- Redis is not a durable batch/retry store.
- Do not silently bypass coordination during Redis failure.
- Synchronous calls return `cache_coordination_unavailable`; workers can classify the condition for durable retry without calling GitHub.

## Tests and verification

- Race-enabled concurrency test: many callers for the same uncached normalized repository cause one upstream call while Redis is available and the lock outlives the request.
- Test cache hit/TTL, cache recheck, different repository keys, waiter cancellation, lock expiry recovery, and ownership-safe release.
- Test concurrent explicit refresh coalescing.
- Test atomic later-cooldown selection, cooldown expiry, and Redis outage behavior.
- Prefer real Redis for coordination semantics; keep time and jitter injectable for deterministic tests.

```bash
make fmt
make test-unit
make test-integration
make test-race
```

## Done when

- Cross-replica coalescing has a tested, accurately documented scope.
- A dead lock holder cannot permanently block requests, and an old owner cannot delete a newer lock.
- Redis outages cannot fan out uncontrolled GitHub calls.
