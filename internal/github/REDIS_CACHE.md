# Redis coordination

Repository cache keys use `github:repo:v1:{lowercase-owner/lowercase-name}` and expire after five minutes. Misses are coalesced across replicas with `github:repo-lock:v1:{normalized-name}` using Redis `SET NX PX`; each holder has a cryptographically random token and can release only its own lock through a Lua compare-and-delete operation.

The one-upstream-call guarantee applies while Redis is reachable and the lock outlives the configured GitHub request deadline. A process that dies can leave at most one lock TTL of waiting; waiters then recheck the cache and can acquire the expired lock. A lock can expire during a pathologically slow upstream call, so the guarantee does not extend beyond that TTL. Redis faults fail closed with `cache_coordination_unavailable`; callers and workers do not bypass the cache/lock and call GitHub directly.

Explicit refresh deletes the cache entry before entering the same lock path. A shared cooldown is stored at `github:rate-limit:cooldown-until:v1`; atomic Lua update retains the later time, and workers can use `WaitForCooldown` with cancellation-aware jitter.
