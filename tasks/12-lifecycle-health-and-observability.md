# Task 12: Lifecycle, Health, and Observability

## Objective

Make API, worker, publisher, and combined modes operable under startup failure, dependency loss, rebalancing, and SIGTERM without losing in-flight work.

## Depends on

- Task 07
- Task 09
- Task 11

## Source

- `PRODUCT_REQUIREMENTS.md`: Production Requirements 5-7
- `INITIAL_PLAN.md`: sections 19-22 and Phase 9

## Graceful shutdown

Use `signal.NotifyContext` and enforce this order:

1. Mark readiness false.
2. Stop accepting new HTTP requests.
3. Stop claiming outbox rows and consuming Kafka records.
4. Drain HTTP requests within the HTTP shutdown deadline.
5. Let active worker jobs finish within the worker deadline.
6. Leave unfinished jobs uncommitted and non-terminal so Kafka redelivery or lease recovery can occur.
7. Finish or safely abandon active publications.
8. Close the HTTP server, Sarama consumer, Sarama producer, Redis, and Ent/PostgreSQL in dependency order.
9. Flush logs where applicable.

Handle Kafka partition revocation by stopping new work for revoked claims, completing only safe in-flight work within the session deadline, and never committing an offset without a durable outcome.

## Health and readiness

- Provide liveness and readiness for the API and a `healthcheck` runtime mode for non-HTTP services.
- Readiness validates PostgreSQL, Redis, Kafka connectivity, and the expected Kafka topic configuration as appropriate to the selected role.
- Liveness must not hide a deadlocked process, while transient dependency loss should primarily affect readiness.
- Dependency checks need individual bounded timeouts and safe errors.

## Structured logs and correlation

- HTTP fields: `request_id`, method, route template, safe path, status, duration, remote IP, user agent, and error code.
- Worker fields: event/job/batch/repository/worker IDs, attempt, topic/partition/offset, duration, result, and error code.
- Outbox fields: event/type/aggregate/publisher IDs, publish attempt, topic, partition key, duration, result, and error code.
- GitHub fields: normalized repository, cache result, lock result, status, rate-limit data, duration, and safe error code.
- Propagate request IDs and job IDs through context and downstream Kafka headers where applicable.

## Resource protection

- Enforce separate configured timeouts for HTTP phases, request execution, GitHub, Redis, PostgreSQL, Kafka, producer acknowledgement, processing/publishing leases, and each shutdown role.
- Enforce database/Redis pools, GitHub transport pools, body/page limits, Kafka fetch/message limits, worker concurrency, and outbox batch size.
- Verify no log path reveals secrets, raw database errors, or unrestricted upstream bodies.

## Tests and verification

- Graceful API test proves new requests stop and an in-flight request drains within deadline.
- Graceful worker test proves an unfinished record is not committed and is later redelivered/recovered.
- Test publisher shutdown, consumer rebalance, readiness transitions, dependency timeouts, and reverse-order cleanup.
- Capture logs to assert required correlation fields and secret/error redaction.

```bash
make fmt
make test-unit
make test-integration
make test-race
```

## Done when

- SIGTERM cannot lose a mid-processing job or acknowledge work without durable state.
- Every role exposes meaningful readiness and closes within configured deadlines.
- Production logs correlate a request/job across boundaries without leaking sensitive data.
