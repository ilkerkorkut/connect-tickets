# Task 08: Kafka Platform with IBM Sarama

## Objective

Implement the pinned IBM Sarama producer, consumer-group, and topic-administration layer with manual offset control and bounded resource settings.

## Depends on

- Task 02

## Source

- `INITIAL_PLAN.md`: section 13, Phase 6, and Prompt 8
- `AGENTS.md`: Kafka requirements

## Fixed topology

- Main topic: `github.repo.refresh.v1`, exactly five partitions.
- DLQ topic: `github.repo.refresh.dlq.v1`.
- Consumer group: `github-repo-refresh-v1`.
- Message key: decimal or otherwise stable representation of `repo_id`.
- Validate the existing main-topic partition count at startup and fail readiness if it is not five.

## Deliverables

- Context-aware producer wrapper with required acknowledgements, bounded retries, delivery result handling, message/header size checks, and idempotent mode only when correctly supported by the pinned Sarama version and its required settings.
- Consumer group with automatic offset commit disabled, explicit marking/commit only when requested by the worker, and one active record per partition.
- Topic administration/validation used by startup and local initialization.
- Versioned refresh message schema containing stable event/job/batch/repository identities, attempt metadata as required, and correlation headers.
- Versioned DLQ schema support without secrets or unrestricted upstream content.
- Explicit dial, request, fetch, production acknowledgement, and close timeouts; TLS/SASL configuration when enabled.
- Assignment, revocation, rebalance, and close behavior exposed safely to lifecycle code.

## Constraints

- Verify every Sarama API against the exact version in `go.mod`; do not infer APIs from another fork/version.
- Preserve ordering within a partition and never start multiple active records from the same partition.
- Keep business retry decisions out of the Kafka transport layer.
- Do not treat Kafka as the source of truth for batch status.

## Tests and verification

- Unit-test schema version rejection, size limits, key generation, header propagation, and config validation.
- Real Kafka integration test creates/validates topics and performs a producer-consumer round trip.
- Assert the `repo_id` key, message decoding, headers, ordered per-partition handling, disabled auto-commit, and explicit offset settlement.
- Test startup failure when the main topic has a partition count other than five.

```bash
make fmt
make test-unit
make infra-up
make kafka-describe
make test-integration
```

## Done when

- The main topic is verified at exactly five partitions.
- A consumed record remains replayable until the worker reports a durable PostgreSQL outcome.
- Producer/consumer shutdown is bounded and rebalance-safe.
