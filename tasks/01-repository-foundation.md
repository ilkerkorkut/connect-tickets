# Task 01: Repository Foundation

## Objective

Create a buildable greenfield Go repository with the required package boundaries and pinned technology choices, without implementing business behavior yet.

## Source

- `INITIAL_PLAN.md`: sections 1-6, Phase 1, Prompt 1, and Prompt 2
- `PRODUCT_REQUIREMENTS.md`: Stack and Ground Rules
- `AGENTS.md`: locked stack and architecture boundaries

## Deliverables

- Initialize the Go 1.25+ module and pin direct dependencies for Gin, Ent, PostgreSQL, Redis, and `github.com/IBM/sarama`.
- Create one application entry point and package structure for:
  - application bootstrap and runtime lifecycle;
  - configuration and structured logging;
  - Gin handlers, middleware, and response rendering;
  - repository and batch application services;
  - Ent persistence stores and migrations;
  - GitHub gateway and Redis coordination;
  - Kafka producer, consumer, and administration;
  - transactional outbox publisher and refresh worker.
- Add test package locations that allow unit and real-infrastructure integration tests to remain separate.
- Add the initial Makefile command surface needed to build, format, test, and generate code. Later tasks may fill in targets that depend on infrastructure.
- Add a minimal `.env.example` containing names only or safe local defaults, never real credentials.

## Constraints

- Handlers may depend only on application-facing interfaces.
- Interfaces must be narrow and use-case driven; do not introduce generic repository or service abstractions.
- Keep generated Ent code separate from handwritten schemas and stores.
- Do not add a second binary; runtime roles will be modes of the same binary.
- Do not add frontend code or replace any locked technology.

## Tests and verification

- Add a compile-level test or bootstrap test proving the initial dependency graph can be constructed without starting external work.
- Verify formatting and dependency metadata.

```bash
make fmt
make build
make test-unit
go mod verify
```

## Done when

- The repository builds from a clean checkout.
- Package boundaries match the architecture and contain no business logic leaks.
- Direct dependencies are pinned and explainable.
- No secrets or machine-specific paths are committed.
