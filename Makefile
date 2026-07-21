SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

BIN_DIR := $(CURDIR)/bin
API_BIN := $(BIN_DIR)/api
WORKER_BIN := $(BIN_DIR)/worker
GO ?= go
DOCKER ?= docker
COMPOSE ?= $(DOCKER) compose
COMPOSE_PROJECT ?= github-tracker
TEST_COMPOSE_PROJECT ?= github-tracker-test
COMPOSE_FILE ?= compose.yaml
TEST_COMPOSE_FILE ?= compose.test.yaml
VERSION ?= dev
GOFLAGS ?= -trimpath
LDFLAGS := -X main.version=$(VERSION)
INTEGRATION_TAGS ?= integration
ENT_VERSION ?= v0.14.6
ATLAS_VERSION ?= 0.36.2
GOLANGCI_LINT_VERSION ?= v2.12.2
ENT := $(BIN_DIR)/ent
ATLAS := $(DOCKER) run --rm -v "$(CURDIR)/migrations:/migrations" arigaio/atlas:$(ATLAS_VERSION)
GOLANGCI_LINT := $(BIN_DIR)/golangci-lint
CONFIRM ?=
name ?=

COMPOSE_CMD = $(COMPOSE) --project-name $(COMPOSE_PROJECT) --env-file .env -f $(COMPOSE_FILE)
TEST_COMPOSE_CMD = $(COMPOSE) --project-name $(TEST_COMPOSE_PROJECT) -f $(TEST_COMPOSE_FILE)

.PHONY: help bootstrap tools deps generate ent-generate generate-check fmt fmt-check vet lint test test-unit test-race test-integration test-all ci build run-api run-worker infra-up infra-down infra-restart infra-logs infra-ps docker-build docker-up docker-down docker-restart docker-logs docker-ps docker-reset migrate-up migrate-down migrate-status migration-new kafka-topics kafka-describe kafka-groups kafka-lag kafka-dlq kafka-recreate-topics smoke smoke-concurrency smoke-concurrency-batch smoke-refresh-batch smoke-shutdown clean

help: ## Show this help
	@awk 'BEGIN { FS = ":.*##"; printf "Usage:\n  make \\033[36m<target>\\033[0m\n\nTargets:\n" } /^[a-zA-Z0-9_-]+:.*##/ { printf "  \\033[36m%-24s\\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

bootstrap: ## Create local configuration and install prerequisites
	@command -v $(GO) >/dev/null
	@command -v $(DOCKER) >/dev/null
	@if [ ! -f .env ]; then cp .env.example .env; fi
	@$(MAKE) tools deps

tools: ## Install pinned development tools into ./bin
	@mkdir -p $(BIN_DIR)
	@GOBIN=$(BIN_DIR) $(GO) install entgo.io/ent/cmd/ent@$(ENT_VERSION)
	@GOBIN=$(BIN_DIR) $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

deps: ## Download and verify Go module dependencies
	@$(GO) mod download
	@$(GO) mod verify

generate: ent-generate ## Generate all repository code

ent-generate: tools ## Regenerate Ent code
	@$(ENT) generate --feature sql/lock ./ent/schema

generate-check: tools ## Fail when code generation changes tracked output
	@$(MAKE) generate
	@git diff --exit-code -- ent migrations

fmt: ## Format Go source
	@$(GO) fmt ./...

fmt-check: ## Verify Go source formatting
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*'))"

vet: ## Run go vet
	@$(GO) vet ./...

lint: tools ## Run the pinned Go linter
	@$(GOLANGCI_LINT) run ./...

test: test-unit ## Run unit tests

test-unit: ## Run unit tests
	@$(GO) test -count=1 ./...

test-race: ## Run unit tests with the race detector
	@$(GO) test -count=1 -race ./...

test-integration: ## Run isolated infrastructure integration tests
	@set +e; $(TEST_COMPOSE_CMD) up --build --abort-on-container-exit --exit-code-from test-runner; status=$$?; $(TEST_COMPOSE_CMD) down --volumes --remove-orphans; exit $$status

test-all: test-unit test-race test-integration ## Run every test suite

ci: generate-check fmt-check vet lint test-unit test-race test-integration build ## Run the complete CI gate

build: ## Build the API and worker binaries
	@mkdir -p $(BIN_DIR)
	@$(GO) build $(GOFLAGS) -o $(API_BIN) ./cmd/api
	@$(GO) build $(GOFLAGS) -o $(WORKER_BIN) ./cmd/worker

run-api: ## Run only the API on the host
	@$(GO) run ./cmd/api

run-worker: ## Run only the worker on the host
	@$(GO) run ./cmd/worker

infra-up: ## Start only infrastructure, topic initialization, and migrations
	@$(COMPOSE_CMD) up -d postgres redis kafka kafka-init migrate

infra-down: ## Stop infrastructure while preserving development volumes
	@$(COMPOSE_CMD) stop postgres redis kafka kafka-init migrate

infra-restart: infra-down infra-up ## Restart infrastructure services

infra-logs: ## Follow infrastructure logs
	@$(COMPOSE_CMD) logs -f postgres redis kafka kafka-init migrate

infra-ps: ## Show infrastructure service state
	@$(COMPOSE_CMD) ps postgres redis kafka kafka-init migrate

docker-build: ## Build the application runtime image
	@$(COMPOSE_CMD) build api worker

docker-up: ## Start the full Docker environment
	@$(COMPOSE_CMD) up -d --build

docker-down: ## Stop Docker services while preserving named volumes
	@$(COMPOSE_CMD) down --remove-orphans

docker-restart: docker-down docker-up ## Restart the full Docker environment

docker-logs: ## Follow all Docker service logs
	@$(COMPOSE_CMD) logs -f

docker-ps: ## Show all Docker service state
	@$(COMPOSE_CMD) ps

docker-reset: ## Destructively remove Docker services and named volumes
	@$(COMPOSE_CMD) down --volumes --remove-orphans

migrate-up: ## Apply committed Atlas migrations
	@$(COMPOSE_CMD) run --rm migrate

migrate-down: ## Refuse implicit destructive migration rollback
	@echo "migrate-down is destructive and requires an explicit migration rollback implementation" >&2
	@exit 1

migrate-status: ## Show applied migration versions
	@$(COMPOSE_CMD) exec -T postgres psql -U tracker -d tracker -c 'SELECT version, applied_at FROM schema_migrations ORDER BY version'

migration-new: ## Create an empty Atlas migration with name=<name>
	@test -n "$(name)"
	@$(ATLAS) migrate new --dir file:///migrations $(name)

kafka-topics: ## List Kafka topics
	@$(COMPOSE_CMD) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

kafka-describe: ## Describe required Kafka topics
	@$(COMPOSE_CMD) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic github.repo.refresh.v1
	@$(COMPOSE_CMD) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic github.repo.refresh.dlq.v1

kafka-groups: ## List Kafka consumer groups
	@$(COMPOSE_CMD) exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --list

kafka-lag: ## Inspect the refresh worker consumer group
	@$(COMPOSE_CMD) exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group github-repo-refresh-v1

kafka-dlq: ## Safely consume the dead-letter topic from the beginning
	@$(COMPOSE_CMD) exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic github.repo.refresh.dlq.v1 --from-beginning --timeout-ms 5000

kafka-recreate-topics: ## Destructively delete and recreate required Kafka topics with CONFIRM=yes
	@test "$(CONFIRM)" = "yes"
	@$(COMPOSE_CMD) exec -T kafka /bin/bash /scripts/recreate-topics.sh

smoke: ## Run the realistic Docker API and worker smoke suite
	@scripts/smoke-test.sh

smoke-concurrency: ## Run isolated Testcontainers API and worker concurrency smoke suites
	@scripts/smoke-concurrency.sh

smoke-concurrency-batch: ## Run isolated Testcontainers batch concurrency smoke suite
	@scripts/smoke-concurrency-batch.sh

smoke-refresh-batch: ## Run the refresh-batch smoke suite
	@scripts/smoke-batch.sh

smoke-shutdown: ## Run the graceful-shutdown smoke suite
	@scripts/smoke-shutdown.sh

clean: ## Remove local build artifacts
	@rm -rf $(BIN_DIR)
