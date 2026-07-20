#!/usr/bin/env bash
set -euo pipefail

database_url="${TEST_DATABASE_URL:-postgres://tracker:tracker@localhost:15432/tracker?sslmode=disable}"
redis_addr="${TEST_REDIS_ADDR:-localhost:16379}"

TEST_DATABASE_URL="$database_url" TEST_REDIS_ADDR="$redis_addr" go test -count=1 -race -tags=integration ./internal/route -run '^TestConcurrentRepositoryCreation$'
