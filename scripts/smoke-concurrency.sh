#!/usr/bin/env bash
set -euo pipefail

go test -count=1 -race -v -tags=testcontainers ./internal/route ./internal/service/worker -run '^(TestConcurrentRepositoryCreation|TestConcurrentWorkerRateLimitSchedulesRetries)$'
