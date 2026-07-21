#!/usr/bin/env bash
set -euo pipefail

go test -count=1 -race -v -tags=testcontainers ./internal/service/worker -run '^(TestBatchConcurrentRefreshSucceedsWithExactlyOnceGitHub|TestBatchRetryAndDLQWithExactlyOnceSemantics)$'
