#!/usr/bin/env bash
set -euo pipefail

base_url="${BASE_URL:-http://localhost:18080}"
curl --fail --silent --show-error "$base_url/health/live" >/dev/null
curl --fail --silent --show-error "$base_url/health/ready" >/dev/null
