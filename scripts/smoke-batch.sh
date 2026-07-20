#!/usr/bin/env bash
set -euo pipefail

base_url="${BASE_URL:-http://localhost:18080}"
curl --fail --silent --show-error --request POST "$base_url/api/repos/refresh-all" >/dev/null
