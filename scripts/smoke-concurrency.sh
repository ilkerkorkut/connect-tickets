#!/usr/bin/env bash
set -euo pipefail

base_url="${BASE_URL:-http://localhost:18080}"
payload='{"owner":"octocat","name":"Hello-World"}'
status_file="$(mktemp)"
trap 'rm -f "$status_file"' EXIT

for _ in 1 2 3 4 5; do
	(curl --silent --show-error --output /dev/null --write-out '%{http_code}\n' --header 'Content-Type: application/json' --data "$payload" "$base_url/api/repos" >>"$status_file") &
done
wait

grep -qx '201' "$status_file"
test "$(grep -Ec '^(201|409)$$' "$status_file")" -eq 5
