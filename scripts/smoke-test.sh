#!/usr/bin/env bash
set -euo pipefail

api_url="${API_URL:-http://localhost:18080}"
owner="${GITHUB_OWNER:-octocat}"
name="${GITHUB_NAME:-hello-world}"
timeout_seconds="${POLL_TIMEOUT_SECONDS:-60}"
work_dir="$(mktemp -d)"
repo_id=""

cleanup() {
  if [[ -n "$repo_id" ]]; then
    curl --silent --output /dev/null --request DELETE "$api_url/api/repos/$repo_id" || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

fail() {
  printf 'smoke test failed: %s\n' "$1" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

request() {
  local method="$1"
  local path="$2"
  local expected_status="$3"
  local payload="${4:-}"
  local response_file="$work_dir/response.json"
  local status
  local args=(--silent --show-error --output "$response_file" --write-out '%{http_code}' --request "$method" "$api_url$path")
  if [[ -n "$payload" ]]; then
    args+=(--header 'Content-Type: application/json' --data "$payload")
  fi
  status="$(curl "${args[@]}")" || fail "request failed: $method $path"
  if [[ "$status" != "$expected_status" ]]; then
    printf 'unexpected HTTP status for %s %s: got %s, want %s\n' "$method" "$path" "$status" "$expected_status" >&2
    cat "$response_file" >&2
    exit 1
  fi
  cat "$response_file"
}

wait_for_health() {
  local deadline=$((SECONDS + timeout_seconds))
  until curl --fail --silent --output /dev/null "$api_url/healthz"; do
    (( SECONDS < deadline )) || fail "API did not become healthy within ${timeout_seconds}s"
    sleep 1
  done
}

require_command curl
require_command jq
wait_for_health

created="$(request POST /api/repos 201 "{\"owner\":\"$owner\",\"name\":\"$name\"}")"
repo_id="$(jq --raw-output '.id' <<<"$created")"
[[ -n "$repo_id" && "$repo_id" != "null" ]] || fail "create response did not include an id"
[[ "$(jq --raw-output '.full_name' <<<"$created")" == "$owner/$name" ]] || fail "created repository does not match $owner/$name"

duplicate="$(request POST /api/repos 409 "{\"owner\":\"$owner\",\"name\":\"$name\"}")"
[[ "$(jq --raw-output '.error.code' <<<"$duplicate")" == "repo_already_tracked" ]] || fail "duplicate request returned the wrong error code"

notes="smoke test $(date -u +%Y%m%dT%H%M%SZ)"
updated="$(request PATCH "/api/repos/$repo_id" 200 "{\"notes\":\"$notes\"}")"
[[ "$(jq --raw-output '.notes' <<<"$updated")" == "$notes" ]] || fail "notes were not persisted"

refreshed="$(request POST "/api/repos/$repo_id/refresh" 200)"
[[ "$(jq --raw-output '.full_name' <<<"$refreshed")" == "$owner/$name" ]] || fail "synchronous refresh returned the wrong repository"

batch="$(request POST /api/repos/refresh-all 202)"
batch_id="$(jq --raw-output '.batch_id' <<<"$batch")"
[[ -n "$batch_id" && "$batch_id" != "null" ]] || fail "refresh-all response did not include a batch id"

deadline=$((SECONDS + timeout_seconds))
while true; do
  batch_status="$(request GET "/api/batches/$batch_id" 200)"
  if [[ "$(jq --raw-output '.complete' <<<"$batch_status")" == "true" ]]; then
    break
  fi
  (( SECONDS < deadline )) || fail "refresh batch did not complete within ${timeout_seconds}s"
  sleep 1
done

(( $(jq --raw-output '.succeeded' <<<"$batch_status") >= 1 )) || fail "refresh batch did not succeed for any repository"

#request DELETE "/api/repos/$repo_id" 204 >/dev/null
repo_id=""
printf 'smoke test passed for %s/%s\n' "$owner" "$name"
