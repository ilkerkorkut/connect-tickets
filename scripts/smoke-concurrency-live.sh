#!/usr/bin/env bash
set -euo pipefail

api_url="${API_URL:-http://localhost:8080}"
owner="${GITHUB_OWNER:-octocat}"
name="${GITHUB_NAME:-hello-world}"
concurrency="${CONCURRENCY:-5}"
refresh_concurrency="${REFRESH_CONCURRENCY:-5}"
poll_timeout="${POLL_TIMEOUT_SECONDS:-60}"
compose_project="${COMPOSE_PROJECT:-github-tracker}"
work_dir="$(mktemp -d)"

normalized="$(printf '%s/%s' "$owner" "$name" | tr '[:upper:]' '[:lower:]')"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"

invariant_failures=0
invariant_checks=0
notes_before_refresh=""
notes_after_refresh=""
sequence_before_refresh=""
sequence_after_refresh=""
created_repo_count=0
refreshed_change_count=0

cleanup() {
  rm -rf "$work_dir"
}
trap cleanup EXIT

fail() {
  printf 'smoke-concurrency-live failed: %s\n' "$1" >&2
  exit 1
}

check() {
  local label="$1"
  local condition="$2"
  local detail="${3:-}"
  invariant_checks=$((invariant_checks + 1))
  if [[ "$condition" == "1" ]]; then
    printf '[PASS] %s' "$label"
    [[ -z "$detail" ]] || printf ' : %s' "$detail"
    printf '\n'
  else
    invariant_failures=$((invariant_failures + 1))
    printf '[FAIL] %s' "$label"
    [[ -z "$detail" ]] || printf ' : %s' "$detail"
    printf '\n'
  fi
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

compose_exec() {
  local service="$1"
  shift
  docker compose --project-name "$compose_project" exec -T "$service" "$@"
}

psql_query() {
  compose_exec postgres psql -U tracker -d tracker -A -F $'\t' -c "$1"
}

psql_scalar() {
  compose_exec postgres psql -U tracker -d tracker -At -F $'\t' -c "$1" | head -n1 | tr -d $'\r'
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
    cat "$response_file" >&2 || true
    exit 1
  fi
  cat "$response_file"
}

wait_for_health() {
  local deadline=$((SECONDS + poll_timeout))
  until curl --fail --silent --output /dev/null "$api_url/healthz"; do
    (( SECONDS < deadline )) || fail "API did not become healthy within ${poll_timeout}s"
    sleep 1
  done
}

reset_identity() {
  local before="$work_dir/repos-before.json"
  local cursor=""
  local id
  while true; do
    local path="/api/repos?limit=100"
    [[ -z "$cursor" ]] || path="${path}&cursor=${cursor}"
    curl --silent --show-error --output "$before" "$api_url$path" || fail "list repositories failed during reset"
    while IFS=$'\t' read -r rid rfull; do
      if [[ "$(printf '%s' "$rfull" | tr '[:upper:]' '[:lower:]')" == "$normalized" ]]; then
        printf 'reset: deleting existing tracked repository %s\n' "$rid"
        request DELETE "/api/repos/$rid" 204 >/dev/null
      fi
    done < <(jq --raw-output '.repos[] | [.id, .full_name] | @tsv' "$before")
    if [[ "$(jq --raw-output '.has_more' "$before")" == "true" ]]; then
      cursor="$(jq --raw-output '.next_cursor' "$before")"
      [[ -n "$cursor" && "$cursor" != "null" ]] || break
    else
      break
    fi
  done
  printf 'reset: flushing redis cache keys for %s\n' "$normalized"
  compose_exec redis redis-cli DEL "github:repo:v1:$normalized" "github:repo-lock:v1:$normalized" >/dev/null 2>&1 || true
}

run_concurrent_burst() {
  printf 'concurrency burst: %d parallel POST /api/repos for %s/%s\n' "$concurrency" "$owner" "$name"
  local start_fifo="$work_dir/start"
  mkfifo "$start_fifo"
  exec 3<>"$start_fifo"
  local pids=()
  for i in $(seq 1 "$concurrency"); do
    curl --silent --show-error \
      --output "$work_dir/burst-$i.json" \
      --write-out '%{http_code}' \
      --request POST "$api_url/api/repos" \
      --header 'Content-Type: application/json' \
      --data "{\"owner\":\"$owner\",\"name\":\"$name\"}" \
      >"$work_dir/status-$i" 2>/dev/null &
    pids+=($!)
  done
  printf 'x\n' >&3
  exec 3>&-
  for pid in "${pids[@]}"; do
    wait "$pid" || true
  done
  local created=0
  local conflicts=0
  local unexpected=0
  repo_id=""
  local i status code
  for i in $(seq 1 "$concurrency"); do
    status="$(cat "$work_dir/status-$i")"
    case "$status" in
      201)
        created=$((created + 1))
        if [[ -z "$repo_id" ]]; then
          repo_id="$(jq --raw-output '.id' "$work_dir/burst-$i.json")"
        fi
        ;;
      409)
        conflicts=$((conflicts + 1))
        code="$(jq --raw-output '.error.code' "$work_dir/burst-$i.json")"
        [[ "$code" == "repo_already_tracked" ]] || fail "409 response returned wrong error code: $code"
        ;;
      *)
        unexpected=$((unexpected + 1))
        printf 'unexpected response #%d: status=%s body=%s\n' "$i" "$status" "$(cat "$work_dir/burst-$i.json")" >&2
        ;;
    esac
  done
  printf 'concurrency burst result: created=%d conflicts=%d unexpected=%d (expected 1/%d/0)\n' "$created" "$conflicts" "$unexpected" $((concurrency - 1))
  [[ "$created" -eq 1 ]] || fail "expected exactly 1 created response, got $created"
  [[ "$conflicts" -eq $((concurrency - 1)) ]] || fail "expected $((concurrency - 1)) conflict responses, got $conflicts"
  [[ "$unexpected" -eq 0 ]] || fail "expected 0 unexpected responses, got $unexpected"
  [[ -n "$repo_id" && "$repo_id" != "null" ]] || fail "created repository id missing from 201 response"
}

update_notes() {
  local notes="live-concurrency $stamp"
  local updated
  updated="$(request PATCH "/api/repos/$repo_id" 200 "{\"notes\":\"$notes\"}")"
  [[ "$(jq --raw-output '.notes' <<<"$updated")" == "$notes" ]] || fail "notes were not persisted"
  notes_before_refresh="$notes"
  sequence_before_refresh="$(psql_scalar "SELECT COALESCE(MAX(sequence),0) FROM repository_changes WHERE repo_id = '$repo_id';")"
}

synchronous_refresh() {
  local refreshed full_name
  refreshed="$(request POST "/api/repos/$repo_id/refresh" 200)"
  full_name="$(jq --raw-output '.full_name' <<<"$refreshed")"
  [[ "$(printf '%s' "$full_name" | tr '[:upper:]' '[:lower:]')" == "$normalized" ]] || fail "synchronous refresh returned wrong repository: $full_name"
}

concurrent_refresh_same_repo() {
  printf 'concurrent refresh: %d parallel POST /api/repos/%s/refresh on the same repo\n' "$refresh_concurrency" "$repo_id"
  local start_fifo="$work_dir/refresh-start"
  mkfifo "$start_fifo"
  exec 4<>"$start_fifo"
  local pids=()
  local i
  for i in $(seq 1 "$refresh_concurrency"); do
    curl --silent --show-error \
      --output "$work_dir/rburst-$i.json" \
      --write-out '%{http_code}' \
      --request POST "$api_url/api/repos/$repo_id/refresh" \
      >"$work_dir/rstatus-$i" 2>/dev/null &
    pids+=($!)
  done
  printf 'x\n' >&4
  exec 4>&-
  for pid in "${pids[@]}"; do
    wait "$pid" || true
  done
  local ok=0
  local unexpected=0
  for i in $(seq 1 "$refresh_concurrency"); do
    local status
    status="$(cat "$work_dir/rstatus-$i")"
    case "$status" in
      200) ok=$((ok + 1)) ;;
      *) unexpected=$((unexpected + 1)); printf 'unexpected refresh response #%d: status=%s body=%s\n' "$i" "$status" "$(cat "$work_dir/rburst-$i.json")" >&2 ;;
    esac
  done
  printf 'concurrent refresh result: ok=%d unexpected=%d (expected %d/0)\n' "$ok" "$unexpected" "$refresh_concurrency"
  [[ "$unexpected" -eq 0 ]] || fail "expected 0 unexpected refresh responses, got $unexpected"
  local current
  current="$(psql_scalar "SELECT COALESCE(MAX(sequence),0) FROM repository_changes WHERE repo_id = '$repo_id';")"
  refreshed_change_count=$((current - sequence_before_refresh))
  sequence_after_refresh="$current"
  printf 'refresh-driven change rows added: %d (sync + %d concurrent refreshes)\n' "$refreshed_change_count" "$refresh_concurrency"
}

batch_refresh_all() {
  local batch
  batch="$(request POST /api/repos/refresh-all 202)"
  batch_id="$(jq --raw-output '.batch_id' <<<"$batch")"
  [[ -n "$batch_id" && "$batch_id" != "null" ]] || fail "refresh-all response did not include a batch id"
  local deadline=$((SECONDS + poll_timeout))
  local status_body
  while true; do
    status_body="$(request GET "/api/batches/$batch_id" 200)"
    if [[ "$(jq --raw-output '.complete' <<<"$status_body")" == "true" ]]; then
      break
    fi
    (( SECONDS < deadline )) || fail "refresh batch did not complete within ${poll_timeout}s"
    sleep 1
  done
  (( $(jq --raw-output '.succeeded' <<<"$status_body") >= 1 )) || fail "refresh batch did not succeed for any repository"
}

print_db_summary() {
  printf '\n==================== database summary ====================\n'
  printf 'identity: %s/%s (normalized=%s) repo_id=%s\n\n' "$owner" "$name" "$normalized" "$repo_id"

  printf -- '---- repositories ----\n'
  psql_query "SELECT id, owner, name, stars, notes, fetched_at, deleted_at FROM repositories WHERE normalized_full_name = '$normalized' ORDER BY created_at;" || true

  printf -- '\n---- repository_changes (commit-ordered) ----\n'
  psql_query "SELECT sequence, operation, repo_id, changed_at FROM repository_changes WHERE repo_id = '$repo_id' ORDER BY sequence;" || true

  printf -- '\n---- batches / batch_jobs ----\n'
  psql_query "SELECT b.id, b.created_at, b.completed_at, bj.status, bj.attempt_count, bj.last_error_code FROM batches b JOIN batch_jobs bj ON bj.batch_id = b.id WHERE bj.repo_id = '$repo_id' ORDER BY b.created_at DESC;" || true

  printf -- '\n---- job_attempts ----\n'
  psql_query "SELECT ja.attempt_number, ja.worker_id, ja.result, ja.error_code, ja.duration_ms FROM job_attempts ja JOIN batch_jobs bj ON bj.id = ja.batch_job_id WHERE bj.repo_id = '$repo_id' ORDER BY ja.attempt_number;" || true

  printf -- '\n---- outbox_events ----\n'
  psql_query "SELECT event_type, status, aggregate_id, partition_key, publish_attempts, published_at FROM outbox_events WHERE partition_key = '$repo_id' ORDER BY created_at;" || true

  printf -- '\n---- counts ----\n'
  psql_query "SELECT (SELECT COUNT(*) FROM repositories WHERE deleted_at IS NULL) AS active_repos, (SELECT COUNT(*) FROM repository_changes WHERE repo_id = '$repo_id') AS changes_for_identity;" || true
  printf '\n==========================================================\n'
}

walk_change_feed() {
  printf '\n==================== change feed walk ====================\n'
  printf 'Walking GET /api/repos/changes via next_cursor from the beginning.\n'
  printf 'Showing the last 10 changes (commit-ordered by sequence); tombstones remain in the feed.\n\n'
  local cursor=""
  local all="$work_dir/feed-all.json"
  : >"$all"
  local pages=0
  local total=0
  while true; do
    local path="/api/repos/changes?limit=100"
    [[ -z "$cursor" ]] || path="${path}&since=${cursor}"
    local body
    body="$(curl --silent --show-error "$api_url$path")" || fail "change feed request failed"
    local count
    count="$(jq '.changes | length' <<<"$body")"
    total=$((total + count))
    jq --raw-output '.changes[] | [.event_id, .operation, (.repo.full_name // "-")] | @tsv' <<<"$body" >>"$all"
    pages=$((pages + 1))
    if [[ "$(jq --raw-output '.has_more' <<<"$body")" == "true" ]]; then
      cursor="$(jq --raw-output '.next_cursor' <<<"$body")"
      [[ -n "$cursor" && "$cursor" != "null" ]] || break
    else
      break
    fi
    (( pages < 100 )) || fail "change feed pagination exceeded 100 pages"
  done
  printf 'feed pages walked: %d | total change rows in feed: %d\n\n' "$pages" "$total"
  printf -- 'seq-range  operation       full_name\n'
  printf -- '---------- --------------- ------------------------------------\n'
  tail -n 10 "$all" | awk -F'\t' '{
    shortid=substr($1,1,8)
    printf "%-10s %-15s %s\n", shortid, $2, $3
  }'
  local tombstones
  tombstones="$(grep -c $'\tdeleted\t' "$all" 2>/dev/null || printf 0)"
  printf '\ntombstone (deleted) rows retained in feed: %s\n' "$tombstones"
  printf '\n==========================================================\n'
}

capture_repo_state() {
  notes_after_refresh="$(psql_scalar "SELECT notes FROM repositories WHERE id = '$repo_id';")"
  created_repo_count="$(psql_scalar "SELECT COUNT(*) FROM repositories WHERE normalized_full_name = '$normalized' AND deleted_at IS NULL;")"
}

print_invariants() {
  printf '\n==================== invariant checks ====================\n'

  local expected_changes
  expected_changes=$((2 + refresh_concurrency))

  local outbox_count outbox_published outbox_key_match outbox_attempts
  outbox_count="$(psql_scalar "SELECT COUNT(*) FROM outbox_events WHERE partition_key = '$repo_id';")"
  outbox_published="$(psql_scalar "SELECT COUNT(*) FROM outbox_events WHERE partition_key = '$repo_id' AND status = 'published';")"
  outbox_key_match="$(psql_scalar "SELECT COUNT(*) FROM outbox_events WHERE partition_key = '$repo_id' AND partition_key = '$repo_id';")"
  outbox_attempts="$(psql_scalar "SELECT COALESCE(MAX(publish_attempts),0) FROM outbox_events WHERE partition_key = '$repo_id';")"

  local batch_status batch_attempts
  batch_status="$(psql_scalar "SELECT bj.status FROM batches b JOIN batch_jobs bj ON bj.batch_id = b.id WHERE bj.repo_id = '$repo_id' ORDER BY b.created_at DESC LIMIT 1;")"
  batch_attempts="$(psql_scalar "SELECT COALESCE(MAX(bj.attempt_count),0) FROM batches b JOIN batch_jobs bj ON bj.batch_id = b.id WHERE bj.repo_id = '$repo_id';")"

  local sequence_contiguous sequence_order
  sequence_contiguous="$(psql_scalar "SELECT CASE WHEN COUNT(*) = 0 THEN 1 WHEN (MAX(sequence) - MIN(sequence) + 1) = COUNT(*) THEN 1 ELSE 0 END FROM repository_changes WHERE repo_id = '$repo_id';")"
  sequence_order="$(psql_scalar "SELECT CASE WHEN EXISTS (SELECT 1 FROM (SELECT sequence, LAG(sequence) OVER (ORDER BY sequence) AS prev FROM repository_changes WHERE repo_id = '$repo_id') t WHERE prev IS NOT NULL AND sequence <= prev) THEN 0 ELSE 1 END;")"

  local expected_ops
  expected_ops="created notes_updated"
  local i
  for ((i = 0; i < refresh_concurrency + 2; i++)); do expected_ops="$expected_ops refreshed"; done
  local ops_match
  local actual_ops
  actual_ops="$(psql_scalar "SELECT string_agg(operation, ' ' ORDER BY sequence) FROM repository_changes WHERE repo_id = '$repo_id';")"
  [[ "$actual_ops" == "$expected_ops" ]] && ops_match=1 || ops_match=0

  local notes_preserved=0
  [[ "$notes_after_refresh" == "$notes_before_refresh" ]] && notes_preserved=1

  check "concurrent-create uniqueness (1 active row)" "$([[ "$created_repo_count" -eq 1 ]] && echo 1 || echo 0)" "active rows = $created_repo_count"
  check "change feed commit-ordered (contiguous sequences)" "$sequence_contiguous" "min..max spans exactly the row count"
  check "change feed monotonic (sequences strictly increase)" "$sequence_order" "no out-of-order sequence"
  check "change operations in expected order" "$ops_match" "got: $actual_ops"
  check "notes preserved across refreshes" "$notes_preserved" "\"$notes_after_refresh\""
  check "outbox published exactly once" "$([[ "$outbox_count" -ge 1 && "$outbox_count" == "$outbox_published" ]] && echo 1 || echo 0)" "events=$outbox_count published=$outbox_published"
  check "outbox message key = repo_id" "$([[ "$outbox_key_match" == "$outbox_count" && "$outbox_count" -ge 1 ]] && echo 1 || echo 0)" "partition_key matches repo_id"
  check "outbox single publish attempt" "$([[ "$outbox_attempts" -le 1 ]] && echo 1 || echo 0)" "max publish_attempts = $outbox_attempts"
  check "batch durable claim succeeded" "$([[ "$batch_status" == "succeeded" ]] && echo 1 || echo 0)" "status=$batch_status attempts=$batch_attempts"
  check "refresh coalescing added change rows" "$([[ "$refreshed_change_count" -ge "$refresh_concurrency" ]] && echo 1 || echo 0)" "added $refreshed_change_count refreshed rows"

  printf '\n%d/%d invariants held' "$((invariant_checks - invariant_failures))" "$invariant_checks"
  if [[ "$invariant_failures" -gt 0 ]]; then
    printf ' (%d FAILED)\n' "$invariant_failures"
    printf '\n==========================================================\n'
    fail "$invariant_failures invariant(s) failed"
  fi
  printf '\n'
  printf '\n==========================================================\n'
}

require_command curl
require_command jq
require_command docker
wait_for_health

reset_identity
run_concurrent_burst
update_notes
synchronous_refresh
concurrent_refresh_same_repo
batch_refresh_all
capture_repo_state
print_db_summary
walk_change_feed
print_invariants

printf '\nsmoke-concurrency-live passed for %s/%s\n' "$owner" "$name"
