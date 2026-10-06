#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
QUERY_URL="${STREAMBED_DEMO_QUERY_URL:-http://127.0.0.1:58080/query}"
SNAPSHOTS_FILE="${STREAMBED_DEMO_SNAPSHOTS_FILE:-$DEMO_DIR/.local/r2-front-page-snapshots.tsv}"

for command in curl jq; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command not found: $command" >&2
    exit 1
  fi
done

query() {
  local sql="$1"
  jq -nc --arg sql "$sql" '{sql:$sql}' |
    curl -fsS -H 'Content-Type: application/json' --data-binary @- "$QUERY_URL"
}

current_count="$(query 'SELECT count(*) FROM front_page' | jq -r '.rows[0][0]')"
if [[ "$current_count" != 30 ]]; then
  echo "current front_page has $current_count rows, want 30" >&2
  exit 1
fi

echo "==> Current front page"
query 'SELECT rank, title, score, comment_count FROM front_page ORDER BY rank LIMIT 10' | jq .

if [[ -f "$SNAPSHOTS_FILE" ]]; then
  historical_timestamp="$(tail -n 1 "$SNAPSHOTS_FILE" | cut -f2)"
  historical_count="$(query \
    "SELECT count(*) FROM front_page AS f AT (TIMESTAMP => TIMESTAMPTZ '$historical_timestamp')" |
    jq -r '.rows[0][0]')"
  if [[ "$historical_count" != 30 ]]; then
    echo "historical front_page has $historical_count rows, want 30" >&2
    exit 1
  fi
  echo "==> Historical front page at $historical_timestamp"
  query \
    "SELECT rank, title, score, comment_count FROM front_page AS f AT (TIMESTAMP => TIMESTAMPTZ '$historical_timestamp') ORDER BY rank LIMIT 10" |
    jq .
fi

assert_blocked() {
  local name="$1"
  local sql="$2"
  local output_file status
  output_file="$(mktemp)"
  status="$(jq -nc --arg sql "$sql" '{sql:$sql}' |
    curl -sS -o "$output_file" -w '%{http_code}' \
      -H 'Content-Type: application/json' --data-binary @- "$QUERY_URL")"
  if [[ "$status" != 400 ]]; then
    echo "$name returned HTTP $status, want 400: $(cat "$output_file")" >&2
    rm -f "$output_file"
    exit 1
  fi
  rm -f "$output_file"
}

assert_blocked write 'DELETE FROM front_page'
assert_blocked filesystem "SELECT * FROM read_text('/etc/passwd')"
assert_blocked row-limit 'SELECT * FROM range(1001)'

echo "HN HTTP query smoke test passed"
