#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCAL_DIR="$DEMO_DIR/.local"
STREAMBED="$LOCAL_DIR/bin/streambed"
QUERY_URL='postgres://demo@127.0.0.1:55433/hn?sslmode=disable'
SNAPSHOTS_FILE="$LOCAL_DIR/front-page-snapshots.tsv"
INGESTER_LOG="$LOCAL_DIR/log/hn-ingester.log"
S3_PREFIX="${STREAMBED_DEMO_S3_PREFIX:-$(cat "$LOCAL_DIR/s3-prefix" 2>/dev/null || printf 'hn-demo')}"

for command in psql; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command not found: $command" >&2
    exit 1
  fi
done
if [[ ! -x "$STREAMBED" ]]; then
  echo "Streambed demo binary not found; run $SCRIPT_DIR/start-local.sh first" >&2
  exit 1
fi

snapshot_list() {
  env \
    AWS_ACCESS_KEY_ID=minioadmin \
    AWS_SECRET_ACCESS_KEY=minioadmin \
    AWS_REGION=us-east-1 \
    AWS_EC2_METADATA_DISABLED=true \
    "$STREAMBED" snapshots \
      --table=public.front_page \
      --s3-bucket=streambed \
      --s3-endpoint='http://localhost:59000' \
      --s3-prefix="$S3_PREFIX"
}

initial_poll_count="$(grep -c 'HN poll completed' "$INGESTER_LOG" 2>/dev/null || true)"
snapshot_list >"$SNAPSHOTS_FILE" 2>/dev/null || true
initial_snapshot_id="$(awk 'NR == 2 { print $1 }' "$SNAPSHOTS_FILE")"

echo "==> Waiting for a fresh HN ingestion poll"
for _ in $(seq 1 240); do
  poll_count="$(grep -c 'HN poll completed' "$INGESTER_LOG" 2>/dev/null || true)"
  [[ "$poll_count" -gt "$initial_poll_count" ]] && break
  sleep 1
done
if [[ "${poll_count:-0}" -le "$initial_poll_count" ]]; then
  echo "HN ingester did not complete a fresh poll" >&2
  exit 1
fi

echo "==> Waiting for the current front page"
for _ in $(seq 1 180); do
  count="$(psql -w "$QUERY_URL" -v ON_ERROR_STOP=1 -Atc \
    'SELECT count(*) FROM front_page' 2>/dev/null || true)"
  [[ "$count" == "30" ]] && break
  sleep 1
done
if [[ "${count:-}" != "30" ]]; then
  echo "front_page did not reach 30 rows" >&2
  exit 1
fi

psql -w "$QUERY_URL" -v ON_ERROR_STOP=1 -c \
  'SELECT rank, title, score, comment_count FROM front_page ORDER BY rank LIMIT 10'

echo "==> Waiting for a new snapshot and retained history"
for _ in $(seq 1 120); do
  if snapshot_list >"$SNAPSHOTS_FILE" 2>/dev/null; then
    snapshot_count="$(tail -n +2 "$SNAPSHOTS_FILE" | grep -c . || true)"
    current_snapshot_id="$(awk 'NR == 2 { print $1 }' "$SNAPSHOTS_FILE")"
    if [[ "$snapshot_count" -ge 2 && -n "$current_snapshot_id" && "$current_snapshot_id" != "$initial_snapshot_id" ]]; then
      break
    fi
  fi
  sleep 1
done
if [[ "${snapshot_count:-0}" -lt 2 || -z "${current_snapshot_id:-}" || "$current_snapshot_id" == "$initial_snapshot_id" ]]; then
  echo "front_page did not produce a fresh snapshot with retained history" >&2
  exit 1
fi
cat "$SNAPSHOTS_FILE"

historical_timestamp="$(tail -n 1 "$SNAPSHOTS_FILE" | cut -f2)"
echo "==> Querying the earlier snapshot at $historical_timestamp"
historical_count="$(psql -w "$QUERY_URL" -v ON_ERROR_STOP=1 -Atc \
  "SELECT count(*) FROM front_page AS f AT (TIMESTAMP => TIMESTAMPTZ '$historical_timestamp')")"
if [[ "$historical_count" != "30" ]]; then
  echo "historical front_page has $historical_count rows, want 30" >&2
  exit 1
fi
psql -w "$QUERY_URL" -v ON_ERROR_STOP=1 -c \
  "SELECT rank, title, score, comment_count FROM front_page AS f AT (TIMESTAMP => TIMESTAMPTZ '$historical_timestamp') ORDER BY rank LIMIT 10"

echo "HN demo smoke test passed"
