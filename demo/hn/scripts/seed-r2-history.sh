#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" == "Darwin" ]] && [[ "${STREAMBED_HISTORY_CAFFEINATED:-}" != "1" ]] && command -v caffeinate >/dev/null 2>&1; then
  exec env STREAMBED_HISTORY_CAFFEINATED=1 caffeinate -ims "$0" "$@"
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$DEMO_DIR/../.." && pwd)"
LOCAL_DIR="$DEMO_DIR/.local"
BIN_DIR="$LOCAL_DIR/bin"
LOG_DIR="$LOCAL_DIR/log"
BUCKET="${STREAMBED_DEMO_BUCKET:-streambed-hn-demo}"
PREFIX="${STREAMBED_DEMO_S3_PREFIX:-hn-demo-v2}"
CLOUDFLARE_ACCOUNT_ID="${CLOUDFLARE_ACCOUNT_ID:-75c3ef432a1ebf389a8fdc403a582b1e}"
S3_ENDPOINT="${STREAMBED_DEMO_S3_ENDPOINT:-https://${CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com}"
BACKFILL_SINCE="${HN_BACKFILL_SINCE:-2024-10-01}"
BACKFILL_UNTIL="${HN_BACKFILL_UNTIL:-2026-10-01}"
POSTGRES_PORT="${STREAMBED_HISTORY_POSTGRES_PORT:-55435}"
QUERY_PORT="${STREAMBED_HISTORY_QUERY_PORT:-58084}"
POSTGRES_CONTAINER="streambed-hn-history-postgres"
SLOT_NAME="streambed_hn_history_seed"
STATE_PATH="$LOCAL_DIR/state-r2-history-seed.db"
SNAPSHOTS_FILE="$LOCAL_DIR/r2-front-page-snapshots.tsv"
RESUME="${STREAMBED_HISTORY_RESUME:-0}"
KEEP_FAILED="${STREAMBED_HISTORY_KEEP_FAILED:-1}"
STREAMBED_PID=""
BACKFILL_PID=""
QUERY_PID=""
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""

cleanup() {
  status=$?
  trap - EXIT
  for pid in "$QUERY_PID" "$BACKFILL_PID" "$STREAMBED_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ "$status" -eq 0 || "$KEEP_FAILED" != "1" ]]; then
    docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
  else
    echo "History seed stopped before completion; source and checkpoints were preserved." >&2
    echo "Resume with STREAMBED_HISTORY_RESUME=1 and the same prefix and date range." >&2
  fi
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  exit "$status"
}
trap cleanup EXIT

for command in curl docker go jq psql security; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command not found: $command" >&2
    exit 1
  fi
done

mkdir -p "$BIN_DIR" "$LOG_DIR"
echo "==> Building Streambed and HN importers"
(
  cd "$PROJECT_DIR"
  go build -o "$BIN_DIR/streambed" ./cmd/streambed
  go build -o "$BIN_DIR/hn-ingester" ./demo/hn/cmd/hn-ingester
  go build -o "$BIN_DIR/hn-backfill" ./demo/hn/cmd/hn-backfill
)

AWS_ACCESS_KEY_ID="$(security find-generic-password \
  -a streambed-hn-demo -s streambed-r2-writer-access-key-id -w)"
AWS_SECRET_ACCESS_KEY="$(security find-generic-password \
  -a streambed-hn-demo -s streambed-r2-writer-secret-access-key -w)"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION=auto AWS_EC2_METADATA_DISABLED=true

for table in public.stories public.story_analytics public.story_monthly public.story_leaders public.front_page; do
  if "$BIN_DIR/streambed" snapshots \
      --table="$table" \
      --s3-bucket="$BUCKET" \
      --s3-prefix="$PREFIX" \
      --s3-endpoint="$S3_ENDPOINT" \
      --s3-region=auto >/dev/null 2>&1 && [[ "$RESUME" != "1" ]]; then
    echo "target s3://$BUCKET/$PREFIX already contains $table; refusing to mix seed runs" >&2
    echo "Set STREAMBED_HISTORY_RESUME=1 only to resume this seed's preserved source." >&2
    exit 1
  fi
done

if [[ "$RESUME" == "1" ]]; then
  if ! docker inspect "$POSTGRES_CONTAINER" >/dev/null 2>&1; then
    echo "cannot resume: preserved container $POSTGRES_CONTAINER was not found" >&2
    exit 1
  fi
  if [[ ! -f "$STATE_PATH" ]]; then
    echo "cannot resume: preserved Streambed state $STATE_PATH was not found" >&2
    exit 1
  fi
  echo "==> Resuming isolated history Postgres"
  docker start "$POSTGRES_CONTAINER" >/dev/null
else
  echo "==> Starting isolated history Postgres"
  docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
  docker run -d \
    --name "$POSTGRES_CONTAINER" \
    -p "127.0.0.1:${POSTGRES_PORT}:5432" \
    -e POSTGRES_DB=hn \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=test \
    postgres:16 \
      -c wal_level=logical \
      -c max_replication_slots=2 \
      -c max_wal_senders=2 >/dev/null
fi

SOURCE_URL="postgres://postgres:test@127.0.0.1:${POSTGRES_PORT}/hn?sslmode=disable"
for _ in $(seq 1 60); do
  if psql -w "$SOURCE_URL" -Atc 'SELECT 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! psql -w "$SOURCE_URL" -Atc 'SELECT 1' >/dev/null 2>&1; then
  echo "history Postgres did not become ready" >&2
  exit 1
fi
HN_DATABASE_URL="$SOURCE_URL" "$BIN_DIR/hn-ingester" --migrate-only

echo "==> Starting CDC writer into s3://$BUCKET/$PREFIX"
if [[ "$RESUME" != "1" ]]; then
  rm -f "$STATE_PATH" "$STATE_PATH-shm" "$STATE_PATH-wal"
fi
"$BIN_DIR/streambed" sync \
  --source-url="$SOURCE_URL" \
  --s3-bucket="$BUCKET" \
  --s3-prefix="$PREFIX" \
  --s3-endpoint="$S3_ENDPOINT" \
  --s3-region=auto \
  --state-path="$STATE_PATH" \
  --slot-name="$SLOT_NAME" \
  --include-tables=public.stories,public.story_analytics,public.story_monthly,public.story_leaders,public.rankings,public.front_page \
  --flush-rows=20000 \
  --flush-interval=30s \
  >"$LOG_DIR/r2-history-streambed.log" 2>&1 &
STREAMBED_PID=$!

for _ in $(seq 1 60); do
  if psql -w "$SOURCE_URL" -Atc \
      "SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT_NAME' AND active" | grep -q 1; then
    break
  fi
  if ! kill -0 "$STREAMBED_PID" 2>/dev/null; then
    tail -100 "$LOG_DIR/r2-history-streambed.log" >&2
    exit 1
  fi
  sleep 1
done
if ! psql -w "$SOURCE_URL" -Atc \
    "SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT_NAME' AND active" | grep -q 1; then
  echo "timed out waiting for active history replication slot" >&2
  exit 1
fi

echo "==> Backfilling HN stories from $BACKFILL_SINCE to $BACKFILL_UNTIL"
"$BIN_DIR/hn-backfill" \
  --database-url="$SOURCE_URL" \
  --since="$BACKFILL_SINCE" \
  --until="$BACKFILL_UNTIL" \
  --checkpoint-name=algolia-hn-two-years-v1 \
  >"$LOG_DIR/r2-history-backfill.log" 2>&1 &
BACKFILL_PID=$!
while kill -0 "$BACKFILL_PID" 2>/dev/null; do
  progress="$(psql -w "$SOURCE_URL" -Atc \
    "SELECT next_start::date || ' rows=' || rows_inserted FROM backfill_status WHERE source='algolia-hn-two-years-v1'" 2>/dev/null || true)"
  [[ -n "$progress" ]] && echo "    $progress"
  sleep 15
done
wait "$BACKFILL_PID"
BACKFILL_PID=""

target_lsn="$(psql -w "$SOURCE_URL" -Atc 'SELECT pg_current_wal_lsn()')"
echo "==> Waiting for Streambed to acknowledge historical WAL through $target_lsn"
for _ in $(seq 1 900); do
  caught_up="$(psql -w "$SOURCE_URL" -Atc \
    "SELECT coalesce(pg_wal_lsn_diff(confirmed_flush_lsn, '$target_lsn') >= 0, false) FROM pg_replication_slots WHERE slot_name='$SLOT_NAME'")"
  if [[ "$caught_up" == "t" ]]; then break; fi
  if ! kill -0 "$STREAMBED_PID" 2>/dev/null; then
    tail -100 "$LOG_DIR/r2-history-streambed.log" >&2
    exit 1
  fi
  sleep 1
done
if [[ "${caught_up:-f}" != "t" ]]; then
  echo "timed out waiting for Streambed to consume historical WAL" >&2
  exit 1
fi

echo "==> Capturing current HN state and two front-page snapshots"
HN_DATABASE_URL="$SOURCE_URL" "$BIN_DIR/hn-ingester" --once
snapshot_count=0
for _ in $(seq 1 120); do
  if "$BIN_DIR/streambed" snapshots \
      --table=public.front_page \
      --s3-bucket="$BUCKET" \
      --s3-prefix="$PREFIX" \
      --s3-endpoint="$S3_ENDPOINT" \
      --s3-region=auto >"$SNAPSHOTS_FILE" 2>/dev/null; then
    snapshot_count="$(tail -n +2 "$SNAPSHOTS_FILE" | grep -c . || true)"
    [[ "$snapshot_count" -ge 1 ]] && break
  fi
  sleep 1
done
if [[ "$snapshot_count" -lt 1 ]]; then
  echo "history seed did not produce the initial front_page snapshot" >&2
  exit 1
fi

HN_DATABASE_URL="$SOURCE_URL" "$BIN_DIR/hn-ingester" --once
psql -w "$SOURCE_URL" -v ON_ERROR_STOP=1 -c \
  'UPDATE front_page SET score = score WHERE rank = 1' >/dev/null
for _ in $(seq 1 120); do
  "$BIN_DIR/streambed" snapshots \
    --table=public.front_page \
    --s3-bucket="$BUCKET" \
    --s3-prefix="$PREFIX" \
    --s3-endpoint="$S3_ENDPOINT" \
    --s3-region=auto >"$SNAPSHOTS_FILE" 2>/dev/null || true
  snapshot_count="$(tail -n +2 "$SNAPSHOTS_FILE" | grep -c . || true)"
  [[ "$snapshot_count" -ge 2 ]] && break
  sleep 1
done
if [[ "$snapshot_count" -lt 2 ]]; then
  echo "history seed did not produce two front_page snapshots" >&2
  exit 1
fi

final_lsn="$(psql -w "$SOURCE_URL" -Atc 'SELECT pg_current_wal_lsn()')"
for _ in $(seq 1 120); do
  caught_up="$(psql -w "$SOURCE_URL" -Atc \
    "SELECT coalesce(pg_wal_lsn_diff(confirmed_flush_lsn, '$final_lsn') >= 0, false) FROM pg_replication_slots WHERE slot_name='$SLOT_NAME'")"
  [[ "$caught_up" == "t" ]] && break
  sleep 1
done
if [[ "${caught_up:-f}" != "t" ]]; then
  echo "timed out waiting for final HN state to reach R2" >&2
  exit 1
fi

kill -INT "$STREAMBED_PID"
wait "$STREAMBED_PID"
STREAMBED_PID=""

echo "==> Compacting historical files for bounded public queries"
for table in public.stories public.story_analytics public.story_monthly public.story_leaders; do
"$BIN_DIR/streambed" maintenance compact \
  --table="$table" \
  --s3-bucket="$BUCKET" \
  --s3-prefix="$PREFIX" \
  --s3-endpoint="$S3_ENDPOINT" \
  --s3-region=auto \
  --state-path="$STATE_PATH" \
  --target-file-size-mb=128 \
  --small-file-threshold-mb=32 \
  --max-input-files=1000 \
  --dry-run=false \
  --force
done

echo "==> Verifying analytical history through the HTTP query server"
"$BIN_DIR/streambed" query \
  --s3-bucket="$BUCKET" \
  --s3-prefix="$PREFIX" \
  --s3-endpoint="$S3_ENDPOINT" \
  --s3-region=auto \
  --http-listen-addr=":${QUERY_PORT}" \
  --query-memory-limit-mb=128 \
  >"$LOG_DIR/r2-history-query.log" 2>&1 &
QUERY_PID=$!
for _ in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:${QUERY_PORT}/health" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$QUERY_PID" 2>/dev/null; then
    tail -100 "$LOG_DIR/r2-history-query.log" >&2
    exit 1
  fi
  sleep 1
done
QUERY_URL="http://127.0.0.1:${QUERY_PORT}/query"
query() {
  jq -nc --arg sql "$1" '{sql:$sql}' |
    curl -fsS -H 'Content-Type: application/json' --data-binary @- "$QUERY_URL"
}

coverage="$(query "SELECT count(*) AS stories, min(created_at) AS coverage_start, max(created_at) AS coverage_end FROM stories")"
story_count="$(jq -r '.rows[0][0]' <<<"$coverage")"
postgres_mentions="$(query "SELECT sum(mentions_postgresql) FROM story_monthly WHERE month >= DATE '$BACKFILL_SINCE' AND month < DATE '$BACKFILL_UNTIL'" | jq -r '.rows[0][0]')"
mysql_mentions="$(query "SELECT sum(mentions_mysql) FROM story_monthly WHERE month >= DATE '$BACKFILL_SINCE' AND month < DATE '$BACKFILL_UNTIL'" | jq -r '.rows[0][0]')"
historical_timestamp="$(tail -n 1 "$SNAPSHOTS_FILE" | cut -f2)"
historical_count="$(query "SELECT count(*) FROM front_page AS f AT (TIMESTAMP => TIMESTAMPTZ '$historical_timestamp')" | jq -r '.rows[0][0]')"

if [[ "$story_count" -lt 100000 || "$postgres_mentions" -lt 1 || "$mysql_mentions" -lt 1 || "$historical_count" != 30 ]]; then
  echo "history verification failed: stories=$story_count postgres=$postgres_mentions mysql=$mysql_mentions historical_front_page=$historical_count" >&2
  exit 1
fi

echo "Seeded and verified s3://$BUCKET/$PREFIX"
echo "  stories=$story_count postgres_mentions=$postgres_mentions mysql_mentions=$mysql_mentions front_page_snapshots=$snapshot_count"
