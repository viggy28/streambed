#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$DEMO_DIR/../.." && pwd)"
COMPOSE_FILE="$DEMO_DIR/docker-compose.yml"
LOCAL_DIR="$DEMO_DIR/.local"
BIN_DIR="$LOCAL_DIR/bin"
RUN_DIR="$LOCAL_DIR/run"
LOG_DIR="$LOCAL_DIR/log"
SUPABASE_PROJECT_REF="${SUPABASE_PROJECT_REF:-tvnljlooazyxocdtovvy}"
SUPABASE_HOST="${SUPABASE_DB_HOST:-db.${SUPABASE_PROJECT_REF}.supabase.co}"
SUPABASE_KEYCHAIN_ACCOUNT="${SUPABASE_KEYCHAIN_ACCOUNT:-streambed-hn-demo}"
SUPABASE_KEYCHAIN_SERVICE="${SUPABASE_KEYCHAIN_SERVICE:-streambed-supabase-db-password}"
STREAMBED_PID=""
INGESTER_PID=""
STARTUP_COMPLETE=false
DB_PASSWORD=""
ENCODED_PASSWORD=""
DB_URL=""

cleanup_on_exit() {
  status=$?
  trap - EXIT
  if [[ "$STARTUP_COMPLETE" != true ]]; then
    [[ -z "$INGESTER_PID" ]] || kill "$INGESTER_PID" 2>/dev/null || true
    [[ -z "$STREAMBED_PID" ]] || kill "$STREAMBED_PID" 2>/dev/null || true
  fi
  unset DB_PASSWORD ENCODED_PASSWORD DB_URL
  exit "$status"
}
trap cleanup_on_exit EXIT

mkdir -p "$BIN_DIR" "$RUN_DIR" "$LOG_DIR"

for command in docker go python3 security; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command not found: $command" >&2
    exit 1
  fi
done

for process in streambed hn-ingester; do
  pid_file="$RUN_DIR/$process.pid"
  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "$process is already running with PID $(cat "$pid_file")" >&2
    exit 1
  fi
  rm -f "$pid_file"
done

printf '%s\n' 'hn-demo-supabase' >"$LOCAL_DIR/s3-prefix"

DB_PASSWORD="$(security find-generic-password \
  -a "$SUPABASE_KEYCHAIN_ACCOUNT" \
  -s "$SUPABASE_KEYCHAIN_SERVICE" \
  -w)"
ENCODED_PASSWORD="$(DB_PASSWORD="$DB_PASSWORD" python3 -c \
  'import os, urllib.parse; print(urllib.parse.quote(os.environ["DB_PASSWORD"], safe=""))')"
DB_URL="postgres://postgres:${ENCODED_PASSWORD}@${SUPABASE_HOST}:5432/postgres?sslmode=require"

echo "==> Starting local MinIO"
docker compose -f "$COMPOSE_FILE" up -d minio --wait
docker compose -f "$COMPOSE_FILE" up createbucket

echo "==> Building Streambed and the HN ingester"
(
  cd "$PROJECT_DIR"
  go build -o "$BIN_DIR/streambed" ./cmd/streambed
  go build -o "$BIN_DIR/hn-ingester" ./demo/hn/cmd/hn-ingester
)

echo "==> Verifying the Supabase migration, RLS, and publication"
HN_DATABASE_URL="$DB_URL" "$BIN_DIR/hn-ingester" --verify-supabase-schema

echo "==> Starting Streambed against Supabase"
env \
  STREAMBED_SOURCE_URL="$DB_URL" \
  AWS_ACCESS_KEY_ID=minioadmin \
  AWS_SECRET_ACCESS_KEY=minioadmin \
  AWS_REGION=us-east-1 \
  AWS_EC2_METADATA_DISABLED=true \
  nohup "$BIN_DIR/streambed" sync \
    --s3-bucket=streambed \
    --s3-endpoint='http://localhost:59000' \
    --s3-prefix=hn-demo-supabase \
    --state-path="$LOCAL_DIR/state-supabase.db" \
    --slot-name=streambed_hn_demo \
    --include-tables=public.stories,public.story_analytics,public.rankings,public.front_page \
    --flush-rows=100 \
    --flush-interval=2s \
    --query-addr=:55433 \
    >"$LOG_DIR/streambed.log" 2>&1 &
STREAMBED_PID=$!
echo "$STREAMBED_PID" >"$RUN_DIR/streambed.pid"

for _ in $(seq 1 45); do
  if grep -q 'replication started' "$LOG_DIR/streambed.log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$(cat "$RUN_DIR/streambed.pid")" 2>/dev/null; then
    echo "Streambed exited during startup:" >&2
    tail -100 "$LOG_DIR/streambed.log" >&2
    exit 1
  fi
  sleep 1
done
if ! grep -q 'replication started' "$LOG_DIR/streambed.log" 2>/dev/null; then
  echo "timed out waiting for Streambed replication" >&2
  exit 1
fi

echo "==> Starting the HN ingester against Supabase"
env HN_DATABASE_URL="$DB_URL" \
  nohup "$BIN_DIR/hn-ingester" >"$LOG_DIR/hn-ingester.log" 2>&1 &
INGESTER_PID=$!
echo "$INGESTER_PID" >"$RUN_DIR/hn-ingester.pid"

STARTUP_COMPLETE=true
unset DB_PASSWORD ENCODED_PASSWORD DB_URL

cat <<EOF

Supabase-backed HN demo is starting.

  Supabase source:  $SUPABASE_HOST
  Streambed query:  postgres://demo@localhost:55433/hn?sslmode=disable
  MinIO console:    http://localhost:59001

Follow progress with:
  tail -f $LOG_DIR/hn-ingester.log $LOG_DIR/streambed.log

Then run:
  $SCRIPT_DIR/smoke-test.sh
EOF
