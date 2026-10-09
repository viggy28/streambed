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
STREAMBED_PID=""
INGESTER_PID=""
STARTUP_COMPLETE=false

cleanup_on_exit() {
  status=$?
  trap - EXIT
  if [[ "$STARTUP_COMPLETE" != true ]]; then
    [[ -z "$INGESTER_PID" ]] || kill "$INGESTER_PID" 2>/dev/null || true
    [[ -z "$STREAMBED_PID" ]] || kill "$STREAMBED_PID" 2>/dev/null || true
  fi
  exit "$status"
}
trap cleanup_on_exit EXIT

mkdir -p "$BIN_DIR" "$RUN_DIR" "$LOG_DIR"

for command in docker go; do
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

printf '%s\n' 'hn-demo' >"$LOCAL_DIR/s3-prefix"

echo "==> Starting Postgres and MinIO"
docker compose -f "$COMPOSE_FILE" up -d postgres minio --wait
docker compose -f "$COMPOSE_FILE" up createbucket

echo "==> Building Streambed and the HN ingester"
(
  cd "$PROJECT_DIR"
  go build -o "$BIN_DIR/streambed" ./cmd/streambed
  go build -o "$BIN_DIR/hn-ingester" ./demo/hn/cmd/hn-ingester
)

echo "==> Ensuring the source schema exists"
"$BIN_DIR/hn-ingester" --migrate-only

echo "==> Starting Streambed"
env \
  AWS_ACCESS_KEY_ID=minioadmin \
  AWS_SECRET_ACCESS_KEY=minioadmin \
  AWS_REGION=us-east-1 \
  AWS_EC2_METADATA_DISABLED=true \
  nohup "$BIN_DIR/streambed" sync \
    --source-url='postgres://postgres:test@localhost:55432/hn?sslmode=disable' \
    --s3-bucket=streambed \
    --s3-endpoint='http://localhost:59000' \
    --s3-prefix=hn-demo \
    --state-path="$LOCAL_DIR/state.db" \
    --slot-name=streambed_hn_demo \
    --include-tables=public.stories,public.story_analytics,public.rankings,public.front_page \
    --flush-rows=100 \
    --flush-interval=2s \
    --query-addr=:55433 \
    >"$LOG_DIR/streambed.log" 2>&1 &
STREAMBED_PID=$!
echo "$STREAMBED_PID" >"$RUN_DIR/streambed.pid"

for _ in $(seq 1 30); do
  if docker compose -f "$COMPOSE_FILE" exec -T postgres \
      psql -U postgres -d hn -tAc \
      "SELECT 1 FROM pg_replication_slots WHERE slot_name='streambed_hn_demo'" \
      | grep -q 1; then
    break
  fi
  if ! kill -0 "$(cat "$RUN_DIR/streambed.pid")" 2>/dev/null; then
    echo "Streambed exited during startup:" >&2
    tail -100 "$LOG_DIR/streambed.log" >&2
    exit 1
  fi
  sleep 1
done
if ! docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U postgres -d hn -tAc \
    "SELECT 1 FROM pg_replication_slots WHERE slot_name='streambed_hn_demo'" \
    | grep -q 1; then
  echo "timed out waiting for Streambed's replication slot" >&2
  exit 1
fi

echo "==> Starting the HN ingester"
nohup "$BIN_DIR/hn-ingester" >"$LOG_DIR/hn-ingester.log" 2>&1 &
INGESTER_PID=$!
echo "$INGESTER_PID" >"$RUN_DIR/hn-ingester.pid"
STARTUP_COMPLETE=true

cat <<EOF

HN demo is starting.

  Source Postgres:  postgres://postgres:test@localhost:55432/hn
  Streambed query:  postgres://demo@localhost:55433/hn?sslmode=disable
  MinIO console:    http://localhost:59001

The first ingestion usually takes a few seconds. Follow progress with:
  tail -f $LOG_DIR/hn-ingester.log $LOG_DIR/streambed.log

Then run:
  $SCRIPT_DIR/smoke-test.sh
EOF
