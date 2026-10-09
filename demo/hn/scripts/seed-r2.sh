#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$DEMO_DIR/../.." && pwd)"
LOCAL_DIR="$DEMO_DIR/.local"
BIN_DIR="$LOCAL_DIR/bin"
LOG_DIR="$LOCAL_DIR/log"
BUCKET="${STREAMBED_DEMO_BUCKET:-streambed-hn-demo}"
PREFIX="${STREAMBED_DEMO_S3_PREFIX:-hn-demo}"
CLOUDFLARE_ACCOUNT_ID="${CLOUDFLARE_ACCOUNT_ID:-75c3ef432a1ebf389a8fdc403a582b1e}"
S3_ENDPOINT="${STREAMBED_DEMO_S3_ENDPOINT:-https://${CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com}"
POSTGRES_PORT="${STREAMBED_SEED_POSTGRES_PORT:-55434}"
QUERY_PORT="${STREAMBED_SEED_QUERY_PORT:-58082}"
POSTGRES_CONTAINER="streambed-hn-seed-postgres"
SLOT_NAME="streambed_hn_demo_seed"
STREAMBED_PID=""
QUERY_PID=""
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""

cleanup() {
  status=$?
  trap - EXIT
  for pid in "$QUERY_PID" "$STREAMBED_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
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
echo "==> Building Streambed and the HN ingester"
(
  cd "$PROJECT_DIR"
  go build -o "$BIN_DIR/streambed" ./cmd/streambed
  go build -o "$BIN_DIR/hn-ingester" ./demo/hn/cmd/hn-ingester
)

AWS_ACCESS_KEY_ID="$(security find-generic-password \
  -a streambed-hn-demo -s streambed-r2-writer-access-key-id -w)"
AWS_SECRET_ACCESS_KEY="$(security find-generic-password \
  -a streambed-hn-demo -s streambed-r2-writer-secret-access-key -w)"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION=auto AWS_EC2_METADATA_DISABLED=true

if AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    "$BIN_DIR/streambed" snapshots \
      --table=public.front_page \
      --s3-bucket="$BUCKET" \
      --s3-prefix="$PREFIX" \
      --s3-endpoint="$S3_ENDPOINT" \
      --s3-region=auto >/dev/null 2>&1; then
  echo "target s3://$BUCKET/$PREFIX already contains front_page history; refusing to mix seed runs" >&2
  exit 1
fi

echo "==> Starting an isolated seed Postgres"
docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
docker run -d --rm \
  --name "$POSTGRES_CONTAINER" \
  -p "127.0.0.1:${POSTGRES_PORT}:5432" \
  -e POSTGRES_DB=hn \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=test \
  postgres:16 \
    -c wal_level=logical \
    -c max_replication_slots=2 \
    -c max_wal_senders=2 >/dev/null

SOURCE_URL="postgres://postgres:test@127.0.0.1:${POSTGRES_PORT}/hn?sslmode=disable"
for _ in $(seq 1 60); do
  if psql -w "$SOURCE_URL" -Atc 'SELECT 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! psql -w "$SOURCE_URL" -Atc 'SELECT 1' >/dev/null 2>&1; then
  echo "seed Postgres did not become ready" >&2
  exit 1
fi

HN_DATABASE_URL="$SOURCE_URL" "$BIN_DIR/hn-ingester" --migrate-only

echo "==> Starting a temporary CDC writer into R2"
rm -f "$LOCAL_DIR/state-r2-seed.db" "$LOCAL_DIR/state-r2-seed.db-shm" "$LOCAL_DIR/state-r2-seed.db-wal"
"$BIN_DIR/streambed" sync \
  --source-url="$SOURCE_URL" \
  --s3-bucket="$BUCKET" \
  --s3-prefix="$PREFIX" \
  --s3-endpoint="$S3_ENDPOINT" \
  --s3-region=auto \
  --state-path="$LOCAL_DIR/state-r2-seed.db" \
  --slot-name="$SLOT_NAME" \
  --include-tables=public.stories,public.story_analytics,public.story_monthly,public.story_leaders,public.rankings,public.front_page \
  --flush-rows=100 \
  --flush-interval=2s \
  >"$LOG_DIR/r2-seed-streambed.log" 2>&1 &
STREAMBED_PID=$!

for _ in $(seq 1 60); do
  if psql -w "$SOURCE_URL" -Atc \
      "SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT_NAME'" | grep -q 1; then
    break
  fi
  if ! kill -0 "$STREAMBED_PID" 2>/dev/null; then
    tail -100 "$LOG_DIR/r2-seed-streambed.log" >&2
    exit 1
  fi
  sleep 1
done
if ! psql -w "$SOURCE_URL" -Atc \
    "SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT_NAME'" | grep -q 1; then
  echo "timed out waiting for the seed replication slot" >&2
  exit 1
fi

echo "==> Running HN seed poll 1/2"
HN_DATABASE_URL="$SOURCE_URL" "$BIN_DIR/hn-ingester" --once

SNAPSHOTS_FILE="$LOCAL_DIR/r2-front-page-snapshots.tsv"
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
if [[ "${snapshot_count:-0}" -lt 1 ]]; then
  echo "R2 seed did not produce the initial front_page snapshot" >&2
  exit 1
fi

# Wait for the first snapshot before producing another transaction. Otherwise
# both polls can be coalesced into one flush when the HN front page is stable.
echo "==> Running HN seed poll 2/2"
HN_DATABASE_URL="$SOURCE_URL" "$BIN_DIR/hn-ingester" --once
# PostgreSQL emits this no-op UPDATE to logical replication, while the stored
# values remain faithful to HN. It makes retained history deterministic.
psql -w "$SOURCE_URL" -v ON_ERROR_STOP=1 -c \
  'UPDATE front_page SET score = score WHERE rank = 1' >/dev/null

for _ in $(seq 1 120); do
  if "$BIN_DIR/streambed" snapshots \
      --table=public.front_page \
      --s3-bucket="$BUCKET" \
      --s3-prefix="$PREFIX" \
      --s3-endpoint="$S3_ENDPOINT" \
      --s3-region=auto >"$SNAPSHOTS_FILE" 2>/dev/null; then
    snapshot_count="$(tail -n +2 "$SNAPSHOTS_FILE" | grep -c . || true)"
    [[ "$snapshot_count" -ge 2 ]] && break
  fi
  sleep 1
done
if [[ "${snapshot_count:-0}" -lt 2 ]]; then
  echo "R2 seed did not produce two front_page snapshots" >&2
  exit 1
fi

kill -INT "$STREAMBED_PID"
wait "$STREAMBED_PID"
STREAMBED_PID=""

echo "==> Verifying R2 through the standalone HTTP query server"
"$BIN_DIR/streambed" query \
  --s3-bucket="$BUCKET" \
  --s3-prefix="$PREFIX" \
  --s3-endpoint="$S3_ENDPOINT" \
  --s3-region=auto \
  --http-listen-addr=":${QUERY_PORT}" \
  --query-memory-limit-mb=128 \
  >"$LOG_DIR/r2-seed-query.log" 2>&1 &
QUERY_PID=$!

for _ in $(seq 1 90); do
  if curl -fsS "http://127.0.0.1:${QUERY_PORT}/health" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$QUERY_PID" 2>/dev/null; then
    tail -100 "$LOG_DIR/r2-seed-query.log" >&2
    exit 1
  fi
  sleep 1
done
QUERY_URL="http://127.0.0.1:${QUERY_PORT}/query"
current_count="$(curl -fsS -H 'Content-Type: application/json' \
  --data '{"sql":"SELECT count(*) FROM front_page"}' "$QUERY_URL" | jq -r '.rows[0][0]')"
historical_timestamp="$(tail -n 1 "$SNAPSHOTS_FILE" | cut -f2)"
historical_payload="$(jq -nc --arg sql \
  "SELECT count(*) FROM front_page AS f AT (TIMESTAMP => TIMESTAMPTZ '$historical_timestamp')" \
  '{sql:$sql}')"
historical_count="$(curl -fsS -H 'Content-Type: application/json' \
  --data "$historical_payload" "$QUERY_URL" | jq -r '.rows[0][0]')"
if [[ "$current_count" != 30 || "$historical_count" != 30 ]]; then
  echo "seed verification failed: current=$current_count historical=$historical_count" >&2
  exit 1
fi

echo "Seeded and verified s3://$BUCKET/$PREFIX with $snapshot_count front_page snapshots"
