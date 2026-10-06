#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$DEMO_DIR/../.." && pwd)"
LOCAL_DIR="$DEMO_DIR/.local"
BIN_DIR="$LOCAL_DIR/bin"
RUN_DIR="$LOCAL_DIR/run"
SUPABASE_PROJECT_REF="${SUPABASE_PROJECT_REF:-tvnljlooazyxocdtovvy}"
SUPABASE_HOST="${SUPABASE_DB_HOST:-db.${SUPABASE_PROJECT_REF}.supabase.co}"
SUPABASE_KEYCHAIN_ACCOUNT="${SUPABASE_KEYCHAIN_ACCOUNT:-streambed-hn-demo}"
SUPABASE_KEYCHAIN_SERVICE="${SUPABASE_KEYCHAIN_SERVICE:-streambed-supabase-db-password}"

streambed_pid_file="$RUN_DIR/streambed.pid"
if [[ -f "$streambed_pid_file" ]] && kill -0 "$(cat "$streambed_pid_file")" 2>/dev/null; then
  echo "Streambed is still running; run $SCRIPT_DIR/stop-local.sh first" >&2
  exit 1
fi

mkdir -p "$BIN_DIR"
(cd "$PROJECT_DIR" && go build -o "$BIN_DIR/hn-ingester" ./demo/hn/cmd/hn-ingester)

DB_PASSWORD="$(security find-generic-password \
  -a "$SUPABASE_KEYCHAIN_ACCOUNT" \
  -s "$SUPABASE_KEYCHAIN_SERVICE" \
  -w)"
ENCODED_PASSWORD="$(DB_PASSWORD="$DB_PASSWORD" python3 -c \
  'import os, urllib.parse; print(urllib.parse.quote(os.environ["DB_PASSWORD"], safe=""))')"
DB_URL="postgres://postgres:${ENCODED_PASSWORD}@${SUPABASE_HOST}:5432/postgres?sslmode=require"

HN_DATABASE_URL="$DB_URL" "$BIN_DIR/hn-ingester" \
  --drop-replication-slot=streambed_hn_demo

unset DB_PASSWORD ENCODED_PASSWORD DB_URL
echo "Supabase replication slot cleanup finished"
