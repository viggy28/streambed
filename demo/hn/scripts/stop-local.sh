#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_DIR="$DEMO_DIR/.local/run"

for process in hn-ingester streambed; do
  pid_file="$RUN_DIR/$process.pid"
  if [[ ! -f "$pid_file" ]]; then
    continue
  fi
  pid="$(cat "$pid_file")"
  if kill -0 "$pid" 2>/dev/null; then
    echo "==> Stopping $process (PID $pid)"
    kill "$pid"
    for _ in $(seq 1 20); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.25
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid"
    fi
  fi
  rm -f "$pid_file"
done

docker compose -f "$DEMO_DIR/docker-compose.yml" down
