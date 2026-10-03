#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

"$SCRIPT_DIR/stop-local.sh"
docker compose -f "$DEMO_DIR/docker-compose.yml" down -v --remove-orphans
rm -rf "$DEMO_DIR/.local"
echo "HN demo state was removed"
