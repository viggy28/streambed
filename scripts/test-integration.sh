#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/test/integration/docker-compose.yml"

cleanup() {
  docker compose -f "$COMPOSE_FILE" down -v
}
trap cleanup EXIT

echo "==> Starting Postgres and Silo..."
docker compose -f "$COMPOSE_FILE" up -d postgres minio --wait

echo "==> Creating test bucket..."
docker compose -f "$COMPOSE_FILE" up createbucket

echo "==> Running integration tests..."
cd "$PROJECT_DIR"
go test -tags integration -v -timeout 120s ./test/integration/...

echo "==> Running query compatibility oracle..."
go test -tags integration -v -timeout 10m ./test/querycompat
