#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/test/integration/docker-compose.yml"

echo "==> Starting Postgres and MinIO..."
docker compose -f "$COMPOSE_FILE" up -d --wait

echo "==> Waiting for services to be healthy..."
sleep 3

echo "==> Running integration tests..."
cd "$PROJECT_DIR"
go test -tags integration -v -timeout 120s ./test/integration/...
TEST_EXIT=$?

echo "==> Stopping services..."
docker compose -f "$COMPOSE_FILE" down -v

exit $TEST_EXIT
