#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CLOUDFLARE_DIR="$DEMO_DIR/cloudflare"
R2_ACCESS_KEY_ID=""
R2_SECRET_ACCESS_KEY=""

cleanup() {
  unset R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY
}
trap cleanup EXIT

for command in docker node npm security; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command not found: $command" >&2
    exit 1
  fi
done

R2_ACCESS_KEY_ID="$(security find-generic-password \
  -a streambed-hn-demo -s streambed-r2-reader-access-key-id -w)"
R2_SECRET_ACCESS_KEY="$(security find-generic-password \
  -a streambed-hn-demo -s streambed-r2-reader-secret-access-key -w)"

(
  cd "$CLOUDFLARE_DIR"
  npm install
  npm run check
  npm run deploy
  printf '%s' "$R2_ACCESS_KEY_ID" | npx wrangler secret put R2_ACCESS_KEY_ID
  printf '%s' "$R2_SECRET_ACCESS_KEY" | npx wrangler secret put R2_SECRET_ACCESS_KEY
)

echo "Cloudflare Worker and query container deployed"
