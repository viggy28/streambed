#!/usr/bin/env bash
set -euo pipefail

# Hugo's enableGitInfo needs history for accurate per-page modification dates.
if [[ "$(git rev-parse --is-shallow-repository)" == "true" ]]; then
  git fetch --unshallow
fi

go mod download
hugo --minify
