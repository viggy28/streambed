#!/usr/bin/env bash
set -euo pipefail

# Hugo's enableGitInfo needs history for accurate per-page modification dates.
if [[ "$(git rev-parse --is-shallow-repository)" == "true" ]]; then
  git fetch --unshallow
fi

go mod download

# Avoid publishing files removed from static/ when a build directory is reused.
rm -rf public
hugo --minify
