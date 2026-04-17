# Contributing to Streambed

Thanks for your interest in contributing to Streambed! Here's how to get started.

## Development Setup

1. **Prerequisites**: Go 1.22+, Docker, CGO enabled (required for go-duckdb and go-sqlite3)

2. **Clone and build**:
   ```bash
   git clone https://github.com/viggy28/streambed.git
   cd streambed
   go build -o streambed ./cmd/streambed
   ```

3. **Run locally**:
   ```bash
   docker compose up -d
   ./streambed sync \
     --source-url="postgres://postgres:test@localhost:5432/postgres" \
     --s3-bucket="streambed" \
     --s3-endpoint="http://localhost:9000" \
     --s3-prefix="test" \
     --query-addr=:5433
   ```

## Running Tests

```bash
# Unit tests
go test ./internal/... ./config/...

# Integration tests (requires Docker)
./scripts/test-integration.sh
```

Integration tests use the `integration` build tag and run against Postgres (port 5434) and MinIO (port 9002) from `test/integration/docker-compose.yml`.

## Submitting Changes

1. Fork the repository and create a feature branch from `main`
2. Make your changes
3. Ensure tests pass (`go test ./internal/... ./config/...`)
4. Open a pull request against `main`

Keep PRs focused — one logical change per PR makes review easier.

## Reporting Bugs

Open a GitHub issue with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Streambed version and environment details

## Questions?

Open a GitHub issue — we're happy to help.
