# Streambed

Postgres-to-Iceberg CDC engine. Offload analytical queries from your production database without changing your application.

streambed streams WAL changes via logical replication, writes Parquet files to S3, and commits Iceberg metadata. Query the result with any Iceberg-compatible engine -- or use the built-in query server, which speaks the Postgres wire protocol so you can connect with `psql`.

## See It In Action

Same analytical query on pgbench (1M accounts, 500K history rows). Postgres on the left, Streambed on the right.

<!-- TODO: Record with: vhs demo.tape -->

![Demo](docs/demo.gif)

No ETL. No Spark. Just Postgres + S3.

## Quick Start

```bash
# Start Postgres + MinIO locally
docker compose up -d

# Build
go build -o streambed ./cmd/streambed

# Start syncing + query server on :5433
./streambed sync \
  --source-url="postgres://postgres:test@localhost:5432/postgres" \
  --s3-bucket="streambed" \
  --s3-endpoint="http://localhost:9000" \
  --s3-prefix="test" \
  --query-addr=:5433

# Query your Postgres tables via Iceberg
psql -h localhost -p 5433 -U postgres -d postgres
```

Run `streambed sync --help` for all configuration options. All flags support environment variables with `STREAMBED_` prefix (e.g. `STREAMBED_SOURCE_URL`).

## Architecture

![Architecture](/docs/architecture.svg)

## How It Works

```
Postgres WAL ──▶ Decode ──▶ Buffer ──▶ Parquet ──▶ S3 ──▶ Iceberg Commit
                                                              │
                                                    DuckDB ◀──┘ (query server)
```

Streambed connects to Postgres as a logical replication subscriber. It decodes WAL messages (inserts, updates, deletes), buffers rows per table, and periodically flushes them as Parquet files to S3 with Iceberg metadata commits. Updates and deletes use copy-on-write merging against existing Parquet data.

A query server exposes Iceberg tables over the Postgres wire protocol using embedded DuckDB, so you can query with psql or any Postgres client.


## Commands

| Command | What it does |
|---------|-------------|
| `streambed sync` | Main daemon. Streams WAL, writes Iceberg, optionally serves queries. |
| `streambed resync --table=public.users` | One-shot backfill via `COPY` under a consistent snapshot. |
| `streambed query` | Standalone query server (no sync). Points at existing Iceberg tables. |
| `streambed cleanup --table=public.users` | Deletes S3 objects and state for a table. Useful before `resync`. |


## Development

Requires Go 1.22+ and CGO (for go-duckdb and go-sqlite3).

```bash
# Build
go build -o streambed ./cmd/streambed

# Unit tests
go test ./internal/... ./config/...

# Integration tests (requires Docker)
./scripts/test-integration.sh
```

Integration tests use the `integration` build tag and run against Postgres (port 5434) and MinIO (port 9002) from `test/integration/docker-compose.yml`.
