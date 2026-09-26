# Streambed

[![CI](https://github.com/viggy28/streambed/actions/workflows/ci.yml/badge.svg)](https://github.com/viggy28/streambed/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/viggy28/streambed.svg)](https://pkg.go.dev/github.com/viggy28/streambed)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

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

Run `streambed sync --help` for all configuration options. All flags support environment variables with `STREAMBED_` prefix (e.g. `STREAMBED_SOURCE_URL`). UPDATE/DELETE use copy-on-write by default; opt into Iceberg v2 equality deletes with `--mutation-mode=mor` (or `STREAMBED_MUTATION_MODE=mor`) after verifying reader compatibility. Once a table has active equality deletes, Streambed fails startup in COW mode; continue using MOR.

Use `--target-file-size-mb` (or `STREAMBED_TARGET_FILE_SIZE_MB`) to split large flushes into approximately target-sized Parquet data files. The default is 128 MiB. This is a target, not a hard maximum; small flushes still create small files.

To reduce load on your primary, point `--source-url` at a **hot-standby replica** and add `--primary-url` for the primary (used only for publication and slot setup). Requires Postgres 16+. See [docs/replica.md](docs/replica.md) for details.

### DuckLake catalog defaults

With `--target-format=ducklake`, Streambed stores catalog metadata in a local
DuckDB database by default:

```text
~/.streambed/ducklake-catalog.duckdb
```

Parquet data remains under `s3://<bucket>/<prefix>/ducklake/`. Both catalog
settings can be overridden explicitly:

```bash
--ducklake-catalog=/path/to/catalog.duckdb \
--ducklake-catalog-store=duckdb
```

SQLite catalogs remain supported with `--ducklake-catalog-store=sqlite`.
Installations created before the DuckDB default must pass both legacy settings
to continue using their existing catalog; Streambed does not silently migrate
or overwrite it:

```bash
--ducklake-catalog="$HOME/.streambed/ducklake-catalog.sqlite" \
--ducklake-catalog-store=sqlite
```

The equivalent environment variables are `STREAMBED_DUCKLAKE_CATALOG` and
`STREAMBED_DUCKLAKE_CATALOG_STORE`.

## Architecture

![Architecture](/docs/architecture.svg)

## How It Works

```
Postgres WAL ──▶ Decode ──▶ Buffer ──▶ Parquet ──▶ S3 ──▶ Iceberg Commit
                                                              │
                                                    DuckDB ◀──┘ (query server)
```

Streambed connects to Postgres as a logical replication subscriber. It decodes WAL messages (inserts, updates, deletes), buffers rows per table, and periodically flushes them as Parquet files to S3 with Iceberg metadata commits. Updates and deletes use copy-on-write merging against existing Parquet data.

A query server exposes lakehouse tables over the Postgres wire protocol using embedded DuckDB, so you can query with psql or any Postgres client.

## Time Travel

Streambed retains a snapshot at each lakehouse commit. Query a retained snapshot with DuckLake's native `AT` syntax:

```sql
SELECT o.id, c.name
FROM orders AS o AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00')
JOIN customers AS c AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00')
  ON c.id = o.customer_id;
```

Aliases must appear before `AT`. V1 supports historical joins, but every historical table in one query must use the same timestamp. Timestamp literals without an offset are interpreted as UTC; explicit offsets are also accepted. Queries without `AT` continue to read the latest state.

The timestamp is the Streambed flush/commit time, not the original Postgres transaction time. DuckLake snapshots are catalog-wide, while Iceberg snapshots are resolved independently per table to the latest retained snapshot at or before the requested time. Expired snapshots and their deleted files cannot be queried. Iceberg also has a known limitation: deleting or truncating every row currently clears that table's accessible snapshot history.

Inspect retained snapshots with:

```bash
./streambed snapshots --target-format=ducklake --table=public.orders
# or
./streambed snapshots --target-format=iceberg --table=public.orders \
  --s3-bucket=streambed --s3-prefix=test
```


## Commands

| Command | What it does |
|---------|-------------|
| `streambed sync` | Main daemon. Streams WAL, writes Iceberg, optionally serves queries. |
| `streambed resync --table=public.users` | One-shot backfill via `COPY` under a consistent snapshot. |
| `streambed query` | Standalone query server (no sync). Points at existing lakehouse tables. |
| `streambed snapshots --table=public.users` | Lists retained snapshots and Streambed commit metadata. |
| `streambed cleanup --table=public.users` | Deletes S3 objects and state for a table. Useful before `resync`. |
| `streambed maintenance --table=public.users` | Expires old Iceberg snapshots and can dry-run orphan planning. |
| `streambed maintenance compact --table=public.users` | Compacts many small active data files into fewer target-sized files. |


See [docs/maintenance.md](docs/maintenance.md) for snapshot expiration and small-file compaction details.

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
