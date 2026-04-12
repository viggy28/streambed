# CLAUDE.md

## What is streambed?

Postgres-to-Iceberg CDC engine. Streams WAL changes via logical replication, writes Parquet data files to S3, commits Iceberg metadata, and exposes tables through a Postgres-compatible query server (DuckDB + psql-wire).

## Build & Run

```bash
# Build
go build -o streambed ./cmd/streambed

# Run locally (requires docker compose stack)
docker compose up
./streambed sync \
  --source-url="postgres://postgres:test@localhost:5432/postgres" \
  --s3-bucket="streambed" \
  --s3-endpoint="http://localhost:9000" \
  --s3-prefix="test" \
  --query-addr=:5433
```

## Testing

```bash
# Unit tests
go test ./internal/... ./config/...

# Integration tests (requires Docker)
./scripts/test-integration.sh

# Or manually:
docker compose -f test/integration/docker-compose.yml up -d --wait
go test -tags integration -v -timeout 120s ./test/integration/...
docker compose -f test/integration/docker-compose.yml down -v
```

Integration tests use build tag `integration`. They require Postgres (port 5434) and MinIO (port 9002) from `test/integration/docker-compose.yml`.

## Project Structure

```
cmd/streambed/main.go      Entry point, Cobra CLI (sync, query, cleanup, resync)
config/                     Config loading (flags + env vars, prefix STREAMBED_)
internal/
  pipeline/pipeline.go      Single-goroutine WAL consumer + writer loop
  iceberg/writer.go         Per-table buffering, CoW merge, Parquet flush to S3
  iceberg/catalog.go        Iceberg snapshot/manifest commit
  iceberg/schema.go         PG OID → Iceberg type mapping
  iceberg/avro.go           Avro encoding for Iceberg metadata
  wal/decoder.go            pgoutput v1 message parsing
  wal/types.go              RowEvent, Column, RelationMessage
  wal/slot.go               Replication slot management
  wal/resync.go             COPY-based table backfill
  parquet/builder.go        Build Parquet files from row data
  server/server.go          DuckDB query server (psql-wire)
  state/store.go            SQLite state persistence (LSNs, table registry)
  storage/s3.go             AWS SDK S3 client
test/integration/           End-to-end tests + docker-compose
specs/                      Design docs (rearchitecture, ack, phase specs)
```

## Key Concepts

- **Pipeline**: single goroutine reads WAL → decodes → buffers → flushes → acks. No channels between stages.
- **LSN (Log Sequence Number)**: WAL position. Used for dedup on restart and ack computation.
- **Ack formula**: `ack = receivedLSN` when all buffers empty; `ack = min(receivedLSN, pendingMinLSN-1)` otherwise.
- **CoW merge**: UPDATEs/DELETEs read existing Parquet, filter deleted rows, dedup new rows by key, write combined result.
- **State**: Iceberg snapshot summary (`last_flush_lsn`) is authoritative; SQLite is a fast-startup cache.
- **Flush triggers**: row count threshold (`--flush-rows`, default 10000) or time interval (`--flush-interval`, default 2s).

## Conventions

- Go standard layout. No Makefile — use `go build` / `go test` directly.
- CGO required (go-duckdb, go-sqlite3). Default `CGO_ENABLED=1`.
- `specs/` is gitignored — design docs exist locally but aren't committed to main.
- The `.gitignore` entry `streambed` matches the binary; use `git add -f` for paths under `cmd/streambed/`.
