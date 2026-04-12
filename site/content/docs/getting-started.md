---
title: Getting Started
weight: 1
next: /docs/commands
prev: /docs
---

## Prerequisites

- Go 1.22+ with CGO enabled (required for go-duckdb and go-sqlite3)
- Docker (for local Postgres and MinIO)

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

## How It Works

Streambed connects to Postgres as a logical replication subscriber. It:

1. **Decodes** WAL messages (inserts, updates, deletes) using pgoutput
2. **Buffers** rows per table in memory
3. **Flushes** them as Parquet files to S3 on a configurable interval or row threshold
4. **Commits** Iceberg metadata (snapshot + manifest)
5. **Serves queries** over the Postgres wire protocol via embedded DuckDB

Updates and deletes use copy-on-write merging against existing Parquet data.

### Flush Triggers

Streambed flushes buffered rows to S3 when either condition is met:

- **Row count**: `--flush-rows` (default: 10,000)
- **Time interval**: `--flush-interval` (default: 2s)

### LSN Acknowledgment

Streambed tracks WAL position using Log Sequence Numbers (LSNs):

- When all buffers are empty: `ack = receivedLSN`
- When buffers have pending data: `ack = min(receivedLSN, pendingMinLSN - 1)`

This ensures Postgres doesn't discard WAL that Streambed hasn't yet flushed to S3.
