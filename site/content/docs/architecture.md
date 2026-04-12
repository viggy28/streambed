---
title: Architecture
weight: 4
prev: /docs/configuration
---

## Overview

```
Postgres WAL ──▶ Decode ──▶ Buffer ──▶ Parquet ──▶ S3 ──▶ Iceberg Commit
                                                              │
                                                    DuckDB ◀──┘ (query server)
```

## Pipeline

The pipeline is a single goroutine that reads WAL, decodes, buffers, flushes, and acknowledges. There are no channels between stages — this keeps the design simple and makes reasoning about ordering and backpressure straightforward.

## Key Components

### WAL Decoder

Parses pgoutput v1 messages from the Postgres logical replication stream. Handles relation messages, inserts, updates, and deletes.

### Writer / Buffer

Buffers rows per table in memory. When a flush trigger fires (row count or time interval), the writer:

1. Builds a Parquet file from buffered rows
2. For updates/deletes: reads existing Parquet data, filters out deleted/updated rows, merges with new rows (copy-on-write)
3. Uploads the Parquet file to S3
4. Commits an Iceberg snapshot with updated manifest

### Iceberg Catalog

Manages Iceberg metadata — snapshots, manifests, and schema. Maps Postgres OIDs to Iceberg types.

### Query Server

Embeds DuckDB and serves queries over the Postgres wire protocol via psql-wire. Reads directly from Iceberg tables on S3.

### State Store

SQLite-backed persistence for LSN positions and table registry. Used for fast startup — the Iceberg snapshot summary (`last_flush_lsn`) is the authoritative source of truth.

## Project Layout

```
cmd/streambed/main.go      Entry point, Cobra CLI
config/                     Config loading (flags + env vars)
internal/
  pipeline/pipeline.go      WAL consumer + writer loop
  iceberg/writer.go         Per-table buffering, CoW merge, Parquet flush
  iceberg/catalog.go        Iceberg snapshot/manifest commit
  iceberg/schema.go         PG OID → Iceberg type mapping
  iceberg/avro.go           Avro encoding for Iceberg metadata
  wal/decoder.go            pgoutput message parsing
  wal/types.go              RowEvent, Column, RelationMessage
  wal/slot.go               Replication slot management
  wal/resync.go             COPY-based table backfill
  parquet/builder.go        Parquet file builder
  server/server.go          DuckDB query server
  state/store.go            SQLite state persistence
  storage/s3.go             S3 client
```
