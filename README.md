# streambed

Postgres-to-Iceberg analytics engine. Streams WAL changes from Postgres via logical replication and writes them as Iceberg tables on S3.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              streambed                                      │
│                   Postgres-to-Iceberg Analytics Engine                       │
└─────────────────────────────────────────────────────────────────────────────┘

                          ┌──────────────┐
                          │   main.go    │
                          │  (runSync)   │
                          │  Orchestrator│
                          └──────┬───────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                   │
              ▼                  ▼                   ▼
    ┌─────────────────┐  ┌──────────────┐  ┌────────────────┐
    │  Signal Handler │  │   Config     │  │  State Store   │
    │  (goroutine)    │  │  config.go   │  │   store.go     │
    │                 │  │              │  │   (SQLite)     │
    │ SIGINT/SIGTERM  │  │ CLI flags +  │  │                │
    │   → cancel()    │  │ env vars     │  │ synced_tables  │
    └─────────────────┘  └──────────────┘  └────────────────┘


═══════════════════════ DATA FLOW ═══════════════════════════

┌──────────┐    WAL Stream     ┌─────────────────────────────────────────┐
│          │  (replication=     │          Consumer (wal/consumer.go)     │
│ Postgres │   database)       │          [main goroutine]               │
│          │──────────────────▶│                                         │
│  ┌─────┐ │                   │  1. StartReplication(startLSN)          │
│  │ WAL │ │                   │  2. ReceiveMessage(ctx) loop            │
│  └─────┘ │                   │  3. Decode: Relation/Insert/Commit      │
│  ┌─────┐ │  confirmed_       │  4. Dedup via per-table LSN             │
│  │Slot │ │◀─flush_lsn───────│  5. SendStandbyStatusUpdate             │
│  └─────┘ │  (via ackCh)     │                                         │
└──────────┘                   └───────────────┬─────────────────────────┘
                                               │
                                               │ events channel
                                               │ (chan RowEvent, 1000)
                                               │
                                               │  RowEvent{Schema, Table,
                                               │   Columns, Values, WALStartLSN}
                                               ▼
                               ┌───────────────────────────────────────┐
                               │        Writer (iceberg/writer.go)     │
                               │        [writer goroutine]             │
                               │                                       │
                               │  select loop:                         │
                               │    case event ← events: buffer(event) │
                               │    case ← ticker: flushAll()          │
                               │    case ← ctx.Done(): flushAll()      │
                               │                                       │
                               │  Per-table buffers:                   │
                               │  map[string]*tableBuffer              │
                               │    └─ {Schema, Table, Columns,        │
                               │        Rows [][]Value, LastLSN}       │
                               └───────────────┬───────────────────────┘
                                               │
                               ┌───────────────┼───────────────────────┐
                               │         flush(key) pipeline           │
                               │                                       │
                               │  ┌──────────────────────────────┐     │
                               │  │ 1. Parquet Builder           │     │
                               │  │    (parquet/builder.go)      │     │
                               │  │    Cols + Rows → .parquet    │     │
                               │  │    Snappy compression        │     │
                               │  │    Pg OID → Parquet types    │     │
                               │  └──────────────┬───────────────┘     │
                               │                 ▼                     │
                               │  ┌──────────────────────────────┐     │
                               │  │ 2. S3 Upload                 │     │
                               │  │    (storage/s3.go)           │     │
                               │  │    PutObject(data.parquet)   │     │
                               │  └──────────────┬───────────────┘     │
                               │                 ▼                     │
                               │  ┌──────────────────────────────┐     │
                               │  │ 3. Iceberg Commit            │     │
                               │  │    (iceberg/catalog.go)      │     │
                               │  │    Manifest (Avro) →         │     │
                               │  │    Manifest List (Avro) →    │     │
                               │  │    v(N+1).metadata.json →    │     │
                               │  │    version-hint.text          │     │
                               │  └──────────────┬───────────────┘     │
                               │                 ▼                     │
                               │  ┌──────────────────────────────┐     │
                               │  │ 4. State Update              │     │
                               │  │    UpdateLastFlush(LSN)      │     │
                               │  └──────────────┬───────────────┘     │
                               │                 ▼                     │
                               │  ┌──────────────────────────────┐     │
                               │  │ 5. Ack: leastLSN → ackCh    │     │
                               │  └──────────────────────────────┘     │
                               └───────────────────────────────────────┘
                                               │
                                               │ ackCh (chan LSN, 10)
                                               │
                                               ▼
                                    back to Consumer for
                                    SendStandbyStatusUpdate


═══════════════════ CONCURRENCY MODEL ══════════════════════

  Goroutine 1 (main)          Goroutine 2 (writer)        Goroutine 3 (signal)
  ┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐
  │ consumer.Start() │──events──▶ writer.Start() │        │ sig := <-sigCh   │
  │                  │◀──ackCh──│                │        │ cancel()         │
  │ close(events)    │         │                 │        └──────────────────┘
  │ <-writerErrCh    │◀─errCh──│                │
  └──────────────────┘         └──────────────────┘
         │                            │                          │
         └────────────────────────────┴──────────────────────────┘
                     shared: ctx (cancel propagation)


═══════════════════ SHUTDOWN FLOW ══════════════════════════

  Ctrl+C → SIGINT → sigCh → cancel() → ctx.Done() closes
                                           │
                      ┌────────────────────┼────────────────────┐
                      ▼                    ▼                    ▼
               Consumer exits       Writer: flushAll()    Signal goroutine
               (ReceiveMessage      (context.Background)  already done
                returns err)
                      │
                      ▼
               close(events)
                      │
                      ▼
               Writer returns → writerErrCh → main exits


═══════════════════ S3 / ICEBERG LAYOUT ════════════════════

  s3://<bucket>/<prefix>/
  └── <schema>/
      └── <table>/
          ├── data/
          │   ├── <uuid>.parquet
          │   ├── <uuid>.parquet
          │   └── ...
          └── metadata/
              ├── version-hint.text          ← current version N
              ├── v1.metadata.json           ← initial (no snapshots)
              ├── v2.metadata.json           ← after first flush
              ├── v3.metadata.json           ← after second flush
              ├── <uuid>-m0.avro             ← manifest files
              └── snap-<id>-<uuid>.avro      ← manifest lists


═══════════════════ STATE STORE (SQLite) ═══════════════════

  synced_tables
  ┌─────────────┬────────────┬──────────────┬────────────┬────────────┬────────────────┐
  │ schema_name │ table_name │ column_count │ first_seen │ last_flush │ last_flush_lsn │
  ├─────────────┼────────────┼──────────────┼────────────┼────────────┼────────────────┤
  │ public      │ users      │ 5            │ 2026-03-27 │ 2026-03-27 │ 1/F84F2938     │
  └─────────────┴────────────┴──────────────┴────────────┴────────────┴────────────────┘

  Used for: dedup on restart, startLSN selection, per-table LSN tracking
```

## Quick Start

```bash
# Install dependencies
go mod download

# Build
go build -o streambed ./cmd/streambed

# Run

streambed sync \
  --query-addr :5433 \
  --source-url="postgres://postgres:test@localhost:5432/postgres" \
  --s3-bucket="streambed" \
  --s3-endpoint="http://localhost:9000" \
  --s3-region="us-east-1" \
  --s3-prefix="test"

# then in another session start querying your Postgres data:
psql -h localhost -U postgres -d postgres -p 5433
```

## Configuration

| Flag | Env | Description |
|------|-----|-------------|
| `--source-url` | `SOURCE_URL` | Postgres connection URL |
| `--s3-bucket` | `S3_BUCKET` | S3 bucket name |
| `--s3-prefix` | `S3_PREFIX` | S3 key prefix |
| `--s3-endpoint` | `S3_ENDPOINT` | Custom S3 endpoint (MinIO) |
| `--s3-region` | `S3_REGION` | AWS region |
| `--state-path` | `STATE_PATH` | SQLite state file path |
| `--slot-name` | `SLOT_NAME` | Replication slot name |
| `--flush-rows` | `FLUSH_ROWS` | Row buffer flush threshold |
| `--flush-interval` | `FLUSH_INTERVAL` | Time-based flush interval |
| `--include-tables` | `INCLUDE_TABLES` | Tables to include |
| `--exclude-tables` | `EXCLUDE_TABLES` | Tables to exclude |
| `--log-level` | `LOG_LEVEL` | Log level (DEBUG, INFO, WARN, ERROR) |

## Profiling

streambed exposes pprof on `localhost:6060`:

```bash
# CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Goroutine dump
go tool pprof http://localhost:6060/debug/pprof/goroutine
```
