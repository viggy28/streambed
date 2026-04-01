# Streambed

## What This Is

Streambed is a Postgres-to-Iceberg analytics engine. It connects to any Postgres
database as a logical replication client, reads the WAL, and writes the data as
Iceberg/Parquet files to S3. It then exposes a Postgres-wire-protocol query
interface backed by DuckDB so any Postgres client can run analytics queries
against the Iceberg data.

**One-line positioning:** If your source of truth is Postgres, you never needed
Snowflake.

**What it is not:** Streambed is not an extension, not a fork of Postgres, and
not a sidecar process that requires changes to the source database. The source
Postgres instance is completely unaware of Streambed. No schema changes. No
extensions. No superuser required beyond `REPLICATION` privilege.

---

## Architecture

### Single Binary

Streambed is a single Go binary. There are no sidecar processes, no catalog
databases, no external dependencies beyond S3 and a Postgres source.

```
streambed (single OS process)
│
├── wal.Consumer        goroutine — reads Postgres replication slot via pglogrepl
│       │
│       │   channel of RowEvents (Insert / Update / Delete + schema)
│       ▼
├── iceberg.Writer      goroutine(s) — buffers rows, flushes Parquet to S3,
│       │               commits Iceberg snapshots via filesystem catalog
│       ▼
│   S3  (Parquet data files + version-hint.text per table)
│
└── state.Store         SQLite embedded in process — tracks LSN watermarks,
                        table registry, sync health. NOT the Iceberg catalog.
```

### Catalog Strategy

**No external catalog database.** Iceberg metadata is stored directly in S3
using the filesystem catalog pattern:

```
s3://<bucket>/<prefix>/
  <schema>/<table>/
    version-hint.text           ← integer, current metadata version
    metadata/
      v00001-<uuid>.metadata.json
      v00002-<uuid>.metadata.json
      ...
    data/
      <uuid>.parquet
      ...
```

To find the current table state: read `version-hint.text`, derive the metadata
path, load the metadata JSON. The catalog is S3 itself.

This means the Iceberg data is fully self-describing. Any engine (Athena, Spark,
DuckDB, Trino) can read it directly from S3 without going through Streambed.

### Internal State (SQLite)

SQLite is used for Streambed's own bookkeeping only — not for Iceberg metadata:

- Current confirmed LSN per table
- Replication slot name and status
- Table registry (which tables are being synced)
- Sync health / last flush timestamp per table

SQLite lives at a configurable path (default: `~/.streambed/state.db`).

---

## Invariants — Never Violate These

1. **No external catalog database.** Never introduce a Postgres, Redis, or any
   other networked dependency for catalog purposes. S3 + SQLite only.

2. **Parquet write before Iceberg commit.** Never update `version-hint.text`
   until the Parquet file is durably written and `PutObject` has returned
   success. The commit is the last step, not a concurrent one.

3. **Single replication slot per deployment.** One slot fans out to per-table
   writers internally via Go channels. Do not create one slot per table.

4. **Flush on two conditions — whichever comes first:**
   - Row buffer reaches threshold (default: 10,000 rows)
   - Time threshold reached (default: 30 seconds)

5. **LSN ack after flush.** Postgres replication slot LSN is only advanced after
   the corresponding Iceberg snapshot is committed. Never ack before commit.

6. **Phase gate.** Do not build Phase 2 or Phase 3 code until Phase 1 has
   passing tests. The current phase is listed below.

7. **Source Postgres is read-only.** Streambed never writes to the source
   database. The only interaction is: create replication slot, consume WAL.

---

## Current Phase: PHASE 1

### Phase 1 Goals (Weeks 1–3)

Produce a working binary that:
- Connects to any Postgres database
- Creates and manages a logical replication slot
- Decodes pgoutput WAL messages: Relation, Insert (UPDATE/DELETE come in Phase 2)
- Buffers rows and flushes to Parquet files on S3
- Commits Iceberg snapshots using the filesystem catalog
- Tracks LSN in SQLite
- Does NOT have a query layer (that is Phase 3)

**End-of-Phase-1 proof:** The following must work:

```bash
streambed sync \
  --source-url postgres://user:pass@host:5432/dbname \
  --s3-bucket my-bucket \
  --s3-prefix streambed/

# Then, in DuckDB:
duckdb -c "SELECT COUNT(*) FROM iceberg_scan('s3://my-bucket/streambed/public/orders/');"
```

### Phase 2 Goals (Weeks 4–6)

- UPDATE and DELETE support (Iceberg v2 equality delete files)
- LSN acknowledgment to allow Postgres to advance the slot
- Schema change detection (ALTER TABLE handling)
- Complete Postgres → Parquet type mapping

### Phase 3 Goals (Weeks 7–9)

- psql-wire server (using `jeroenrinzema/psql-wire`)
- Embedded DuckDB query engine reading from S3 Iceberg files
- Schema resolution: `SELECT * FROM orders` → correct Iceberg path
- Verified compatibility: psql, TablePlus, Metabase, Grafana, dbt

### Phase 4 Goals (Weeks 10–14)

- Parquet file compaction (merge small files)
- Parallel table sync (goroutine per table, shared LSN store)
- Prometheus metrics endpoint
- Multi-tenant namespace support

---

## Directory Structure

```
streambed/
├── CLAUDE.md                       ← you are here
├── go.mod
├── go.sum
├── cmd/
│   └── streambed/
│       └── main.go                 ← CLI entrypoint (cobra)
├── internal/
│   ├── wal/
│   │   ├── consumer.go             ← replication slot consumer loop
│   │   ├── decoder.go              ← pgoutput → typed RowEvent
│   │   └── slot.go                 ← slot creation / teardown
│   ├── iceberg/
│   │   ├── writer.go               ← receives RowEvents, flushes to S3
│   │   ├── catalog.go              ← S3 filesystem catalog (version-hint.text)
│   │   └── schema.go               ← Postgres OID → Iceberg/Parquet type map
│   ├── parquet/
│   │   └── builder.go              ← builds Parquet files from row batches
│   ├── state/
│   │   └── store.go                ← SQLite: LSN tracking, table registry
│   ├── storage/
│   │   └── s3.go                   ← S3 abstraction (PutObject, GetObject)
│   └── server/                     ← Phase 3 only — do not build in Phase 1
│       └── server.go
├── config/
│   └── config.go                   ← env vars + optional YAML config
└── docs/
    └── architecture.md
```

---

## Go Module Dependencies

```
jackc/pglogrepl                     WAL consumer, pgoutput decoder
jackc/pgx/v5                        Postgres driver (also used by pglogrepl)
apache/iceberg-go                   Iceberg table management
parquet-go/parquet-go               Parquet file writing
aws/aws-sdk-go-v2                   S3 client
mattn/go-sqlite3                    SQLite (CGO required)
spf13/cobra                         CLI
```

Phase 3 adds:
```
jeroenrinzema/psql-wire             Postgres wire protocol server
marcboeker/go-duckdb                DuckDB embedded query engine
apache/arrow/go/v17                 Arrow in-memory format (DuckDB interop)
```

---

## Type Mapping (Postgres → Parquet/Iceberg)

| PostgreSQL          | Parquet               | Iceberg           |
|---------------------|-----------------------|-------------------|
| bool                | BOOLEAN               | boolean           |
| int2, int4          | INT32                 | int               |
| int8                | INT64                 | long              |
| float4              | FLOAT                 | float             |
| float8              | DOUBLE                | double            |
| numeric             | FIXED_LEN_BYTE_ARRAY  | decimal(P, S)     |
| date                | INT32 (DATE)          | date              |
| timestamp           | INT64 (MICROS)        | timestamp         |
| timestamptz         | INT64 (MICROS, UTC)   | timestamptz       |
| varchar, text       | BYTE_ARRAY (UTF8)     | string            |
| uuid                | BYTE_ARRAY (UTF8)     | uuid              |
| json, jsonb         | BYTE_ARRAY (UTF8)     | string (JSON)     |
| bytea               | BYTE_ARRAY            | binary            |
| _* (arrays)         | LIST                  | list              |

---

## Configuration

All config via environment variables (CLI flags override):

```
STREAMBED_SOURCE_URL          Postgres connection string (required)
STREAMBED_S3_BUCKET           S3 bucket name (required)
STREAMBED_S3_PREFIX           S3 key prefix (default: streambed/)
STREAMBED_S3_ENDPOINT         Custom S3 endpoint for MinIO etc (optional)
STREAMBED_S3_REGION           AWS region (default: us-east-1)
AWS_ACCESS_KEY_ID             AWS credentials
AWS_SECRET_ACCESS_KEY         AWS credentials
STREAMBED_STATE_PATH          SQLite file path (default: ~/.streambed/state.db)
STREAMBED_SLOT_NAME           Replication slot name (default: streambed)
STREAMBED_FLUSH_ROWS          Row buffer threshold (default: 10000)
STREAMBED_FLUSH_INTERVAL_SEC  Time flush threshold in seconds (default: 30)
STREAMBED_INCLUDE_TABLES      Comma-separated schema.table to include (optional)
STREAMBED_EXCLUDE_TABLES      Comma-separated schema.table to exclude (optional)
STREAMBED_LOG_LEVEL           ERROR, WARN, INFO, DEBUG (default: INFO)
```

---

## How to Build and Test

```bash
# Build
go build ./cmd/streambed

# Run tests
go test ./...

# Run locally against a Postgres + MinIO
docker run -p 9000:9000 minio/minio server /data
./streambed sync \
  --source-url postgres://user:pass@localhost:5432/mydb \
  --s3-bucket streambed \
  --s3-endpoint http://localhost:9000 \
  --s3-region us-east-1

# Validate output with DuckDB
duckdb -c "
  INSTALL iceberg; LOAD iceberg;
  SELECT COUNT(*) FROM iceberg_scan('s3://streambed/streambed/public/orders/');
"
```

---

## Key Context

- **Language:** Go. No Python, no scripting languages in the hot path.
- **Target users:** Engineers at companies running Postgres on managed services
  (RDS, Cloud SQL, Supabase, Neon) who want analytics without a data warehouse.
- **Key differentiator:** WAL-native CDC from day one (not full-table
  resync). No external catalog database. Single binary.
- **Key differentiator vs pg_mooncake / pg_lake:** Works on any existing
  Postgres, including all managed services. No extension install. No schema
  changes required.
- **License:** TBD (avoid AGPL — use Apache 2.0 or MIT for ecosystem friendliness)