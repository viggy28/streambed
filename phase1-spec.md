# Phase 1 Implementation Spec

**Goal:** A working `streambed sync` binary that connects to Postgres, reads the WAL, writes Parquet to S3, and commits Iceberg snapshots — verifiable with DuckDB.

**Duration:** Weeks 1–3
**Current Phase Gate:** This is Phase 1. No query layer, no UPDATE/DELETE, no psql-wire.

---

## Scope: What Phase 1 Does

1. Connects to a Postgres database as a logical replication client
2. Creates and manages a logical replication slot (`pgoutput` plugin)
3. Decodes WAL messages: `Relation`, `Begin`, `Insert`, `Commit`
4. Buffers rows per table in memory
5. Flushes to Parquet files on S3 (on row threshold OR time threshold)
6. Commits Iceberg snapshots using the filesystem catalog pattern
7. Tracks LSN watermarks in embedded SQLite
8. Provides a CLI via cobra: `streambed sync --source-url ... --s3-bucket ...`

## Scope: What Phase 1 Does NOT Do

- No UPDATE or DELETE handling (Phase 2)
- No schema change detection / ALTER TABLE (Phase 2)
- No query layer, no DuckDB, no psql-wire (Phase 3)
- No compaction (Phase 4)
- No parallel per-table goroutines (Phase 4)
- No Prometheus metrics (Phase 4)

---

## Architecture Decision: Iceberg, Not Delta Lake

The earlier spec.md references Delta Lake. This spec uses **Iceberg with the filesystem catalog** per streambed.md. Reasons:

- `apache/iceberg-go` is a first-party Apache project with active development
- No external catalog database required — S3 is the catalog via `version-hint.text`
- DuckDB has native `iceberg_scan()` support for verification
- Iceberg is the industry convergence point (Snowflake, Databricks, AWS all support it)

Delta Lake references in spec.md and context.md should be considered superseded.

---

## 1. CLI Entrypoint

### File: `cmd/streambed/main.go`

Single cobra command for Phase 1: `streambed sync`.

```
streambed sync \
  --source-url postgres://user:pass@host:5432/dbname \
  --s3-bucket my-bucket \
  --s3-prefix streambed/ \
  --s3-endpoint http://localhost:9000 \   # optional, for MinIO
  --s3-region us-east-1 \                 # optional
  --state-path ~/.streambed/state.db \    # optional
  --slot-name streambed \                 # optional
  --flush-rows 10000 \                    # optional
  --flush-interval 30s \                  # optional
  --include-tables public.orders,public.users \  # optional
  --exclude-tables public.sessions \             # optional
  --log-level INFO                        # optional
```

**Behavior:** Runs indefinitely until SIGINT/SIGTERM. On signal, flushes current buffer, commits snapshot, acks LSN, closes replication slot cleanly, exits 0.

---

## 2. Configuration

### File: `config/config.go`

```go
type Config struct {
    SourceURL        string        // required
    S3Bucket         string        // required
    S3Prefix         string        // default: "streambed/"
    S3Endpoint       string        // optional (MinIO)
    S3Region         string        // default: "us-east-1"
    StatePath        string        // default: "~/.streambed/state.db"
    SlotName         string        // default: "streambed"
    FlushRows        int           // default: 10000
    FlushInterval    time.Duration // default: 30s
    IncludeTables    []string      // optional filter
    ExcludeTables    []string      // optional filter
    LogLevel         string        // default: "INFO"
}
```

Resolution order: CLI flags > environment variables > defaults.

Environment variable names follow the `STREAMBED_` prefix convention documented in streambed.md.

---

## 3. WAL Consumer

### Files: `internal/wal/consumer.go`, `internal/wal/decoder.go`, `internal/wal/slot.go`

### 3.1 Replication Slot Management (`slot.go`)

```go
// CreateSlot creates a logical replication slot if it doesn't exist.
// Uses pgoutput plugin. Returns the consistent_point LSN.
func CreateSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) (pglogrepl.LSN, error)

// DropSlot drops the replication slot. Called on clean shutdown only if
// the slot was created by this process (not if it pre-existed).
func DropSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) error
```

**Slot creation behavior:**
- Check if slot exists first via `pg_replication_slots`
- If exists, reuse it (resume from last acked LSN)
- If not, create with `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput`
- Publication: create `streambed` publication if it doesn't exist, covering tables per include/exclude filters. If no filters, use `FOR ALL TABLES`.

**Note on source Postgres writes:** Creating the replication slot and publication are the ONLY writes to the source database. These are replication infrastructure, not application data. The slot is created once and reused across restarts.

### 3.2 WAL Decoder (`decoder.go`)

Decodes `pgoutput` protocol messages into typed events:

```go
// RelationMessage is emitted when Postgres sends a Relation message
// describing a table's schema. This happens before the first row
// for each table and whenever the schema changes.
type RelationMessage struct {
    RelationID uint32
    Namespace  string   // schema name (e.g., "public")
    Name       string   // table name (e.g., "orders")
    Columns    []Column
}

type Column struct {
    Name     string
    OID      uint32    // Postgres type OID
    Modifier int32     // type modifier (e.g., varchar length, numeric precision)
}

// InsertMessage represents a single INSERT decoded from the WAL.
type InsertMessage struct {
    RelationID uint32
    Row        []ColumnValue
}

type ColumnValue struct {
    Name   string
    OID    uint32
    Value  []byte   // raw text-format value from pgoutput
    IsNull bool
}

// Decode takes a raw pglogrepl.Message and returns one of:
// RelationMessage, InsertMessage, BeginMessage, CommitMessage, or nil for
// messages we don't handle in Phase 1 (Update, Delete, Truncate, Origin).
func Decode(msg pglogrepl.Message) (interface{}, error)
```

**Phase 1 message handling:**

| Message Type | Action |
|---|---|
| Relation | Store in relation map (RelationID → RelationMessage). Used to resolve column names/types for subsequent rows. |
| Begin | Record transaction begin LSN. No other action. |
| Insert | Resolve columns via relation map, emit InsertMessage to writer channel. |
| Commit | Record commit LSN. This is the LSN we'll eventually ack. |
| Update | Log warning, skip. Phase 2. |
| Delete | Log warning, skip. Phase 2. |
| Truncate | Log warning, skip. Phase 2. |
| Origin | Ignore. |
| Type | Ignore (custom types). |

### 3.3 Consumer Loop (`consumer.go`)

The main replication loop:

```go
type Consumer struct {
    conn       *pgconn.PgConn
    slotName   string
    relations  map[uint32]*RelationMessage  // relation cache
    startLSN   pglogrepl.LSN               // resume point
    events     chan<- RowEvent              // sends to writer
}

// RowEvent is sent from consumer to writer via channel.
type RowEvent struct {
    Schema    string
    Table     string
    Columns   []Column
    Values    []ColumnValue
    CommitLSN pglogrepl.LSN
}

// Start begins consuming WAL events. Blocks until ctx is cancelled.
// Sends RowEvents to the events channel.
// Periodically sends standby status updates to Postgres to keep
// the connection alive (every 10 seconds).
func (c *Consumer) Start(ctx context.Context) error
```

**Consumer loop pseudocode:**

```
1. Start replication: START_REPLICATION SLOT slotName LOGICAL startLSN
     (proto_version '1', publication_names 'streambed')
2. Loop:
   a. Receive message (with 10s timeout for standby keepalive)
   b. If timeout: send StandbyStatusUpdate with current flushed LSN
   c. If XLogData: decode the contained pgoutput message
      - Relation → update relation map
      - Insert → build RowEvent, send on channel
      - Begin/Commit → track LSN
   d. If ctx cancelled: return
```

**Standby status updates:** The consumer must send periodic standby status updates to Postgres to prevent the connection from being terminated. The `flushed_lsn` in the status update is the last LSN that has been durably written to S3 and committed as an Iceberg snapshot. This is received from the writer via a callback or channel.

---

## 4. Parquet Builder

### File: `internal/parquet/builder.go`

Converts a batch of rows into an in-memory Parquet file (as `[]byte`).

```go
// Builder creates Parquet files from row batches.
type Builder struct{}

// Build takes a schema and rows, returns a Parquet file as bytes.
// Uses Snappy compression. Single row group per file.
func (b *Builder) Build(columns []ColumnDef, rows [][]interface{}) ([]byte, error)

type ColumnDef struct {
    Name        string
    PgOID       uint32
    ParquetType parquet.Type  // mapped from PgOID
}
```

### Phase 1 Type Mapping (Postgres OID → Parquet)

Only the common types needed for Phase 1. Full mapping in Phase 2.

| Postgres Type | OID | Go Type | Parquet Type |
|---|---|---|---|
| bool | 16 | bool | BOOLEAN |
| int2 | 21 | int32 | INT32 |
| int4 | 23 | int32 | INT32 |
| int8 | 20 | int64 | INT64 |
| float4 | 700 | float32 | FLOAT |
| float8 | 701 | float64 | DOUBLE |
| text | 25 | string | BYTE_ARRAY (UTF8) |
| varchar | 1043 | string | BYTE_ARRAY (UTF8) |
| timestamp | 1114 | int64 | INT64 (TIMESTAMP_MICROS) |
| timestamptz | 1184 | int64 | INT64 (TIMESTAMP_MICROS) |
| date | 1082 | int32 | INT32 (DATE) |
| uuid | 2950 | string | BYTE_ARRAY (UTF8) |
| json | 114 | string | BYTE_ARRAY (UTF8) |
| jsonb | 3802 | string | BYTE_ARRAY (UTF8) |
| numeric | 1700 | string | BYTE_ARRAY (UTF8) |
| bytea | 17 | []byte | BYTE_ARRAY |

**Unsupported types in Phase 1:** Arrays, hstore, inet, interval, enums. These are stored as their text representation (BYTE_ARRAY UTF8) with a log warning on first encounter.

**Value parsing:** pgoutput sends column values in text format. The builder must parse these text representations into the correct Go types before writing to Parquet:
- `"t"` / `"f"` → bool
- `"12345"` → int32/int64
- `"3.14"` → float32/float64
- `"2024-01-15 10:30:00"` → timestamp micros since epoch
- `"2024-01-15"` → date days since epoch
- NULL → Parquet null

---

## 5. Iceberg Catalog

### Files: `internal/iceberg/catalog.go`, `internal/iceberg/schema.go`, `internal/iceberg/writer.go`

### 5.1 Filesystem Catalog (`catalog.go`)

Implements the Iceberg filesystem catalog pattern. No external catalog service.

**S3 layout per table:**

```
s3://<bucket>/<prefix>/<schema>/<table>/
    metadata/
        version-hint.text                   ← contains integer: current version
        v00001-<uuid>.metadata.json         ← Iceberg table metadata v1
        v00002-<uuid>.metadata.json         ← Iceberg table metadata v2 (after snapshot)
        snap-<snapshot-id>-<uuid>.avro      ← manifest list
        <uuid>-m0.avro                      ← manifest file
    data/
        <uuid>.parquet                      ← data files
```

```go
// Catalog manages Iceberg metadata on S3 using the filesystem catalog pattern.
type Catalog struct {
    storage *storage.S3Client
    bucket  string
    prefix  string
}

// CreateTable creates initial Iceberg metadata for a new table.
// Writes v00001 metadata JSON and version-hint.text.
func (c *Catalog) CreateTable(ctx context.Context, schema, table string, columns []ColumnDef) error

// CommitSnapshot appends a new snapshot to the table metadata.
// Steps (in order, all must succeed):
//   1. Read current version-hint.text → N
//   2. Read v<N> metadata JSON
//   3. Write new manifest file referencing the new data file
//   4. Write new manifest list referencing the manifest
//   5. Write v<N+1> metadata JSON with new snapshot
//   6. Write version-hint.text with N+1
// If step 6 fails, the orphaned metadata is invisible and harmless.
func (c *Catalog) CommitSnapshot(ctx context.Context, schema, table string, dataFile DataFile) error

// TableExists checks if version-hint.text exists for this table.
func (c *Catalog) TableExists(ctx context.Context, schema, table string) (bool, error)
```

**Invariant: Parquet write before Iceberg commit.** The data file must be durably written (PutObject returns success) before CommitSnapshot is called. The version-hint.text update is the very last step.

### 5.2 Schema Mapping (`schema.go`)

Maps Postgres column definitions to Iceberg schema types.

```go
// ToIcebergSchema converts Postgres columns to an Iceberg schema.
func ToIcebergSchema(columns []Column) iceberg.Schema

// PgOIDToIcebergType maps a single Postgres OID to its Iceberg type.
func PgOIDToIcebergType(oid uint32) iceberg.Type
```

Phase 1 Iceberg type mapping:

| Postgres OID | Iceberg Type |
|---|---|
| 16 (bool) | BooleanType |
| 21 (int2), 23 (int4) | IntegerType |
| 20 (int8) | LongType |
| 700 (float4) | FloatType |
| 701 (float8) | DoubleType |
| 25 (text), 1043 (varchar) | StringType |
| 1114 (timestamp) | TimestampType |
| 1184 (timestamptz) | TimestamptzType |
| 1082 (date) | DateType |
| 2950 (uuid) | UUIDType |
| 114 (json), 3802 (jsonb) | StringType |
| 1700 (numeric) | StringType (Phase 1 simplification; DecimalType in Phase 2) |
| 17 (bytea) | BinaryType |
| everything else | StringType (with warning) |

### 5.3 Writer (`writer.go`)

Receives RowEvents from the WAL consumer, buffers them, and flushes to S3 + Iceberg.

```go
type Writer struct {
    catalog     *Catalog
    parquet     *parquet.Builder
    storage     *storage.S3Client
    state       *state.Store
    flushRows   int
    flushInterval time.Duration

    // Per-table buffers
    buffers     map[string]*TableBuffer  // key: "schema.table"
    schemas     map[string][]Column      // key: "schema.table"
}

type TableBuffer struct {
    Schema    string
    Table     string
    Columns   []Column
    Rows      [][]ColumnValue
    FirstLSN  pglogrepl.LSN
    LastLSN   pglogrepl.LSN
}

// Start reads from the events channel and buffers rows.
// Flushes when either condition is met:
//   - Any table's buffer reaches flushRows
//   - flushInterval has elapsed since last flush
// Returns the last flushed LSN via the ackCh channel.
func (w *Writer) Start(ctx context.Context, events <-chan RowEvent, ackCh chan<- pglogrepl.LSN) error
```

**Flush sequence (per table with buffered rows):**

```
1. Convert buffered rows to Parquet bytes (parquet.Builder)
2. Generate data file path: data/<uuid>.parquet
3. Upload Parquet to S3: s3://<bucket>/<prefix>/<schema>/<table>/data/<uuid>.parquet
4. If table not yet in Iceberg catalog: CreateTable
5. CommitSnapshot with reference to the new data file
6. Record flushed LSN in SQLite state store
7. Send flushed LSN on ackCh (consumer uses this for standby status)
8. Clear the table buffer
```

**Flush trigger logic:**

```go
// In the Start loop:
ticker := time.NewTicker(w.flushInterval)
for {
    select {
    case event := <-events:
        w.buffer(event)
        if w.buffers[key].Rows >= w.flushRows {
            w.flush(ctx, key)
        }
    case <-ticker.C:
        w.flushAll(ctx)  // flush all non-empty buffers
    case <-ctx.Done():
        w.flushAll(ctx)  // final flush on shutdown
        return
    }
}
```

---

## 6. State Store

### File: `internal/state/store.go`

Embedded SQLite for Streambed's own bookkeeping.

```go
type Store struct {
    db *sql.DB
}

// Open opens or creates the SQLite database at the given path.
// Creates tables if they don't exist.
func Open(path string) (*Store, error)
```

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS replication_state (
    slot_name    TEXT PRIMARY KEY,
    flushed_lsn  TEXT NOT NULL,     -- hex string, e.g., "0/1234ABCD"
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS synced_tables (
    schema_name  TEXT NOT NULL,
    table_name   TEXT NOT NULL,
    column_count INTEGER NOT NULL,
    first_seen   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_flush   TIMESTAMP,
    PRIMARY KEY (schema_name, table_name)
);
```

```go
// GetFlushedLSN returns the last flushed LSN for the given slot.
// Returns 0 if no record exists (fresh start).
func (s *Store) GetFlushedLSN(slotName string) (pglogrepl.LSN, error)

// SetFlushedLSN records the last flushed LSN.
func (s *Store) SetFlushedLSN(slotName string, lsn pglogrepl.LSN) error

// RegisterTable records a table being synced.
func (s *Store) RegisterTable(schema, table string, columnCount int) error

// UpdateLastFlush updates the last flush timestamp for a table.
func (s *Store) UpdateLastFlush(schema, table string) error
```

---

## 7. S3 Storage

### File: `internal/storage/s3.go`

Thin wrapper around `aws-sdk-go-v2`.

```go
type S3Client struct {
    client *s3.Client
    bucket string
}

// NewS3Client creates an S3 client. If endpoint is non-empty, uses it
// as a custom endpoint (for MinIO, LocalStack, etc.).
func NewS3Client(ctx context.Context, bucket, region, endpoint string) (*S3Client, error)

// PutObject uploads bytes to S3. Returns error if the upload fails.
func (s *S3Client) PutObject(ctx context.Context, key string, data []byte, contentType string) error

// GetObject downloads an object from S3.
func (s *S3Client) GetObject(ctx context.Context, key string) ([]byte, error)

// HeadObject checks if an object exists. Returns false if 404.
func (s *S3Client) HeadObject(ctx context.Context, key string) (bool, error)
```

No multipart upload in Phase 1 (individual Parquet files from 10k-row batches will be well under 5GB).

---

## 8. Graceful Shutdown

On SIGINT or SIGTERM:

1. Cancel the context passed to Consumer and Writer
2. Consumer stops reading WAL, returns from Start
3. Writer flushes all non-empty buffers (final flush)
4. Writer commits any pending Iceberg snapshots
5. State store records final flushed LSN
6. Consumer sends final standby status update with flushed LSN
7. Replication connection closed (slot is NOT dropped — it persists for resume)
8. SQLite connection closed
9. Process exits 0

The replication slot is intentionally kept alive across restarts. On next `streambed sync`, the consumer resumes from the last acked LSN stored in SQLite.

---

## 9. Include/Exclude Table Filtering

- `--include-tables public.orders,public.users` → only sync these tables
- `--exclude-tables public.sessions` → sync all tables except these
- Cannot use both simultaneously (error on startup)
- If neither is specified, sync all tables via `FOR ALL TABLES` publication
- Filtering is applied at publication level (Postgres only sends WAL for published tables)
- Relation messages for non-published tables are ignored

---

## 10. Logging

Structured logging via Go's `slog` package (stdlib, no external dependency).

Log levels: ERROR, WARN, INFO, DEBUG.

**Key log events:**

| Event | Level | Data |
|---|---|---|
| Replication slot created | INFO | slot_name, consistent_point |
| Replication started | INFO | slot_name, start_lsn |
| New table discovered | INFO | schema, table, column_count |
| Flush completed | INFO | schema, table, row_count, parquet_bytes, duration_ms |
| Iceberg snapshot committed | INFO | schema, table, version, data_file |
| LSN advanced | DEBUG | slot_name, old_lsn, new_lsn |
| Standby status sent | DEBUG | flushed_lsn |
| Unsupported WAL message | WARN | message_type |
| Unsupported column type | WARN | oid, column_name, table, fallback_type |
| S3 upload failed | ERROR | key, error |
| Replication error | ERROR | error |
| Shutdown initiated | INFO | signal |
| Final flush completed | INFO | tables_flushed, total_rows |

---

## 11. Error Handling

### Transient errors (retry with backoff)
- S3 upload failures → retry 3x with exponential backoff (1s, 2s, 4s)
- Postgres connection drops → reconnect and resume from last flushed LSN
- Network timeouts → same as connection drops

### Fatal errors (log and exit)
- Invalid source-url (can't connect to Postgres at all)
- S3 bucket doesn't exist or credentials are invalid
- SQLite state file can't be created (bad path, permissions)
- Replication slot creation denied (insufficient privileges)

### Invariant violations (panic)
- Attempt to ack LSN before flush (programming error, not runtime)
- version-hint.text update before Parquet write completes

---

## 12. Testing Strategy

### Unit Tests

| Package | What to Test |
|---|---|
| `config` | Default values, env var override, CLI flag override, validation |
| `internal/wal` | Decoder: parse each pgoutput message type correctly |
| `internal/parquet` | Builder: round-trip (build Parquet → read back with DuckDB, verify values) |
| `internal/iceberg` | Schema mapping: every OID → correct Iceberg type |
| `internal/state` | SQLite CRUD: set/get LSN, register table, update flush time |
| `internal/storage` | S3 client: mock/interface based (actual S3 tested in integration) |

### Integration Tests (require Docker)

| Test | Setup | Verification |
|---|---|---|
| End-to-end INSERT sync | Postgres + MinIO in Docker Compose | Insert 100 rows → run streambed sync for 10s → stop → DuckDB `iceberg_scan` returns 100 rows |
| Resume from LSN | Same as above | Insert 50 rows → sync → stop → insert 50 more → sync → verify 100 total (no duplicates) |
| Multi-table sync | 3 tables with different schemas | All 3 appear as separate Iceberg tables on S3 |
| Table filtering | `--include-tables public.orders` | Only orders table appears on S3, others absent |
| Graceful shutdown | SIGINT during active sync | Final buffer flushed, LSN recorded, no data loss |

### Docker Compose for Testing

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: test
    command: >
      postgres
        -c wal_level=logical
        -c max_replication_slots=4
        -c max_wal_senders=4
    ports:
      - "5432:5432"

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
```

---

## 13. Go Module Dependencies

```
github.com/jackc/pglogrepl       # WAL consumer, pgoutput decoder
github.com/jackc/pgx/v5          # Postgres driver (used by pglogrepl)
github.com/parquet-go/parquet-go  # Parquet file writing (simpler API than arrow-go, no Arrow dep)
github.com/aws/aws-sdk-go-v2     # S3 client
github.com/mattn/go-sqlite3      # SQLite (CGO required)
github.com/spf13/cobra           # CLI framework
github.com/google/uuid           # UUID generation for file names
```

**Not using `apache/iceberg-go`** — it does not support the filesystem catalog pattern.
Iceberg metadata JSON is written directly by our `internal/iceberg/` package.

**Not using `apache/arrow-go`** — heavier API (~30 lines vs ~7 for parquet-go), requires
Arrow dependency. parquet-go is sufficient for write-only Parquet generation.

---

## 14. Implementation Order

Build and test each layer bottom-up. Each step should have passing tests before moving to the next.

### Step 1: Project scaffold
- `go mod init github.com/viggy28/streambed`
- Directory structure: `cmd/streambed/`, `config/`, `internal/{wal,iceberg,parquet,state,storage}/`
- Add all dependencies to `go.mod`
- Skeleton `main.go` with cobra root + sync command that prints config and exits

### Step 2: Config
- `config/config.go` — parse env vars + CLI flags
- Tests: defaults, overrides, validation

### Step 3: State store
- `internal/state/store.go` — SQLite open, schema creation, LSN get/set, table registry
- Tests: CRUD operations, fresh start returns zero LSN

### Step 4: S3 storage
- `internal/storage/s3.go` — PutObject, GetObject, HeadObject
- Tests: interface-based unit tests; integration test against MinIO

### Step 5: Parquet builder
- `internal/parquet/builder.go` — type mapping, value parsing, Parquet generation
- Tests: build a Parquet file, read it back, verify schema and values

### Step 6: Iceberg catalog
- `internal/iceberg/catalog.go` — CreateTable, CommitSnapshot, version-hint management
- `internal/iceberg/schema.go` — OID to Iceberg type mapping
- Tests: create table metadata on MinIO, commit snapshot, verify with DuckDB `iceberg_scan`

### Step 7: WAL decoder
- `internal/wal/decoder.go` — parse pgoutput messages into typed events
- Tests: decode each message type from known byte sequences

### Step 8: WAL consumer + slot management
- `internal/wal/slot.go` — create/check/drop slot, create publication
- `internal/wal/consumer.go` — replication loop, standby keepalive
- Tests: integration test against Postgres with `wal_level=logical`

### Step 9: Writer (glue layer)
- `internal/iceberg/writer.go` — buffer management, flush triggers, orchestration
- Tests: mock consumer → writer → verify S3 output

### Step 10: End-to-end integration
- Wire everything together in `cmd/streambed/main.go`
- Docker Compose integration test: INSERT rows → sync → DuckDB verification
- Graceful shutdown test
- Resume-from-LSN test

---

## 15. End-of-Phase-1 Acceptance Criteria

All of the following must pass:

```bash
# 1. Build succeeds
go build ./cmd/streambed

# 2. Unit tests pass
go test ./...

# 3. End-to-end test (Docker Compose with Postgres + MinIO)

# Setup: create table and insert rows
psql -h localhost -U postgres -c "
  CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer TEXT NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
  INSERT INTO orders (customer, amount)
  SELECT 'customer_' || i, (random() * 1000)::numeric(10,2)
  FROM generate_series(1, 1000) AS i;
"

# Run streambed sync for 15 seconds
timeout 15 ./streambed sync \
  --source-url postgres://postgres:test@localhost:5432/postgres \
  --s3-bucket streambed \
  --s3-prefix test/ \
  --s3-endpoint http://localhost:9000 \
  --s3-region us-east-1

# Verify with DuckDB
duckdb -c "
  INSTALL iceberg; LOAD iceberg;
  SELECT COUNT(*) AS row_count
  FROM iceberg_scan('s3://streambed/test/public/orders/');
"
# Expected output: row_count = 1000

# 4. Resume test: insert more rows, sync again, verify total
psql -h localhost -U postgres -c "
  INSERT INTO orders (customer, amount)
  SELECT 'customer_' || i, (random() * 1000)::numeric(10,2)
  FROM generate_series(1001, 2000) AS i;
"

timeout 15 ./streambed sync \
  --source-url postgres://postgres:test@localhost:5432/postgres \
  --s3-bucket streambed \
  --s3-prefix test/ \
  --s3-endpoint http://localhost:9000 \
  --s3-region us-east-1

duckdb -c "
  INSTALL iceberg; LOAD iceberg;
  SELECT COUNT(*) AS row_count
  FROM iceberg_scan('s3://streambed/test/public/orders/');
"
# Expected output: row_count = 2000
```

---

## 16. Resolved Questions

1. **`apache/iceberg-go`:** Does NOT support filesystem catalog. We write Iceberg metadata JSON directly to S3 ourselves. The format is well-documented: metadata JSON, manifest lists (Avro), manifest files (Avro), and `version-hint.text`. DuckDB's `iceberg_scan()` reads this natively.

2. **Parquet library:** Using `parquet-go/parquet-go`. Simpler API (~7 lines vs ~30), no Arrow dependency, native Go struct support. Since our schemas are dynamic (discovered from WAL), we use `parquet.GenericWriter` with runtime schema construction rather than struct tags.

3. **Publication scope:** Default to `FOR ALL TABLES`. Include/exclude flags available from day 1 but optional. `--include-tables` creates a scoped publication. `--exclude-tables` uses `FOR ALL TABLES` and filters at the decoder level (Postgres publications don't support exclude natively).

4. **Numeric precision:** Store as UTF8 string in Phase 1. DuckDB casts string to numeric transparently (`SELECT CAST('123.45' AS DECIMAL(10,2))`). Proper DECIMAL Parquet encoding added in Phase 2 with the full type mapping pass.

5. **Initial table snapshot (Phase 2):** Logical replication only sends changes after slot creation. Existing rows are NOT captured in Phase 1. This is a documented limitation.

   **Planned approach for Phase 2 backfill:**
   ```
   1. CREATE_REPLICATION_SLOT → returns consistent_point LSN + snapshot_name
   2. Open transaction: SET TRANSACTION SNAPSHOT '<snapshot_name>'
   3. For each table: COPY <table> TO STDOUT (within snapshot transaction)
   4. Write Parquet files + Iceberg commits for snapshot data
   5. Close snapshot transaction
   6. Start WAL streaming from consistent_point LSN
   ```
   The slot's snapshot represents the exact point-in-time before WAL streaming begins — zero duplicates, zero gaps. This is the same approach PeerDB uses (with optional CTID-based parallelism for large tables). BemiDB uses a simpler `COPY TO STDOUT WITH CSV` but without the snapshot-to-CDC transition (they only support full refresh today, no incremental CDC).
