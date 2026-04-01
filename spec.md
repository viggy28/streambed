# streambed — Analytics Query Engine
### Technical Specification: Analytics Query Layer
**Version 0.1 · March 2026 · Confidential**

---

## 1. The Problem

### 1.1 Postgres Is the Source of Truth — and That Is the Problem

For most SaaS companies, Postgres is the operational database. It stores every user, order, event, and transaction. It is reliable, well-understood, and the right choice for transactional workloads.

The problem emerges the moment someone asks an analytical question:

```sql
SELECT country, COUNT(*), AVG(order_total), SUM(revenue)
FROM orders
WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY country
ORDER BY revenue DESC;
```

This is not an unusual query. It is the kind of question every business needs to answer. But on Postgres, it causes a full sequential scan of the orders table — reading every row, every column, on a storage engine designed for row-level random access, not bulk column aggregation.

On a table with 50 million rows, this query takes minutes. During that time, it consumes buffer pool pages and I/O bandwidth that app traffic depends on. On a shared read replica, a single such query can degrade response times for every concurrent user.

---

### 1.2 The Two Attempted Solutions (and Why They Fail)

#### Solution A: Dedicated Analytics Read Replica

Teams spin up a Postgres read replica specifically for analytics traffic. This works briefly and fails for predictable reasons:

- Row-oriented storage is fundamentally wrong for column aggregations. A replica of Postgres is still Postgres — more of the wrong architecture does not fix the architecture.
- Replicas are full RDS/Aurora instances: EBS-backed compute at $300–$800/month each, running 24/7 regardless of query volume.
- Managed providers (RDS, Aurora) cap replica counts. Horizontal scaling past that cap is not possible.
- Long-running analytical queries still block each other on a shared replica. Workload isolation requires a separate instance per team.

#### Solution B: A Data Warehouse (Snowflake, Redshift, BigQuery)

The default enterprise path: stand up a data warehouse, build an ETL pipeline from Postgres to the warehouse, route analytics there. This works technically. The cost is severe:

- Snowflake costs $2,000–$50,000+/month at scale. Bill overruns of 200–300% are commonly reported by engineering teams.
- An ETL pipeline requires a dedicated data engineer to build and maintain. Schema changes in Postgres silently break the pipeline.
- Data lives in a proprietary storage format inside a vendor's cloud. Migrating away is expensive and slow.
- You pay per-query compute against a copy of data you already own on infrastructure you are already paying for.

> **Root Cause:** Postgres couples storage and compute. Row-oriented storage on local EBS is optimal for OLTP but wrong for analytical column scans. The solution is not more Postgres — it is to separate storage from compute and use the right query engine for each workload type.

---

### 1.3 What Analytics Workloads Actually Look Like

| Query Pattern | Example | Why Postgres Struggles |
|---|---|---|
| Aggregations over large tables | SUM, COUNT, AVG across millions of rows | Full sequential scan, row-by-row iteration |
| Time-series rollups | GROUP BY day/week/month over event logs | Reads all columns to extract one date column |
| Multi-table joins for reporting | Orders JOIN users JOIN products | Hash joins spill to disk, no columnar optimization |
| BI tool dashboards | Metabase/Grafana polling every 5 min | Repeated expensive scans, no result cache |
| dbt transformation jobs | Hourly aggregate table builds | Competes with app traffic on shared storage |
| Ad-hoc analyst queries | Exploratory GROUP BY over full history | Unpredictable resource consumption |
| Data exports | SELECT * FROM orders WHERE month = 'Jan' | Large result sets hold connections and buffer pool |

These workloads share a common structure: they read many rows but only a few columns, they aggregate rather than look up individual records, and they tolerate seconds of latency. They are precisely the workload class that columnar storage and vectorized execution were designed for.

---

## 2. Solution Overview

### 2.1 Core Principle: Right Engine for the Right Workload

The Postgres primary is untouched. It continues to handle all writes, all OLTP reads, and all user-facing transactional queries. streambed is purely additive — a second query endpoint that specializes in the queries Postgres is bad at.

| Workload | Engine | Why |
|---|---|---|
| App writes (INSERT/UPDATE/DELETE) | Postgres primary | ACID, indexes, constraints — unchanged |
| OLTP reads (id lookups, user-facing) | Postgres primary or replica | B-tree index, <1ms latency required |
| Analytics, reports, BI dashboards | streambed (DuckDB + S3) | Columnar, vectorized, no impact on primary |
| Long-running aggregations | streambed (DuckDB + S3) | Workload isolation, no buffer pool contention |
| dbt / ETL pipelines | streambed (DuckDB + S3) | S3 scan is cheap, no write amplification on primary |

---

### 2.2 System Architecture

```
+----------------------------------------------------------+
|               POSTGRES PRIMARY                           |
|       (untouched -- writes + OLTP reads)                 |
+------------------------+---------------------------------+
                         |
              WAL / logical replication
                (pgoutput plugin)
                         |
                         v
+----------------------------------------------------------+
|               WAL CONSUMER (Go)                          |
|  pglogrepl + pgx/v5                                      |
|  Decodes INSERT / UPDATE / DELETE events                 |
|  Tracks LSN for crash recovery                           |
+------------------------+---------------------------------+
                         |
           Parquet writes + Delta Lake commits
                         |
                         v
+----------------------------------------------------------+
|               S3 -- DELTA LAKE TABLES                   |
|                                                          |
|  orders/_delta_log/00000000000000000001.json  <- commit  |
|  orders/part-001.parquet                      <- data    |
|  orders/part-delete-003.parquet               <- deletes |
+------------------------+---------------------------------+
                         |
         DuckDB reads Delta Lake natively
                         |
                         v
+----------------------------------------------------------+
|          DUCKDB QUERY ENGINE (go-duckdb CGo)             |
|  Vectorized columnar execution                           |
|  Parallel Parquet scan with predicate pushdown           |
+------------------------+---------------------------------+
                         |
         Postgres wire protocol (psql-wire)
                         |
                         v
+----------------------------------------------------------+
|       ANALYTICS CLIENTS (no changes required)            |
|  Metabase  Grafana  Superset  dbt  psql  Python          |
+----------------------------------------------------------+
```

---

## 3. Analytics Query Layer: Technical Specification

### 3.1 Query Engine: DuckDB

DuckDB is an embedded OLAP query engine designed for in-process analytical workloads. It is the execution engine at the core of streambed's query layer.

**Why DuckDB:**

- **Vectorized execution:** processes columns in batches using CPU SIMD instructions rather than row-by-row iteration. Typical speedup: 10–100x vs Postgres for analytical workloads.
- **Native Parquet and Delta Lake reader:** DuckDB reads from S3 directly without a separate data loading step.
- **Full SQL support:** window functions, CTEs, lateral joins, UNNEST, ROLLUP, CUBE, GROUPING SETS. Approximately 90% compatibility with Postgres analytical SQL.
- **In-process embedding via go-duckdb (CGo):** query execution runs in the same process as the WAL consumer with no IPC overhead.
- **Automatic parallelism:** DuckDB partitions Parquet files across CPU cores. A 12-core machine runs 12-way parallel scans with no configuration.

#### Vectorized Execution

Standard row-by-row execution in Postgres:

```python
for each row in orders:
    if row.country == 'US':
        total += row.revenue    # one operation per row
```

DuckDB vectorized execution:

```python
revenue  = load_column('revenue')       # entire column as array
country  = load_column('country')       # entire column as array
mask     = country == 'US'              # SIMD: compare whole array
result   = sum(revenue[mask])           # SIMD: sum in one pass
```

SIMD instructions operate on 4–16 values simultaneously using the CPU's vector registers. The per-row loop disappears. For 100 million rows, this is the difference between 60 seconds and 2 seconds on the same hardware.

#### Predicate Pushdown and File Pruning

DuckDB integrates with the Delta Lake transaction log to prune Parquet files before reading them. Each commit entry records per-column min/max statistics for every Parquet file. For a query with a WHERE clause, DuckDB:

1. Reads the Delta Lake log to enumerate all active Parquet files
2. Checks each file's column statistics against the WHERE predicate
3. Skips files whose statistics cannot satisfy the predicate — without opening them
4. Opens only surviving files; reads only the projected columns

**Example:** `WHERE created_at > '2024-06-01'` on a 500-file table. Files with `max(created_at)` before June 1 are skipped entirely. In a typical deployment this eliminates 80–90% of files for date-range queries.

> **Performance Target:** Analytical aggregation over 100M rows: < 5 seconds. Date-range predicate pushdown should eliminate 80%+ of Parquet files for typical time-series queries. BI dashboard queries repeated every 5 minutes should be served from result cache after the first execution.

---

### 3.2 Storage Layer: Delta Lake on S3

#### Why Delta Lake Over Raw Parquet

| Requirement | Raw Parquet | Delta Lake |
|---|---|---|
| Track which files are current | No — must scan all files | Yes — transaction log tracks active set |
| Handle UPDATE/DELETE from WAL | No — files are immutable | Yes — delete files + new data files |
| Concurrent writers without corruption | No — last write wins | Yes — atomic log entry serialization |
| Schema evolution (new columns) | No — all files must match schema | Yes — metadata-only change in log |
| Time travel (query as-of yesterday) | No | Yes — query any past snapshot |
| Per-file column statistics for pruning | No | Yes — stored in each commit entry |

#### Handling Postgres Mutations

Parquet files are immutable — a row cannot be edited in place. streambed handles all Postgres DML:

- **INSERT:** buffer rows, flush to new Parquet file, write add commit entry with column statistics.
- **DELETE:** write a small delete file containing the primary keys of deleted rows. Delta Lake marks those rows as logically deleted. Physical removal happens during compaction.
- **UPDATE:** treated as DELETE of the old row followed by INSERT of the new row. Old row enters a delete file; new row enters a new data file. *Merge-on-read*: cheap at write time, cleaned up at compaction.

#### Delta Lake Transaction Log Structure

```
s3://customer-bucket/streambed/public.orders/
  _delta_log/
    00000000000000000000.json   # table creation + schema
    00000000000000000001.json   # add: part-001.parquet (1000 rows)
    00000000000000000002.json   # add: part-002.parquet, remove: part-001.parquet
    00000000000000000010.json   # checkpoint (every 10 commits)
  part-001.parquet              # data files
  part-002.parquet
  part-delete-003.parquet       # delete file: marks rows deleted for UPDATEs
```

#### Compaction

Many small Parquet files accumulate over time — one per WAL flush batch. Small files increase S3 GET overhead per query. A background compaction job runs on a configurable schedule (default: every 4 hours):

- Reads all active small files for a table partition
- Merges into fewer larger files (target: 128MB–512MB per file)
- Applies pending deletes (rewrites merged file without deleted rows)
- Writes a new commit entry: remove old files, add merged files
- Old files retained for the time-travel retention window (default: 7 days), then vacuumed

---

### 3.3 Postgres Wire Protocol Interface

#### Client Connectivity

streambed runs a psql-wire server implementing the Postgres frontend/backend wire protocol v3. Clients connect identically to a Postgres database:

```bash
psql -h streambed.example.com -p 5433 -U analytics -d mydb

# Standard Postgres connection string — no driver changes
postgresql://analytics:token@streambed.example.com:5433/mydb
```

No driver changes are required. Metabase, Grafana, Superset, dbt, SQLAlchemy, psycopg2, pgx — all connect and execute queries without modification.

#### Query Execution Flow

```
Client              psql-wire           DuckDB             S3
  |                     |                  |                |
  |-- Query message --> |                  |                |
  |                     |-- Execute SQL --> |                |
  |                     |                  |-- Read log --> |
  |                     |                  |<-- file list --|
  |                     |                  |-- GET parquet >|
  |                     |                  |<-- col data ---|
  |                     |                  | (vectorized)   |
  |                     |<-- result rows --|                |
  |<-- DataRow msgs ----|                  |                |
  |<-- CommandComplete -|                  |                |
```

#### SQL Compatibility

DuckDB supports approximately 90% of Postgres analytical SQL.

**Fully supported:**
- Aggregates: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `STDDEV`, `PERCENTILE_CONT`, `PERCENTILE_DISC`
- Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTILE`
- Common Table Expressions (`WITH`, `WITH RECURSIVE`)
- `LATERAL` joins, `CROSS JOIN LATERAL`
- `UNNEST` for array columns
- JSON path queries (`json_extract_path`, `->>` operator, `json_array_elements`)
- Date/time: `DATE_TRUNC`, `DATE_PART`, `EXTRACT`, `AT TIME ZONE`, date arithmetic
- `ROLLUP`, `CUBE`, `GROUPING SETS`

**Documented incompatibilities (communicated upfront):**
- Read-only endpoint — no `INSERT`, `UPDATE`, `DELETE`, `DDL`
- No stored procedures or PL/pgSQL
- No row-level security policies
- Some Postgres system functions absent (`pg_get_expr`, advisory locks, `lo_*` functions)

---

### 3.4 Replication Lag and Freshness

| Mode | Typical Lag | Best For |
|---|---|---|
| Micro-batch (default) | 2–10 seconds | BI dashboards refreshing every minute or more |
| Low-latency mode | < 2 seconds | Near-real-time reporting requirements |
| Batch mode | 1–5 minutes | Overnight dbt runs, maximum cost optimization |

Replication lag is visible via a system table:

```sql
SELECT * FROM streambed.replication_status;
-- source_lsn | replicated_lsn | lag_bytes | lag_seconds
```

> **Scope Boundary:** streambed does not serve user-facing reads that require the latest committed state (e.g., "show the order I just placed"). Those queries go to the Postgres primary. streambed targets analytical queries where seconds of lag are invisible to end users — identical to the behavior of any data warehouse.

---

## 4. Data Model and Type Mapping

### 4.1 Schema Discovery

streambed discovers table schemas automatically from the WAL stream. When logical replication starts, Postgres sends a Relation message for each replicated table containing column names, types, and OIDs. streambed uses this to create the corresponding Delta Lake schema before the first row arrives.

Schema changes (`ALTER TABLE ADD COLUMN`, `RENAME COLUMN`) arrive as new Relation messages. streambed detects these, flushes the current write buffer, applies the schema change to the Delta Lake metadata, and continues without restart.

### 4.2 Postgres to Parquet Type Mapping

| Postgres Type | Arrow / Parquet Type | Notes |
|---|---|---|
| `integer` | `Int32` | Direct mapping |
| `bigint` | `Int64` | Direct mapping |
| `smallint` | `Int16` | Direct mapping |
| `float4 / real` | `Float32` | Direct mapping |
| `float8 / double precision` | `Float64` | Direct mapping |
| `numeric / decimal` | `Decimal128(p, s)` | Precision and scale preserved |
| `text / varchar / char` | `Utf8` | Direct mapping |
| `boolean` | `Boolean` | Direct mapping |
| `timestamp` | `Timestamp(us, UTC)` | Normalized to UTC |
| `timestamptz` | `Timestamp(us, UTC)` | UTC offset preserved |
| `date` | `Date32` | Days since Unix epoch |
| `time / timetz` | `Time64(us)` | Microsecond precision |
| `uuid` | `Utf8` | Canonical lowercase string |
| `json / jsonb` | `Utf8` | JSON string; queryable via DuckDB `json_` functions |
| `bytea` | `LargeBinary` | Raw bytes |
| `integer[]` | `List<Int32>` | Recursive element type mapping |
| `text[]` | `List<Utf8>` | Recursive element type mapping |
| `hstore` | `Utf8` | Serialized as JSON string |
| `enum` | `Utf8` | String value of the enum label |
| `inet / cidr` | `Utf8` | String representation |
| `interval` | `Duration(us)` | Microsecond duration |

---

## 5. Write Path Specification

### 5.1 WAL Consumer

The WAL consumer is a Go process that connects to Postgres as a logical replication client using the `pgoutput` plugin:

- Opens a replication slot on the primary (or a dedicated physical standby to protect primary I/O)
- Streams WAL events: `Relation`, `Begin`, `Insert`, `Update`, `Delete`, `Commit`
- Decodes events via `pglogrepl` into structured row change events with full column values
- Buffers row changes in memory until a flush threshold is reached (default: 1,000 rows or 5 seconds)
- Flushes buffered rows to Parquet, writes Delta Lake commit, acknowledges LSN back to Postgres

**LSN acknowledgement is the crash-safety guarantee:** streambed only acknowledges an LSN after data has been durably written to S3 and committed to the Delta Lake log. On restart, the consumer resumes from the last acknowledged LSN with no data loss.

### 5.2 Parquet Write Strategy

Each flush produces one Parquet file per table that received changes in the flush window. Files are written with row groups targeting 128MB for optimal DuckDB read granularity.

Column encoding strategy:
- **Dictionary encoding:** low-cardinality string columns (status fields, country codes, enum-like values)
- **Delta encoding:** monotonically increasing integers and timestamps (primary keys, `created_at`)
- **Snappy compression:** all columns (fast decompression with 3–5x compression ratio vs raw data)

### 5.3 Crash Safety

- LSN position persisted to a state store (configurable: Postgres table or S3 key) after each successful S3 write + Delta Lake commit
- On restart: read last persisted LSN, request WAL replay from that point — exactly-once delivery via LSN deduplication
- Partial S3 write without Delta Lake commit: orphaned Parquet file is invisible to all readers (commit was never written); cleaned up during next vacuum run
- Replication slot prevents Postgres from discarding unacknowledged WAL segments; monitored for bloat

---

## 6. Query Optimizations

### 6.1 Partition Pruning

Tables are partitioned by a configurable time column (default: `created_at` or `updated_at`) during compaction. DuckDB uses per-partition min/max statistics from the Delta Lake log to skip partitions that cannot satisfy a date-range predicate. For typical business analytics queries filtering to the last 30 days, this eliminates 90%+ of historical files from the scan path.

### 6.2 Projection Pushdown

DuckDB reads only the columns referenced in a query. Parquet stores each column independently — reading 3 columns from a 50-column table reads approximately 6% of the total stored bytes. This operates automatically without any query hints.

### 6.3 Result Caching

Frequently-executed queries (identified by query fingerprint hash) are cached in DuckDB's in-memory result cache. BI dashboard queries that run every 5 minutes on the same time range are served from cache after the first execution. Cache entries are invalidated when a new Delta Lake commit touches the underlying tables.

### 6.4 Parallel S3 Reads

DuckDB issues parallel S3 GET requests for independent Parquet files, saturating available network bandwidth. For a 12-core host, up to 12 Parquet files are read concurrently. Scan performance scales with both CPU count and network bandwidth rather than being bottlenecked by single-threaded I/O.

---

## 7. Operational Concerns

| Risk | Severity | Mitigation |
|---|---|---|
| WAL slot bloat if consumer falls behind | High | Monitor lag; alert at 1GB unacked WAL; auto-pause slot at critical threshold |
| Schema changes breaking pipeline | Medium | Detect Relation messages; flush before applying; schema evolution in Delta Lake log |
| DuckDB SQL compatibility gaps | Medium | Document the 10% upfront; position as analytical-only endpoint |
| Small file accumulation (high S3 GET cost) | Medium | Compaction job every 4 hours; target 128MB+ files |
| delta-go library immaturity | Low | Proven write patterns; migration path to Iceberg when Go library matures |
| CGo complexity in go-duckdb | Low | Overhead is microseconds vs seconds of query time |
| Replication lag surprises | Low | Document explicitly; expose `streambed.replication_status` system table |

### 7.1 WAL Slot Bloat Detail

Postgres replication slots prevent WAL recycling until the consumer acknowledges. If streambed falls behind (S3 write latency, network partition, consumer crash), unbounded WAL accumulation can fill the primary's disk. Mitigations:

- Continuous monitoring of `pg_replication_slots.confirmed_flush_lsn` vs current WAL position
- Alert at configurable lag threshold (default: 1GB unacknowledged WAL)
- Auto-pause: if lag exceeds a critical threshold, drop the replication slot temporarily to protect the primary; consumer resumes from checkpoint on recovery
- Recommended production setup: use a physical standby as the replication source, fully isolating the primary from streambed's replication load

### 7.2 S3 Cost Estimates

| Resource | Estimate | Basis |
|---|---|---|
| Storage (compressed Parquet) | ~$4.60/month per TB of Postgres data | 1TB Postgres → ~200GB Parquet (5x compression) → $0.023/GB-month |
| S3 GET requests (post-compaction) | ~$0.50–$2.00/month per TB | 128MB files, typical query fan-out after pruning |
| S3 PUT requests (writes) | ~$0.10–$0.50/month per TB | Flush batches + compaction rewrites |
| Egress | Varies by region | DuckDB reads S3 in same region; minimize cross-region placement |

---

## 8. Go Technology Stack

| Component | Library | Purpose |
|---|---|---|
| WAL consumer | `jackc/pglogrepl` + `pgx/v5` | Logical replication decode and Postgres connection management |
| Parquet writes | `parquet-go/parquet-go` | Columnar Parquet file generation with encoding and compression |
| Delta Lake | `rivian/delta-go` | Delta Lake transaction log: atomic commits, snapshot management |
| S3 operations | `aws/aws-sdk-go-v2` | S3 multipart uploads, listing, presigned URLs |
| Query engine | `marcboeker/go-duckdb` | DuckDB embedded via CGo — vectorized SQL execution |
| PG wire protocol | `jeroenrinzema/psql-wire` | Postgres frontend/backend protocol v3 server |
| State store | `pgx/v5` or S3 key | LSN position persistence for crash recovery |

> **Language Decision:** Go was chosen over Rust for faster time to market. PeerDB — the best production Postgres CDC implementation — is written in Go and serves as a direct reference. Rust has better ecosystem coherence for this stack (arrow-rs + iceberg-rust are the same Apache repo) but represents a 6-month learning curve before productive product code.

---

## 9. Build Phases

| Phase | Weeks | Deliverable | Success Criteria |
|---|---|---|---|
| 1 — Prototype | 1–3 | WAL consumer + INSERT to Parquet to S3 + DuckDB SELECT | `SELECT COUNT(*)` on a replicated table returns correct row count |
| 2 — Correctness | 4–6 | UPDATE/DELETE via delete files, full type map, crash-safe LSN ack | All Postgres DML reflected correctly after consumer restart |
| 3 — Postgres Interface | 7–9 | psql-wire server, BI tool connection, lag metrics, schema changes | Metabase connects and renders a dashboard without errors |
| 4 — Production Hardening | 10–14 | Reconnect/resume, compaction, multi-tenant isolation, observability | 72-hour soak test: zero data loss, no WAL slot bloat |

---

## 10. Open Questions

- **Iceberg migration:** `delta-go` is the pragmatic choice now. Evaluate `iceberg-go` maturity at end of Phase 4. Delta Lake Parquet files are identical — only the metadata layer changes on migration.
- **Replication source:** connect to the primary, or require customers to provision a dedicated physical standby? What does the managed SaaS setup guide recommend as the default?
- **Catalog for Iceberg path:** Delta Lake requires no external catalog. Iceberg requires Glue, Nessie, or Polaris REST catalog. What is the operational cost of this dependency for self-hosted customers?
- **Multi-database:** initial target is one Postgres database per streambed instance. When does a single tenant need multiple source databases replicated to the same S3 bucket?
- **DuckDB compatibility gaps:** document the specific 10% of queries DuckDB cannot serve before GA. Customer expectations must be set before they discover limitations in production.
- **Pricing model:** per-GB of Postgres data replicated, per-query compute, or flat monthly? Needs validation against target customer (CPA/tax firms, SaaS companies on Snowflake) willingness to pay.