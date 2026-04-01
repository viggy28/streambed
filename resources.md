# streambed — Resources & Reference Projects

A curated list of projects, libraries, papers, and articles relevant to building a Postgres-to-S3 analytics engine. Organized by layer.

---

## Competitive & Related Projects

### Direct Competitors / Closest Architecture

---

#### BemiDB
**Repo:** https://github.com/BemiHQ/BemiDB
**Site:** https://bemidb.com
**HN Launch (Nov 2024):** https://news.ycombinator.com/item?id=42078067 — 209 points, 117 comments
**HN Launch 2 (May 2025):** https://news.ycombinator.com/item?id=44054593

Closest open-source competitor. Postgres CDC → Iceberg on S3 → DuckDB → Postgres wire protocol. Positions as "open-source Snowflake + Fivetran alternative." Single Docker image deployment.

- **Stack:** Go, DuckDB embedded, Apache Iceberg, Parquet, S3
- **What it does:** Syncs Postgres via logical replication to Iceberg tables on S3, serves queries via Postgres-compatible analytical query engine
- **Claims:** 2000x faster than Postgres on TPC-H benchmark, 4x data compression
- **Key gap vs streambed:** OSS/self-hosted focused; managed SaaS offering is early; their v1 used WAL replication, v2 expanding to multi-source
- **Strategic note:** They went from "Postgres read replica for analytics" to "data warehouse replacement" — validates the market evolution we've identified

---

#### pg_lake (Snowflake Labs)
**Repo:** https://github.com/Snowflake-Labs/pg_lake ⭐ 1.4k
**Docs:** https://github.com/Snowflake-Labs/pg_lake/blob/main/docs/README.md
**History:** Built at Crunchy Data (Citus team), acquired by Snowflake June 2025, open-sourced November 2025

Postgres *extension* that adds Iceberg and data lake query capabilities inside Postgres. NOT a WAL consumer — no CDC, no automated replication.

- **Stack:** C (Postgres extension), Python, DuckDB (via pgduck_server sidecar), Apache Iceberg, Avro, S3
- **Architecture:** Postgres hooks → pgduck_server (separate DuckDB process, PG wire protocol) → S3 Iceberg tables
- **What it does:** Lets you `CREATE TABLE ... USING iceberg`, `COPY` to/from S3, query Parquet/CSV/JSON foreign tables, read geospatial formats
- **What it does NOT do:** Replicate your Postgres WAL to S3 automatically. You bring the data; pg_lake gives you a nice interface.
- **Components:** pg_lake_iceberg, pg_lake_table, pg_lake_copy, pg_lake_engine, pgduck_server, duckdb_streambed (10 total)
- **Key learnings for streambed:**
  - pgduck_server pattern validated by Snowflake — separate DuckDB process via PG wire is the right architecture (threading/memory-safety limitations rule out embedding DuckDB inside Postgres)
  - Iceberg chosen over Delta Lake — signals industry direction
  - Built by Citus team (most experienced Postgres extension builders in the world) and still took this many components — confirms complexity of the query side
- **Relationship:** Integration/partnership target, not a threat. They cover query-from-Postgres; we cover replication-to-S3.

---

#### pg_duckdb (MotherDuck / DuckDB)
**Repo:** https://github.com/duckdb/pg_duckdb ⭐ 3k+
**Docs:** https://github.com/duckdb/pg_duckdb

DuckDB-powered Postgres for analytics. Built by DuckDB + Hydra + MotherDuck collaboration.

- **What it does:** Embeds DuckDB inside Postgres process. Intercepts analytical queries and routes them to DuckDB's vectorized engine. Reads/writes Parquet, Iceberg, Delta Lake from S3/GCS/Azure.
- **What it does NOT do:** CDC or WAL replication. No automated sync to S3.
- **Usage:**
  ```sql
  SET duckdb.force_execution = true;
  SELECT order_date, COUNT(*), SUM(amount) FROM orders GROUP BY order_date;
  -- DuckDB executes this, not Postgres
  ```
- **Key difference from streambed:** In-process DuckDB (single node), no replication, no S3 sync. Good for querying external S3 files. Not a replacement for the replication layer.
- **Note:** pg_lake explicitly chose NOT to embed DuckDB in-process (chose separate pgduck_server process instead) due to threading and memory-safety concerns. Worth reading their engineering notes on this.

---

#### ParadeDB / pg_analytics
**Repo:** https://github.com/paradedb/pg_analytics
**Previously:** pg_lakehouse

DuckDB inside Postgres via foreign data wrappers. Pushes analytical queries to DuckDB. Rust-based (uses pgrx).

- **What it does:** FDW + executor hook to query S3 Parquet/Iceberg/Delta Lake files from inside Postgres
- **Approach:** Foreign tables pointing at S3 paths; query planner pushes predicate to DuckDB
- **Note:** No WAL replication. External data query tool only.

---

#### Xata (Postgres Analytics)
**Site:** https://xata.io/postgres-analytics

Managed cloud service: Postgres → Iceberg on S3 → DuckDB analytics endpoint.

- Uses logical replication from a read-replica (minimal production impact)
- DuckDB-powered query engine via pg_duckdb extension
- Zero-ETL sync to Apache Iceberg
- Available as Xata Cloud or BYOC (Bring Your Own Cloud — deployed in your AWS/Azure/GCP)
- Most architecturally similar to streambed as a managed product

---

### Related CDC Tools

---

#### PeerDB (ClickHouse / acquired)
**Repo:** https://github.com/PeerDB-io/peerdb
**Docs:** https://docs.peerdb.io
**Blog:** https://blog.peerdb.io
**License:** AGPLv3

Best-in-class Postgres CDC tool. Acquired by ClickHouse in 2024. Now powers ClickPipes (ClickHouse's native Postgres replication).

- **Stack:** Go — the primary reference implementation for Postgres WAL consumption in Go
- **What it does:** Postgres → ClickHouse, Snowflake, BigQuery, Kafka, S3, Postgres via CDC. Multiple streaming modes: log-based (WAL/CDC), cursor-based, XMIN-based
- **Scale:** 200+ TB of Postgres data replicated to ClickHouse per month across 400+ companies
- **Key differentiator:** Postgres-compatible SQL interface for ETL (`CREATE MIRROR` syntax). Parallel snapshotting for initial loads.
- **Relevance to streambed:** Direct reference implementation for Go-based WAL consumption. Their WAL consumer code, LSN tracking, and type mapping are the best reference in the ecosystem. They land in ClickHouse; we land in customer-owned S3 (open format, no vendor lock-in).
- **Customer stories:** AutoNation, Seemplicity, Ashby, Vapi, SpotOn, LC Waikiki (largest retailer in Turkey)

---

#### Debezium
**Site:** https://debezium.io
**Repo:** https://github.com/debezium/debezium

Industry-standard CDC platform. Java-based, Kafka-native.

- **Architecture:** Postgres → Debezium (Kafka Connect) → Kafka → sink connectors
- **Pros:** Battle-tested, wide connector ecosystem, exactly-once delivery with Kafka
- **Cons:** Complex (Zookeeper + Kafka + Schema Registry + connectors), Java, not Postgres-optimized
- **Relevance:** What PeerDB was explicitly built to replace. Useful for understanding what NOT to build. Our Go WAL consumer should be as simple as PeerDB, not as complex as Debezium.

---

## Core Libraries (streambed Stack)

### WAL / Postgres

---

#### pglogrepl
**Repo:** https://github.com/jackc/pglogrepl
**Author:** Jack Christensen (author of pgx)

Go library for Postgres logical replication. Implements the pgoutput plugin protocol.

- Decodes WAL messages: `Relation`, `Begin`, `Insert`, `Update`, `Delete`, `Commit`, `Truncate`
- Handles LSN (Log Sequence Number) tracking and acknowledgement
- The lowest-level correct way to do Postgres logical replication in Go
- Used by PeerDB as their WAL consumer foundation

---

#### pgx
**Repo:** https://github.com/jackc/pgx
**Docs:** https://pkg.go.dev/github.com/jackc/pgx/v5

PostgreSQL driver and toolkit for Go. The standard Postgres client library for Go.

- Connection pooling, prepared statements, COPY protocol, notification listeners
- Works alongside pglogrepl for replication slot management
- v5 is the current stable version

---

### Parquet / Storage Format

---

#### parquet-go
**Repo:** https://github.com/parquet-go/parquet-go

Pure Go Parquet implementation. Read/write Parquet files with full column encoding support.

- Dictionary encoding, delta encoding, RLE, Snappy/Zstd/Gzip compression
- Row group and page-level control
- Schema reflection from Go structs or dynamic schema definition
- The library to use for writing Parquet in Go (more active than older `xitongsys/parquet-go`)

---

### Table Format

---

#### delta-go
**Repo:** https://github.com/rivian/delta-go

Go implementation of the Delta Lake protocol. Maintained by Rivian.

- Writes Delta Lake transaction log entries (JSON commits)
- Supports add/remove file operations, schema evolution, checkpoints
- Production-ready for write patterns needed by a WAL consumer
- More mature than `iceberg-go` for writes as of early 2026

---

#### iceberg-go (Apache)
**Repo:** https://github.com/apache/iceberg-go

Official Apache Iceberg Go library.

- Read/write Iceberg tables
- **Current status (early 2026):** Gaps in atomic snapshot commits, merge-on-read deletes, and schema evolution — not production-ready for our write patterns yet
- **Migration path:** Delta Lake Parquet files are identical to Iceberg Parquet files. Only the metadata layer changes. Watch this repo for maturity.
- REST catalog support (Nessie, Polaris, Glue) would be needed for production Iceberg writes

---

#### Apache XTable (formerly OneTable)
**Repo:** https://github.com/apache/incubator-xtable
**Site:** https://xtable.apache.org

Interoperability layer between Delta Lake, Iceberg, and Hudi. Translates metadata without touching Parquet files.

- Convert a Delta Lake table to Iceberg metadata and vice versa
- Enables "write Delta, read as Iceberg" during migration
- Relevant for streambed's migration path: start with delta-go, expose as Iceberg via XTable, then migrate natively when iceberg-go matures

---

### Query Engine

---

#### DuckDB
**Site:** https://duckdb.org
**Repo:** https://github.com/duckdb/duckdb ⭐ 30k+
**Docs:** https://duckdb.org/docs

Embeddable OLAP SQL database. The query engine powering streambed's analytical layer.

- Vectorized execution engine (SIMD-based column processing)
- Native Parquet, Delta Lake, Iceberg, CSV, JSON reader
- Direct S3 read via `httpfs` extension (no data loading step)
- Full SQL: window functions, CTEs, ROLLUP, CUBE, lateral joins, UNNEST
- ~90% Postgres SQL compatibility for analytical workloads
- 50.7% YoY developer growth (2024), #3 most desired DB (Stack Overflow 2024)
- **DuckLake** (May 2025): DuckDB's own lakehouse format — worth monitoring

---

#### go-duckdb
**Repo:** https://github.com/marcboeker/go-duckdb

CGo bindings for DuckDB in Go. The bridge between the Go WAL consumer and DuckDB's query engine.

- Embeds DuckDB as a library (not a separate process) — same process as the WAL consumer
- SQL execution, prepared statements, appender API (high-performance bulk inserts)
- CGo overhead is microseconds vs seconds of query time — acceptable trade-off
- Used in production by multiple Go services

---

### Postgres Wire Protocol

---

#### psql-wire
**Repo:** https://github.com/jeroenrinzema/psql-wire

Go implementation of the Postgres frontend/backend wire protocol v3.

- Build a "Postgres-compatible" server in Go
- Handles authentication, query messages, DataRow, CommandComplete, ErrorResponse
- Any Postgres client (psql, pgx, psycopg2, SQLAlchemy, JDBC) connects without modification
- Used by projects that want to expose a non-Postgres backend via the Postgres protocol

---

### S3 / Object Storage

---

#### aws-sdk-go-v2
**Repo:** https://github.com/aws/aws-sdk-go-v2
**Docs:** https://aws.github.io/aws-sdk-go-v2/docs

Official AWS SDK for Go v2. Used for all S3 operations in streambed.

- Multipart uploads (required for Parquet files > 5GB)
- Presigned URLs, bucket listing, object metadata
- Context-aware, modular, significantly improved over v1

---

## Data Format Standards

---

#### Apache Parquet
**Site:** https://parquet.apache.org
**Spec:** https://github.com/apache/parquet-format

Columnar storage file format. The physical storage format for all streambed data.

- Column-oriented: reads only columns referenced in queries
- Row groups (typically 128MB): unit of parallelism for DuckDB
- Page-level min/max statistics: enables predicate pushdown
- Encodings: dictionary, delta, RLE, bit-packing, plain
- Compression codecs: Snappy (fast), Zstd (better ratio), Gzip (max ratio)
- Immutable once written — mutations require new files + table format management

---

#### Apache Arrow
**Site:** https://arrow.apache.org
**Repo:** https://github.com/apache/arrow

In-memory columnar data format. The wire format between DuckDB internals and external systems.

- IPC format for zero-copy data exchange between processes/languages
- Arrow Flight SQL: transport protocol for query results (used by some DuckDB clients)
- Not a storage format (that's Parquet) — Arrow is for in-memory and in-flight data
- DuckDB uses Arrow internally; `go-duckdb` returns results as Arrow record batches

---

#### Apache Iceberg
**Site:** https://iceberg.apache.org
**Spec:** https://iceberg.apache.org/spec

Open table format for huge analytic datasets. ACID transactions on plain object storage.

- **Created at:** Netflix, donated to Apache Software Foundation
- **Backed by:** Apple, LinkedIn, Adobe, Netflix, AWS, Google, Snowflake, Databricks
- Metadata tree: `metadata.json` → snapshot manifest list → manifest files → Parquet data files
- Features: ACID transactions, time travel, schema evolution, hidden partitioning, partition evolution
- REST catalog spec: standardized catalog API (Polaris, Nessie, AWS Glue, Apache Gravitino)
- **Industry direction:** Iceberg has won the table format standards war. All major cloud services (Athena, BigQuery, Databricks, Snowflake) support Iceberg natively.

---

#### Delta Lake
**Site:** https://delta.io
**Spec:** https://github.com/delta-io/delta/blob/master/PROTOCOL.md
**Repo:** https://github.com/delta-io/delta

Open table format built by Databricks. streambed's initial table format choice.

- Transaction log: append-only JSON commit entries in `_delta_log/`
- Simpler metadata structure than Iceberg (flat log vs tree)
- No external catalog required — pointer lives in `_delta_log/` directory
- DuckDB reads Delta Lake natively
- `delta-go` (Rivian): most mature Go write implementation
- Dominant in Databricks/Spark ecosystem; Iceberg dominant elsewhere

---

#### Apache Hudi
**Site:** https://hudi.apache.org

Third table format. Record-level upserts, incremental queries. Less relevant for streambed but worth knowing.

- Copy-on-write vs Merge-on-read storage types
- Strong upsert primitives (relevant for UPDATE-heavy workloads)
- Heavy Java/Spark dependency; less DuckDB support than Delta/Iceberg
- Used heavily at Uber (original creator)

---

## Catalog Services (Iceberg Migration Path)

---

#### Project Nessie
**Site:** https://projectnessie.org
**Repo:** https://github.com/projectnessie/nessie

Open-source transactional catalog for Iceberg (and Delta Lake). Git-like branching for data.

- REST catalog API compatible with Iceberg spec
- Self-hosted or managed via Dremio Arctic
- Useful for: multi-writer coordination, atomic commits across multiple tables

---

#### Apache Polaris (Snowflake donation)
**Repo:** https://github.com/apache/polaris
**Blog:** https://www.snowflake.com/blog/introducing-polaris-catalog

Open-source Iceberg REST catalog donated by Snowflake to Apache.

- Full Iceberg REST catalog implementation
- Fine-grained access control, multi-tenant
- Backed by Snowflake, Apple, AWS — likely to become the standard catalog

---

#### AWS Glue Data Catalog
**Docs:** https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html

AWS-managed Iceberg/Hive/Delta catalog. Zero operational overhead on AWS.

- Native Iceberg REST catalog support (2023+)
- Free tier: 1M objects, then $1/100k objects/month
- Works with Athena, EMR, Redshift Spectrum, and DuckDB
- Easiest catalog option for AWS-native deployments

---

## Articles & Blog Posts

---

### WAL / CDC

**Logical Replication Internals**
https://www.postgresql.org/docs/current/logical-replication.html
Official Postgres docs. Start here before any WAL consumer work.

**How Logical Decoding Works**
https://www.postgresql.org/docs/current/logicaldecoding-explanation.html
Deep dive into pgoutput protocol, replication slots, LSN semantics.

**PeerDB Engineering Blog**
https://blog.peerdb.io
Best practical writing on production Postgres CDC. Topics: WAL slot management, TOAST columns, large table snapshotting, schema evolution handling.

---

### DuckDB

**DuckDB as the New Jedi**
https://www.databricks.com/blog/2021/05/27/introducing-delta-lake-with-apache-spark.html
(Background on the lakehouse pattern that DuckDB enables at small/medium scale)

**DuckDB — An Embeddable Analytical Database**
https://duckdb.org/2019/06/10/announcement.html
Original announcement. Explains the design philosophy.

**DuckDB + Iceberg**
https://duckdb.org/docs/extensions/iceberg

**DuckDB + Delta Lake**
https://duckdb.org/docs/extensions/delta

**Awesome DuckDB**
https://github.com/davidgasquez/awesome-duckdb
Curated list of DuckDB integrations, tools, and projects. Comprehensive.

---

### Delta Lake / Iceberg

**Delta Lake Protocol Specification**
https://github.com/delta-io/delta/blob/master/PROTOCOL.md
The actual spec. Read before implementing delta-go usage. Key sections: transaction log format, commit protocol, checkpoint semantics, deletion vectors.

**Iceberg Table Spec**
https://iceberg.apache.org/spec
The actual Iceberg specification. More complex than Delta but better designed for multi-engine access.

**Apache XTable (Delta ↔ Iceberg interoperability)**
https://xtable.apache.org/docs/how-to
How to convert between Delta Lake and Iceberg metadata without touching Parquet files.

**Delta vs Iceberg vs Hudi comparison (Databricks)**
https://www.databricks.com/blog/2022/06/30/open-sourcing-all-of-delta-lake.html

---

### Architecture & Positioning

**BemiDB HN thread — Nov 2024**
https://news.ycombinator.com/item?id=42078067
209 points, 117 comments. Real customer pain validation. Read the comments — they contain the best articulation of why teams want "Postgres → analytics without a data warehouse."

**Snowflake acquired Crunchy Data — June 2025**
https://www.crunchydata.com/blog/crunchy-data-joins-snowflake
Strategic context for pg_lake open-sourcing. Snowflake wants Postgres to be an Iceberg citizen so more data flows into their compute.

**Databricks acquired Neon — ~$1B**
https://neon.tech/blog/databricks-neon
Serverless Postgres with S3-backed storage. Signals how seriously the data platform companies take Postgres/S3 convergence.

**One team cut Snowflake costs 79% with DuckDB**
https://motherduck.com/blog/big-data-is-dead (MotherDuck)
The cost compression story that validates streambed's positioning.

---

## Go Ecosystem Reference

---

| Library | Purpose | Repo |
|---|---|---|
| `jackc/pglogrepl` | Postgres logical replication decode | https://github.com/jackc/pglogrepl |
| `jackc/pgx/v5` | Postgres driver | https://github.com/jackc/pgx |
| `parquet-go/parquet-go` | Parquet read/write | https://github.com/parquet-go/parquet-go |
| `rivian/delta-go` | Delta Lake transaction log | https://github.com/rivian/delta-go |
| `apache/iceberg-go` | Iceberg (watch for maturity) | https://github.com/apache/iceberg-go |
| `marcboeker/go-duckdb` | DuckDB CGo binding | https://github.com/marcboeker/go-duckdb |
| `jeroenrinzema/psql-wire` | Postgres wire protocol server | https://github.com/jeroenrinzema/psql-wire |
| `aws/aws-sdk-go-v2` | AWS S3 operations | https://github.com/aws/aws-sdk-go-v2 |
| `apache/arrow/go/v17` | Arrow in-memory format | https://github.com/apache/arrow/tree/main/go |

---

## Market Context

| Metric | Value | Source |
|---|---|---|
| Cloud DWH market size 2025 | $36.3B | Industry reports |
| Cloud DWH market size 2034 (projected) | $155.7B at 17.55% CAGR | Industry reports |
| Snowflake FY2025 product revenue | $3.63B (+29% YoY) | Snowflake earnings |
| Snowflake customers | 9,400+ | Snowflake earnings |
| DuckDB developer growth (YoY) | +50.7% | Stack Overflow 2024 |
| DuckDB rank (most desired DB) | #3 | Stack Overflow 2024 |
| Databricks/Neon acquisition | ~$1B | May 2025 |
| Snowflake/Crunchy Data acquisition | ~$250M | June 2025 |
| PeerDB CDC volume (ClickPipes) | 200+ TB/month | ClickHouse blog Dec 2025 |
| PeerDB customers via ClickPipes | 400+ | ClickHouse blog Dec 2025 |