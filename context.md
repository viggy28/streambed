# PG-Lake: Problem & Architecture Summary

## The Problem

Postgres couples storage and compute. This creates two distinct pain points that look different on the surface but share the same root cause.

### Pain Point 1: The Data Warehouse Tax

Companies with Postgres as their source of truth eventually need analytics — dashboards, reports, long-running aggregations. The default path is to stand up a data warehouse (Snowflake, Redshift, BigQuery) and build an ETL pipeline to keep it in sync. This works, but it's expensive:

- Snowflake costs $2k–$50k+/month, with documented 200–300% bill overruns
- ETL pipelines require a dedicated data engineer to maintain
- Schema changes in Postgres break the pipeline
- You're paying to store and compute against a copy of data you already own

The fundamental absurdity: if Postgres is your source of truth, why does analytics require a second database, a pipeline, and a separate bill?

### Pain Point 2: Read Replica Limitations (Secondary)

Analytics workloads routed to Postgres read replicas create resource contention — a long-running `GROUP BY` over 50M rows starves app traffic. Teams spin up dedicated analytics replicas, but this doesn't scale: managed providers cap replica counts, and replicas are full Postgres instances (expensive EBS-backed compute running 24/7 regardless of query volume).

This pain is real, but it's a symptom of the warehouse problem, not a standalone problem. The right fix is removing analytics traffic from Postgres entirely, not adding more replicas.

---

## The Architecture

### Core Insight

Separate storage from compute. Store Postgres data in open columnar format (Parquet) on customer-owned S3. Route analytics queries to a vectorized query engine (DuckDB) that reads directly from S3. The Postgres primary is untouched — it handles writes and OLTP reads exactly as before.

### Data Flow

```
Postgres Primary (writes + OLTP reads, untouched)
    │
    │  WAL / logical replication (pgoutput)
    ▼
WAL Consumer (Go + pglogrepl)
    │  decodes INSERT / UPDATE / DELETE
    ▼
S3 (Delta Lake tables — Parquet files + transaction log)
    │
    ▼
DuckDB (vectorized query engine, reads Parquet natively)
    │
    ▼
psql-wire (Postgres wire protocol)
    │
    ▼
Any Postgres client — BI tools, psql, dbt, reporting pipelines
```

### Key Components

**WAL Consumer** — reads Postgres's Write-Ahead Log via logical replication. Every INSERT, UPDATE, and DELETE is decoded in real time without touching the primary's query path.

**Delta Lake on S3** — an open table format layered on top of immutable Parquet files. Provides ACID semantics (atomic commits, snapshot isolation), time travel, and schema evolution on plain S3 storage. Handles the UPDATE/DELETE problem: since Parquet files are immutable, mutations are represented as delete files + new data files, with Delta Lake's transaction log tracking which files constitute the current table state.

**DuckDB** — a vectorized OLAP query engine. Operates on batches of column values using modern CPU instructions rather than row-by-row iteration. Reads Parquet and Delta Lake natively. Can aggregate 100M rows in ~2 seconds. Embedded via CGo (`go-duckdb`).

**psql-wire** — serves DuckDB query results over the Postgres wire protocol, making the system compatible with any Postgres client without any driver changes.

### What "Vectorized" Means

Standard execution: loop over rows one at a time, apply filter, accumulate result.

Vectorized execution: load an entire column as a vector, apply filter mask to the whole batch at once using CPU SIMD instructions, sum the matching values in a single pass. This is why DuckDB is 10–100x faster than Postgres for analytics — it skips reading columns you don't need and processes the ones you do in bulk.

### What Iceberg / Delta Lake Solves

Parquet files are immutable — you can't edit a row in place. Delta Lake wraps Parquet files with a transaction log that tracks:

- Which files are current vs. superseded
- Which rows are deleted (delete files)
- Schema versions over time
- Atomic snapshot transitions (two writers don't corrupt each other)

This gives ACID semantics to plain files sitting on S3, with no database process required.

---

## What This Product Is (and Isn't)

| | This Product | Postgres Primary | Snowflake |
|---|---|---|---|
| Handles OLTP app reads | ❌ | ✅ | ❌ |
| Handles writes | ❌ | ✅ | ❌ |
| Handles analytics / reports | ✅ | ❌ (bad) | ✅ |
| Requires ETL pipeline | ❌ | — | ✅ |
| Data on customer-owned S3 | ✅ | — | ❌ |
| Cost at scale | Low (S3 + compute) | Medium | High |

**What it replaces:** The analytics read replica and/or the data warehouse (Snowflake, Redshift).

**What it doesn't replace:** The Postgres primary for app traffic, OLTP reads, or writes. These stay on Postgres.

**Replication lag:** Data appears in S3 seconds to low minutes after being written to Postgres. This is acceptable for every analytics use case (BI dashboards, reports, dbt pipelines) and is no different from any other data warehouse. It is not acceptable for user-facing transactional reads — those stay on Postgres.

---

## Competitive Landscape

**pg_lake (Snowflake Labs)** — lets you query Iceberg tables from inside Postgres using DuckDB. Does NOT replicate data automatically. You're responsible for getting data into Iceberg. Zero WAL/CDC capability. This is the query side without the replication side.

**pg_duckdb (MotherDuck)** — embeds DuckDB inside the Postgres process. No CDC, no S3 replication. Analytical query acceleration only.

**PeerDB (ClickHouse)** — Postgres CDC to ClickHouse or Snowflake. Excellent replication, but lands data in a third-party warehouse, not customer-owned S3.

**BemiDB** — closest competitor. Postgres CDC → S3 → Iceberg → DuckDB, open source, early stage.

**The gap this product fills:** Automated, continuous Postgres WAL → customer's own S3 (open format) → queryable via Postgres wire protocol, as a managed SaaS. The WAL consumer is the hard, valuable part none of the above fully provide.

---

## Go Stack

| Component | Library |
|---|---|
| WAL consumer | `pglogrepl` + `pgx/v5` |
| Parquet writes | `parquet-go` |
| Delta Lake metadata | `delta-go` |
| S3 operations | `aws-sdk-go v2` |
| Query execution | `go-duckdb` (CGo) |
| Postgres wire protocol | `psql-wire` |

---

## Positioning

**"If your source of truth is Postgres, you should never have needed Snowflake."**

The on-ramp to analytics used to require a $2k–$50k/month warehouse plus an ETL engineer. This product makes analytics a native capability of your Postgres stack — zero ETL, zero new query language, zero data leaving your own S3 bucket.