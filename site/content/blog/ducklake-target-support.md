---
title: "The small-file problem gets worse with CDC"
date: 2026-08-30
authors:
  - name: Vignesh (Viggy) Ravichandran
---

_Why Streambed is adopting DuckLake for Postgres-to-DuckDB analytics._

The [small-file problem](https://www.dremio.com/blog/apache-iceberg-small-files-problem-causes-fixes-and-prevention/) is well known in the lakehouse world, but it gets worse for CDC workloads.

Every flush to a lakehouse table has a cost. It creates more files, more metadata, and more commit work. Updates and deletes make it worse, especially with copy-on-write.

Streambed started with Iceberg as the target format. Iceberg is mature, widely supported, and a good choice when you want an open table format across many engines.

But Streambed has a narrower path:

```text
Postgres WAL → Streambed → DuckDB query server
```

So the question became simpler:

> If DuckDB is already the query engine, is Iceberg still the best default target for small, repeated CDC flushes?

We started testing DuckLake to answer that.

<!--more-->

![What one CDC flush costs](/images/streamed-ducklake-iceberg.png)

## Why CDC makes this painful

CDC is rarely one large clean batch. It is usually a steady stream of small changes:

```text
small batch → commit
small batch → commit
small batch → commit
```

Small commits are good for freshness. They are bad when each commit creates a lot of lake metadata.

With Iceberg, small commits can mean more data files, manifests, metadata files, and snapshots. For updates and deletes, copy-on-write can also mean reading old files, removing changed rows, and writing new files.

That work adds up quickly.

## Why DuckLake fits Streambed better

DuckLake still stores table data as Parquet. The difference is where the catalog metadata lives.

Instead of storing all table metadata as object-store files, DuckLake keeps catalog metadata in a database such as DuckDB, SQLite, or Postgres.

For Streambed, that matters because DuckDB is already in the hot path. Streambed writes Postgres changes, and the query server uses DuckDB to read them back.

So DuckLake gives us a shorter path:

```text
Postgres changes → DuckDB/DuckLake writer → DuckDB query server
```

It is not magic. The data still has to be written. But the commit path fits the shape of CDC better.

## What the benchmark showed

We added an initial benchmark comparing:

- Iceberg COW
- DuckLake with SQLite catalog
- DuckLake with DuckDB catalog

The benchmark uses:

- baseline tables: 100k and 1M rows
- mutation workload: 10% of the table
- flush sizes: 100, 500, 1,000, and 10,000 buffered units
- read queries: full aggregate, range filter, point lookup, and TopN

The question was not “is DuckLake faster in every possible setup?”

The question was narrower:

> Does DuckLake handle small, repeated CDC-style flushes better than Streambed’s Iceberg COW path?

The initial answer is yes.

For example, with a 1M-row baseline, 100k updates, and `flush=1,000`:

```text
Iceberg COW:       269,344 ms, 806 objects
DuckLake SQLite:     7,075 ms, 405 objects
DuckLake DuckDB:     6,576 ms, 405 objects
```

That is not a tiny difference. That is “go get coffee” vs “blink a few times.”

The benchmark bypasses PostgreSQL logical replication and psql-wire overhead. It measures the writer/catalog commit path and DuckDB reads over local MinIO. So treat it as a lakehouse target benchmark, not an end-to-end Streambed benchmark.

Full benchmark notes and raw results are here:

[DuckLake vs Iceberg benchmark](https://github.com/viggy28/streambed/blob/main/docs/benchmarks/ducklake-vs-iceberg.md)

## What this means for Streambed

Iceberg is not going away. It is still useful if you need broad lakehouse interoperability across engines like Spark, Trino, Snowflake, Flink, and others.

But for Streambed, DuckLake is becoming the preferred direction. Streambed's goal is:

> Make Postgres data available as a DuckDB-powered analytical replica, while preserving as much Postgres client and query compatibility as possible.

The table format should mostly stay in the background. Users should care that their Postgres data is fresh, queryable, and easy to connect to.

So the next big Streambed work is on the Postgres-facing layer:

- Postgres clients
- SQL compatibility
- functions and operators
- `information_schema` and `pg_catalog`
- prepared statements and parameters
- type fidelity for JSON, arrays, UUID, numeric, bytea, timestamps, time zones, and TOAST-heavy columns

DuckLake helps because it makes the default write/read path simpler for the system Streambed is actually building.

Streambed started with Iceberg because it was the obvious open lakehouse answer. For a DuckDB-backed analytical replica of Postgres, DuckLake looks like the better default.
