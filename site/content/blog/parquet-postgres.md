---
title: "Parquet is to file formats what Postgres is to databases"
date: 2026-09-25
authors:
  - name: Vignesh (Viggy) Ravichandran
---

The more I work with modern data infrastructure, the more I believe this:

> Parquet is to file formats what Postgres is to databases.

<!--more-->

![Postgres and Parquet as ecosystem defaults](/images/parquet-postgres.png)

When a new database launches, one of the first questions is whether it speaks the Postgres wire protocol. One of the first demos often involves connecting with `psql`. Existing clients and tools already understand that interface.

That does not make every Postgres-compatible database PostgreSQL. Compatibility can stop at the wire protocol, the SQL dialect, or a subset of behavior. But Postgres has become the center of gravity. New databases explain themselves by describing how close they are to it.

Parquet increasingly plays the same role for warehouses and lakehouses.

A new query engine is expected to read it. DuckDB reads it. Spark reads it. ClickHouse reads it. Snowflake can load it and query it through external tables. Object stores are full of it. If a system works with analytical data, not supporting Parquet now requires an explanation.

Postgres became the database ecosystem's common language. Parquet is becoming the data ecosystem's common file format.

## Defaults create ecosystems

Postgres did not reach this position by being the perfect database for every workload.

It became a dependable default. It runs almost everywhere. Every major language has mature drivers. Frameworks support it without ceremony. Cloud providers offer it. Extensions can turn it into a geospatial database, a vector store, a time-series system, or something its original authors never planned.

That creates a flywheel:

```text
More users
    ↓
More drivers, tools, and integrations
    ↓
Lower cost for the next system to support Postgres
    ↓
More users
```

Eventually, compatibility becomes a product feature by itself. A database does not need to persuade users to adopt a new client protocol if it can use the one they already have.

Parquet has entered a similar flywheel.

The format is supported across languages, engines, warehouses, and table formats. Producers write Parquet because consumers already understand it. Consumers add Parquet support because so much data is already stored in it.

The result is bigger than any individual engine.

A Spark job can write a Parquet dataset. DuckDB can inspect it locally. ClickHouse can ingest it. A warehouse can load it. A lakehouse table format can use it as its data layer. Each system may have its own execution model, catalog, and preferred query language, but the bytes have a common shape.

## The useful property is not ownership

We see this while building Streambed.

Streambed reads changes from PostgreSQL's write-ahead log and publishes them into DuckLake or Iceberg. The table metadata differs between those targets, but the durable data layer is built around Parquet on object storage.

Conceptually, the path is PostgreSQL WAL → Streambed → Parquet data files, with DuckLake or Iceberg managing the table metadata.

That boundary matters.

The data is not encoded in a private Streambed file format that only Streambed can read. Standard tools already know how to inspect Parquet. Iceberg is an open table format, and DuckLake keeps its table metadata in a queryable SQL catalog.

This does not mean that a pile of Parquet files is automatically a table. Parquet describes the data inside each file. It does not, by itself, define which files are currently active, which rows were deleted, how schema changes should be interpreted, or which historical snapshot a query should read. DuckLake and Iceberg provide those table-level semantics.

There is a useful parallel here:

> Speaking the Postgres wire protocol does not reproduce all of PostgreSQL.
>
> Reading Parquet does not reproduce all of a lakehouse table.

The common format gets a system into the ecosystem. The layers above it still matter.

## Boring infrastructure compounds

The most valuable standards often disappear into the architecture. Teams want existing database clients to connect and new query engines to read the files already in object storage. Every integration makes the next system cheaper to build and safer to adopt.

Neither standard is perfect. Postgres compatibility varies, while Parquet has sharp edges around logical types, timestamps, nested data, compression codecs, and engine-specific conventions. A strong ecosystem learns those edges and builds the tests and adapters needed to live with them.

A successful standard does not need to solve the entire stack. It needs to provide a stable meeting point: Postgres at the database boundary, and Parquet at the analytical data boundary. Boring standards tend to win.
