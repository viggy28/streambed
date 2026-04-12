---
title: "Why I Built Streambed"
date: 2026-04-12
authors:
  - name: Viggy
---

Every Postgres database eventually hits the same wall: analytical queries that are too slow for production but too important to ignore.

<!--more-->

The usual answer is to build an ETL pipeline — set up Spark, Airflow, or some managed service to extract data from Postgres, transform it, and load it into a data warehouse. By the time you're done, you have a dozen moving parts, a multi-hour data delay, and a new class of bugs where your warehouse diverges from your source of truth.

Streambed takes a different approach. It connects to Postgres as a logical replication subscriber — the same mechanism Postgres uses for read replicas — and streams WAL changes directly into Apache Iceberg tables on S3. No intermediate queue. No batch jobs. No Spark.

The result: your analytical data is seconds behind production, stored in an open format (Parquet + Iceberg), and queryable from `psql` using the built-in DuckDB query server.

## The benchmark that convinced me

pgbench schema, 1M accounts, 500K history rows. A correlated subquery that joins across tables:

```
Postgres (port 5432):  ~45s
Streambed (port 5433): ~2s
```

Same query. Same `psql`. Same results. DuckDB's columnar engine on Parquet files is simply a better fit for analytical workloads than Postgres's row-oriented storage.

## What's next

This blog will be a place to share the deeper technical challenges — WAL decoding edge cases, copy-on-write merge strategies, Iceberg metadata management, and more. Stay tuned.
