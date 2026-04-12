---
title: Commands
weight: 2
next: /docs/configuration
prev: /docs/getting-started
---

Streambed provides four commands via the CLI.

## `streambed sync`

Main daemon. Streams WAL changes from Postgres, writes Iceberg tables to S3, and optionally serves queries.

```bash
./streambed sync \
  --source-url="postgres://user:pass@host:5432/db" \
  --s3-bucket="my-bucket" \
  --s3-prefix="iceberg" \
  --query-addr=:5433
```

This is the primary command you'll use. It runs continuously, consuming WAL events and flushing them as Parquet files with Iceberg metadata.

## `streambed resync`

One-shot backfill of a table via `COPY` under a consistent snapshot.

```bash
./streambed resync --table=public.users
```

Use this when you need to re-ingest a table from scratch — for example, after schema changes or data corrections.

## `streambed query`

Standalone query server (no sync). Points at existing Iceberg tables on S3.

```bash
./streambed query \
  --s3-bucket="my-bucket" \
  --s3-prefix="iceberg" \
  --query-addr=:5433
```

Useful when you want to query Iceberg data without running a sync process.

## `streambed cleanup`

Deletes S3 objects and local state for a table.

```bash
./streambed cleanup --table=public.users
```

Run this before `resync` to start fresh, or when decommissioning a table from Streambed.
