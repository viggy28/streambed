---
title: Commands
weight: 2
next: /docs/configuration
prev: /docs/getting-started
---

Streambed provides operational commands for syncing, querying, backfilling, cleanup, and maintenance.

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

## `streambed maintenance`

Expires old Iceberg snapshots and can dry-run orphan planning.

```bash
./streambed maintenance \
  --table=public.users \
  --retain-last=1 \
  --dry-run
```

Use apply mode with `--dry-run=false --force` after reviewing the plan. Orphan deletion is currently dry-run only.

## `streambed maintenance compact`

Compacts many small active Parquet data files into fewer target-sized files.

```bash
./streambed maintenance compact \
  --table=public.users \
  --target-file-size-mb=128 \
  --small-file-threshold-mb=32 \
  --max-input-files=1000 \
  --dry-run
```

Apply with `--dry-run=false --force`. Online compaction coordinates with `streambed sync` through the SQLite state DB, so the maintenance process must use the same `--state-path`, S3 bucket, and S3 prefix as the running sync daemon.

See [Maintenance](/docs/maintenance/) for details and safety notes.
