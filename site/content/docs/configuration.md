---
title: Configuration
weight: 3
next: /docs/architecture
prev: /docs/commands
---

All configuration is done via CLI flags or environment variables. Every flag has a corresponding environment variable with the `STREAMBED_` prefix.

## Source Database

| Flag | Env Var | Description |
|------|---------|-------------|
| `--source-url` | `STREAMBED_SOURCE_URL` | Postgres connection string |

The source database must have logical replication enabled (`wal_level = logical`).

## S3 / Object Storage

| Flag | Env Var | Description |
|------|---------|-------------|
| `--s3-bucket` | `STREAMBED_S3_BUCKET` | S3 bucket name |
| `--s3-prefix` | `STREAMBED_S3_PREFIX` | Key prefix for Iceberg data |
| `--s3-endpoint` | `STREAMBED_S3_ENDPOINT` | Custom S3 endpoint (for MinIO, etc.) |
| `--s3-region` | `STREAMBED_S3_REGION` | AWS region |

## Flush Behavior

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--flush-rows` | `STREAMBED_FLUSH_ROWS` | `10000` | Flush after this many buffered rows |
| `--flush-interval` | `STREAMBED_FLUSH_INTERVAL` | `2s` | Flush after this time interval |

## Query Server

| Flag | Env Var | Description |
|------|---------|-------------|
| `--query-addr` | `STREAMBED_QUERY_ADDR` | Address for the Postgres-compatible query server (e.g. `:5433`) |
