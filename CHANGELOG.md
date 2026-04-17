# Changelog

All notable changes to Streambed will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Apache 2.0 license
- CI/CD with GitHub Actions (build, test, release)
- Contributing guide and issue/PR templates
- README badges (CI, license, Go reference)

## [0.1.0] - Upcoming

First tagged release. Core CDC pipeline is functional.

### Added
- Postgres logical replication consumer with WAL decoding
- Parquet file writing to S3 with Iceberg metadata commits
- Copy-on-write merge for UPDATE and DELETE operations
- TRUNCATE replication event support
- Built-in query server (DuckDB over Postgres wire protocol)
- `sync`, `resync`, `query`, and `cleanup` CLI commands
- Configurable flush triggers (row count and time interval)
- LSN watermark acknowledgment with Iceberg snapshot as source of truth
- SQLite state persistence for fast startup
- Integration test suite with Postgres and MinIO
- pgbench query performance benchmarks

### Fixed
- Within-batch row deduplication during CoW merge
- Postgres LSN acknowledgment via watermark approach
