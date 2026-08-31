# DuckLake vs Iceberg CDC-style benchmark

This benchmark compares Streambed's lakehouse writer/read path across:

- Iceberg COW
- Iceberg MOR/equality deletes
- DuckLake with SQLite catalog
- DuckLake with DuckDB catalog

The goal is to measure the workload that matters for Streambed: frequent CDC-like appends, updates, and deletes followed by DuckDB reads.

## Benchmark harness

Executable harness:

```text
test/integration/lakehouse_feature_benchmark_test.go
```

Run the benchmark with the integration stack:

```bash
docker compose -f test/integration/docker-compose.yml up -d --wait || true

STREAMBED_RUN_LAKEHOUSE_FEATURE_BENCH=1 \
STREAMBED_LAKEHOUSE_BENCH_ROWS=100000 \
STREAMBED_LAKEHOUSE_BENCH_FLUSH_ROWS=100,500,1000,10000 \
STREAMBED_LAKEHOUSE_BENCH_QUERY_RUNS=7 \
STREAMBED_LAKEHOUSE_BENCH_OUTPUT=/tmp/streambed-bench/lakehouse-matrix-100k.json \
go test -tags integration -run TestLakehouseFeatureBenchmark -count=1 ./test/integration -v
```

For long 1M runs, increase the Go test timeout:

```bash
STREAMBED_RUN_LAKEHOUSE_FEATURE_BENCH=1 \
STREAMBED_LAKEHOUSE_BENCH_ROWS=1000000 \
STREAMBED_LAKEHOUSE_BENCH_FLUSH_ROWS=500,1000,10000 \
STREAMBED_LAKEHOUSE_BENCH_QUERY_RUNS=7 \
STREAMBED_LAKEHOUSE_BENCH_OUTPUT=/tmp/streambed-bench/lakehouse-matrix-1m-500plus.json \
go test -timeout 60m -tags integration -run TestLakehouseFeatureBenchmark -count=1 ./test/integration -v
```

MOR is opt-in because read validation can be slow when equality deletes accumulate:

```bash
STREAMBED_RUN_LAKEHOUSE_FEATURE_BENCH=1 \
STREAMBED_LAKEHOUSE_BENCH_ROWS=1000000 \
STREAMBED_LAKEHOUSE_BENCH_FLUSH_ROWS=1000 \
STREAMBED_LAKEHOUSE_BENCH_QUERY_RUNS=7 \
STREAMBED_LAKEHOUSE_BENCH_INCLUDE_MOR=1 \
STREAMBED_LAKEHOUSE_BENCH_OUTPUT=/tmp/streambed-bench/lakehouse-mor-1m-flush1000.json \
go test -timeout 45m -tags integration \
  -run 'TestLakehouseFeatureBenchmark/iceberg-mor/update-10pct/flush=1000' \
  -count=1 ./test/integration -v
```

## Workload

For each target/scenario, the benchmark:

1. Loads the baseline table in one bulk flush.
2. Reopens the writer against the same target.
3. Applies only the scenario workload at the configured `flush_rows`.
4. Measures scenario `write_duration_ms` only, excluding baseline load.
5. Opens a DuckDB read connection and validates the final snapshot.
6. Runs each read query multiple times and records median/p95 latency.

Table schema:

```sql
id BIGINT PRIMARY KEY,
version BIGINT,
amount BIGINT,
payload VARCHAR
```

Scenarios:

- `append`: insert 10% new rows
- `update-10pct`: update 10% of existing rows
- `delete-10pct`: delete 10% of existing rows
- `mixed-append-update-delete`: append 10%, update 10%, delete 10%

Read queries:

- `full_aggregate`: full-table count/distinct/sum
- `selective_range`: range predicate around the middle of the table
- `point_lookup`: lookup by a single `id`
- `topn_ordered`: `ORDER BY amount DESC LIMIT 10`

## Caveats

- This is not an end-to-end replication benchmark.
- It bypasses PostgreSQL logical replication and psql-wire overhead.
- It measures writer/catalog commit performance plus DuckDB reads over local MinIO.
- Results are from a local Apple M3 Max development machine with a dirty git tree while the benchmark harness was under development. Use these as a baseline snapshot, not an absolute production claim.
- The 1M / `flush=100` Iceberg COW matrix did not complete within the 90 minute test timeout; only DuckLake results are captured for that flush size.
- Iceberg MOR is currently captured for one targeted 1M update scenario. It is included to separate Iceberg's COW write amplification from the MOR read-time delete-merge tradeoff.

## Committed result snapshots

Raw JSON outputs from this benchmark run are committed under:

```text
docs/benchmarks/results/ducklake-vs-iceberg-100k.json
docs/benchmarks/results/ducklake-vs-iceberg-1m-flush500-plus.json
docs/benchmarks/results/ducklake-vs-iceberg-1m-flush100-ducklake.json
```

Each JSON file includes environment metadata, scenario shape, write duration, object counts, total object bytes, and read latency medians/p95s.

## Summary: 100k baseline rows

Values are `write_duration_ms` with total object count in parentheses.

| scenario | flush | iceberg-cow | ducklake-sqlite | ducklake-duckdb |
|---|---:|---:|---:|---:|
| append | 100 | 1,731 (406) | 770 (101) | 542 (101) |
| append | 500 | 236 (86) | 189 (21) | 151 (21) |
| append | 1,000 | 120 (46) | 120 (11) | 88 (11) |
| append | 10,000 | 26 (10) | 40 (2) | 35 (2) |
| update 10% | 100 | 34,334 (806) | 4,074 (401) | 3,798 (401) |
| update 10% | 500 | 6,753 (166) | 851 (81) | 800 (81) |
| update 10% | 1,000 | 3,322 (86) | 450 (41) | 421 (41) |
| update 10% | 10,000 | 346 (14) | 82 (5) | 73 (5) |
| delete 10% | 100 | 16,420 (406) | 1,502 (101) | 1,153 (101) |
| delete 10% | 500 | 3,214 (86) | 362 (21) | 376 (21) |
| delete 10% | 1,000 | 1,582 (46) | 190 (11) | 131 (11) |
| delete 10% | 10,000 | 163 (10) | 44 (2) | 38 (2) |
| mixed | 100 | 53,381 (1,606) | 6,991 (601) | 6,255 (601) |
| mixed | 500 | 10,668 (326) | 1,473 (121) | 1,230 (121) |
| mixed | 1,000 | 5,118 (166) | 735 (61) | 607 (61) |
| mixed | 10,000 | 527 (22) | 122 (7) | 123 (7) |

## Summary: 1M baseline rows

Values are `write_duration_ms` with total object count in parentheses.

| scenario | flush | iceberg-cow | iceberg-mor | ducklake-sqlite | ducklake-duckdb |
|---|---:|---:|---:|---:|---:|
| append | 100 | — | — | 6,630 (1,005) | 5,597 (1,005) |
| update 10% | 100 | — | — | 103,336 (4,005) | 97,904 (4,005) |
| delete 10% | 100 | — | — | 29,966 (1,005) | 26,780 (1,005) |
| mixed | 100 | — | — | 205,288 (6,005) | 196,791 (6,005) |
| append | 500 | 4,468 (806) | — | 1,558 (205) | 1,206 (205) |
| update 10% | 500 | 539,958 (1,606) | — | 14,277 (805) | 13,308 (805) |
| delete 10% | 500 | 256,689 (806) | — | 5,639 (205) | 5,325 (205) |
| mixed | 500 | 804,298 (3,206) | — | 29,479 (1,205) | 28,312 (1,205) |
| append | 1,000 | 2,227 (406) | — | 955 (105) | 732 (105) |
| update 10% | 1,000 | 269,344 (806) | 4,665 (1,206) | 7,075 (405) | 6,576 (405) |
| delete 10% | 1,000 | 128,079 (406) | — | 2,877 (105) | 2,724 (105) |
| mixed | 1,000 | 399,228 (1,606) | — | 13,989 (605) | 13,576 (605) |
| append | 10,000 | 242 (46) | — | 246 (15) | 208 (15) |
| update 10% | 10,000 | 26,999 (86) | — | 891 (45) | 834 (45) |
| delete 10% | 10,000 | 12,836 (46) | — | 416 (15) | 405 (15) |
| mixed | 10,000 | 40,040 (166) | — | 1,792 (65) | 1,697 (65) |

## Example read medians: 1M baseline, flush=1,000

Values are median milliseconds across 7 query runs.

| scenario | target | full agg | range | point | topN |
|---|---|---:|---:|---:|---:|
| append | iceberg-cow | 60 | 37 | 42 | 31 |
| append | ducklake-sqlite | 10 | 4 | 3 | 8 |
| append | ducklake-duckdb | 8 | 2 | 2 | 6 |
| update 10% | iceberg-cow | 42 | 14 | 15 | 52 |
| update 10% | iceberg-mor | 86,786 | 272 | 272 | 85,077 |
| update 10% | ducklake-sqlite | 17 | 4 | 4 | 22 |
| update 10% | ducklake-duckdb | 16 | 2 | 2 | 21 |
| delete 10% | iceberg-cow | 36 | 12 | 14 | 48 |
| delete 10% | ducklake-sqlite | 14 | 3 | 3 | 22 |
| delete 10% | ducklake-duckdb | 12 | 3 | 2 | 20 |
| mixed | iceberg-cow | 56 | 35 | 38 | 32 |
| mixed | ducklake-sqlite | 24 | 4 | 4 | 27 |
| mixed | ducklake-duckdb | 25 | 2 | 2 | 24 |

## Takeaway

With realistic small CDC flush sizes, DuckLake is materially faster than Iceberg COW for mutation-heavy workloads. Iceberg MOR changes that tradeoff: in the targeted 1M update run, MOR made the write path much faster than COW, but reads became much slower because equality deletes must be applied at query time until compaction catches up. DuckDB catalog is usually faster than SQLite catalog, especially for point/range reads and commit-heavy workloads. Iceberg remains useful for broad ecosystem interoperability, but DuckLake better matches Streambed's DuckDB-centered query path and small-batch CDC mutation workload.
