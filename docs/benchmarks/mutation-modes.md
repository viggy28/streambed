# Copy-on-Write vs Merge-on-Read

This opt-in integration benchmark compares Iceberg mutation modes at logical
table targets of 10 MiB, 100 MiB, and 1 GiB. It updates 100 primary keys in one
flush, validates the result through DuckDB `iceberg_scan`, and records:

- CDC startup-to-commit and writer-only flush duration;
- successful logical `ObjectStorage` call counts/bytes during startup reconciliation and mutation commit (not physical HTTP request counts);
- median/p95 correctness-query duration across five query runs;
- retained data/delete object counts and physical bytes, including objects retained only by historical snapshots.

Targets describe generated source payload bytes, not compressed Parquet size.
The report includes actual physical file bytes.

## Run

Docker and at least 16 GiB of available memory are recommended for the 1 GiB
COW case because the current writer materializes the full table and replacement
Parquet file in memory.

```bash
docker compose -f test/integration/docker-compose.yml up -d --wait

STREAMBED_RUN_MUTATION_SIZE_BENCH=1 \
STREAMBED_MUTATION_BENCH_MAX_MIB=1024 \
STREAMBED_MUTATION_BENCH_OUTPUT=/tmp/streambed-mutation-benchmark.json \
STREAMBED_MUTATION_BENCH_REPETITIONS=3 \
go test -tags integration -v -timeout 90m \
  -run '^TestMutationModeByTableSize$' ./test/integration/...

docker compose -f test/integration/docker-compose.yml down -v
```

For a lower-resource smoke run, omit `STREAMBED_MUTATION_BENCH_MAX_MIB`; the
resource guard runs 10 MiB and 100 MiB and skips 1 GiB.

## 100 GiB model

When all three sizes are measured, the JSON report first fits the 10/100 MiB
points and predicts the measured 1 GiB holdout. A 100 GiB projection is emitted
only when relative holdout error is at most 20%. Each projection is labeled
`measured: false` and includes holdout error, R², slope, and intercept. No
100 GiB workload is executed.

Treat an emitted projection as directional only. Three local-MinIO sizes, one
100-row mutation batch, Parquet compression, cache effects, object-store latency,
and COW memory pressure all limit external validity. Modes alternate order and
each size/mode has at least three raw samples. CPU and RSS are intentionally not
fabricated; collect them with an external profiler when running the benchmark.

## Results

Measured 2026-08-05 on an Apple M3 Max (64 GiB RAM), local Docker Postgres 16
and MinIO, Go 1.26.1, DuckDB 1.5.5 with Iceberg extension `45163a28`. Values
are medians of three fresh runs. Exact ordered row digests validate workload
results, and every sample asserts its expected row/delete counts and flush count.

### Sparse UPDATE scaling

Each case updates 100 rows in one Iceberg flush.

| Logical table | Mode | Writer flush | CDC start→commit | GET bytes | PUT bytes | Query median | Query p95 |
|---:|:---:|---:|---:|---:|---:|---:|---:|
| 10 MiB | COW | 83 ms | 102 ms | 1.05 MiB | 1.03 MiB | 9 ms | 19 ms |
| 10 MiB | MOR | 24 ms | 27 ms | 7.4 KiB | 24.3 KiB | 10 ms | 26 ms |
| 100 MiB | COW | 357 ms | 360 ms | 10.28 MiB | 10.25 MiB | 20 ms | 61 ms |
| 100 MiB | MOR | 25 ms | 159 ms | 11.5 KiB | 25.7 KiB | 17 ms | 61 ms |
| 1 GiB | COW | 3,077 ms | 3,079 ms | 105.05 MiB | 104.84 MiB | 139 ms | 470 ms |
| 1 GiB | MOR | 45 ms | 48 ms | 52.2 KiB | 39.8 KiB | 40 ms | 402 ms |

At 1 GiB, MOR reduced flush time by **68.4×**, reads by **2,059×**, and writes
by **2,698×** for this sparse update.

### Workload-shape benchmark

These cases use a 100 MiB logical baseline. Mixed batches contain 40 INSERTs,
40 UPDATEs (including a key-changing update in the single mixed case), and 20
DELETEs. Repeated cases use the same 40/40/20 split per batch.

| Scenario | COW flush | MOR flush | Flush speedup | COW query median | MOR query median |
|:---|---:|---:|---:|---:|---:|
| INSERT 100 | 18 ms | 17 ms | 1.1× | 24 ms | 23 ms |
| UPDATE 100 | 362 ms | 23 ms | 15.7× | 42 ms | 27 ms |
| UPDATE 1% | 359 ms | 23 ms | 15.6× | 46 ms | 63 ms |
| UPDATE 10% | 364 ms | 42 ms | 8.7× | 50 ms | 516 ms |
| UPDATE 100% | 356 ms | 228 ms | 1.6× | 88 ms | 4,120 ms |
| DELETE 100 | 354 ms | 14 ms | 25.3× | 43 ms | 28 ms |
| DELETE 1% | 349 ms | 17 ms | 20.5× | 43 ms | 65 ms |
| DELETE 10% | 334 ms | 19 ms | 17.6× | 41 ms | 481 ms |
| DELETE 100% | 171 ms | 58 ms | 2.9× | 7 ms | 4,142 ms |
| Mixed 100 | 362 ms | 19 ms | 19.1× | 46 ms | 30 ms |
| Mixed × 10 batches | 3,505 ms | 141 ms | 24.9× | 48 ms | 58 ms |
| Mixed × 100 batches | 34,036 ms | 1,882 ms | 18.1× | 45 ms | 370 ms |

INSERT-only performance is effectively identical. MOR strongly improves write
latency for sparse updates/deletes and mixed workloads, but query cost rises
quickly with mutation density and delete-file accumulation. After 100 mixed
batches MOR retained 100 equality-delete files and query median was **8.2×**
COW, while COW had rewritten about 1.01 GiB of compressed data across the run.
No 1,000-batch run was performed; it remains an opt-in stress case rather than
a measured result.

Run the workload matrix with:

```bash
STREAMBED_RUN_MUTATION_WORKLOAD_BENCH=1 \
STREAMBED_MUTATION_WORKLOAD_BENCH_OUTPUT=/tmp/streambed-mutation-workloads.json \
go test -tags integration -count=1 -v -timeout 60m \
  -run '^TestMutationModeWorkloads$' ./test/integration/...
```

### 100 GiB extrapolation

The holdout-validated linear model produced these directional estimates. They
are **not measurements**:

| Mode | Writer flush | Logical GET | Logical PUT | Holdout result |
|:---:|---:|---:|---:|:---|
| COW | 302 s (~5m02s) | 10.26 GiB | 10.24 GiB | emitted; 3.0% time error |
| MOR | suppressed | 4.42 MiB | 1.55 MiB | time exceeded 20% threshold |

Physical bytes are much lower than logical table size because the deterministic
payload compresses well. MOR I/O projections reflect metadata/file-layout
growth for a fixed 100-row mutation and are directional only.
