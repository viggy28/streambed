# DuckLake logical equality indexes (V1)

Streambed can maintain a persistent logical index from one PostgreSQL `BIGINT` column to the DuckLake data files that contain each value. The original SQL predicate is still evaluated by DuckDB; the index only reduces the candidate file list.

## Usage

Logical indexes require the Streambed DuckLake extension build and a DuckDB-backed metadata catalog:

```bash
streambed sync \
  --target-format=ducklake \
  --ducklake-catalog-store=duckdb \
  --ducklake-extension=/opt/streambed/ducklake.duckdb_extension \
  --logical-index=public.events:user_id
```

`--logical-index` is repeatable. It creates or ensures the index; it is not required on later runs. The pinned `--ducklake-extension` **is** required on every writer run. Streambed refuses to open an indexed catalog for writing with the stock extension because stock DuckLake would publish files without index mappings.

Build the pinned extension from the Streambed DuckLake fork and its pinned submodules:

```bash
git submodule update --init --recursive
CMAKE_BUILD_PARALLEL_LEVEL=8 make release
# Artifact: build/release/extension/ducklake/ducklake.duckdb_extension
```

The extension is currently unsigned, so Streambed enables unsigned loading only on connections given an explicit extension path. Deploy the extension and Streambed binary together and record the artifact checksum.

V1 recognizes `column = BIGINT constant` and its commuted form. Ranges, `IN`, parameters, casts, expressions, strings, composite indexes, `NULL`, and historical snapshots use ordinary DuckLake pruning.

## Correctness and lifecycle

- Definitions have `BUILDING`, `READY`, or `INVALID` state. Only `READY` indexes prune files.
- A durable capability marker is loaded once at catalog attach, preserving maintenance across restarts without probing metadata on every non-indexed DuckLake commit.
- Backfill mappings and the transition to `READY` commit in one metadata transaction.
- New-file mappings commit atomically with DuckLake file metadata.
- Stale mappings are safe: candidates are intersected with snapshot-visible files and DuckDB still evaluates the predicate.
- Indexed tables persist `data_inlining_row_limit = 0`; existing inlined rows are flushed before backfill.
- Dropping an indexed table removes its definitions and mappings in the same Streambed transaction as the table drop.
- Dropping or changing the indexed column marks the definition `INVALID` before schema evolution.
- V1 assumes Streambed is the sole DuckLake writer. Do not write to an indexed catalog through stock DuckLake or another process.

Metadata is stored in the DuckLake catalog as one row per `(index_id, value, data_file_id)`. There is no cardinality cap. A completed Parquet file is scanned once for exact distinct indexed values before its short metadata-publication transaction.

## Validation evidence

The extension SQL logic test covers backfill rollback, normal and commuted equality, absent values, UPDATE replacement files, time-travel fallback, signed boundaries, incremental maintenance, idempotent creation, prepare-without-side-effects, invalidation, and cleanup. Streambed's extension-backed Go test covers failed-publication rollback, restart persistence, rejection of the stock extension, CDC-style key updates, schema invalidation, and table-drop cleanup. The segmented DuckLake debug suite passed except `concurrent_table_creation.test_slow`; the same snapshot-retry exhaustion reproduced on an isolated, unmodified upstream v1.5 worktree.

A public-boundary E2E run used the compiled `streambed` CLI, PostgreSQL logical replication, the psql-wire query endpoint, and the DuckLake metadata catalog. After three initial files, a PostgreSQL key update from `42` to `43` removed the old result, returned the updated row, and `EXPLAIN ANALYZE` reported `Total Files Read: 1`. Restarting Streambed without `--logical-index`, then inserting `777`, preserved the `READY` definition and created the new value mapping.

## Initial local benchmark

Apple Silicon debug build, local disk, 64 tiny files, 100 distinct values per file, with every file's min/max spanning the searched value:

| Measurement | Result |
|---|---:|
| Backfill, 6,400 mappings | 101.9 ms |
| Candidate files, unindexed | 64 |
| Candidate files, indexed | 1 |
| 200 point queries, unindexed | 28.47 s |
| 200 point queries, indexed | 34.89 s |
| Indexed incremental file, 10 distinct values | 79.7 ms |
| Indexed incremental file, 1,000 distinct values | 113.8 ms |
| Indexed incremental file, 10,000 distinct values | 194.3 ms |

A separate release-build microbenchmark isolated one-file publication cost over five repetitions:

| Distinct values/file | Unindexed average | Indexed average |
|---:|---:|---:|
| 10 | 3.24 ms | 3.20 ms |
| 1,000 | 3.27 ms | 4.23 ms |
| 10,000 | 3.68 ms | 10.15 ms |

The index proved file pruning, but it was slower for these tiny local files because metadata lookup dominated cheap local reads. Object-storage tests with realistic file sizes are required before making a latency claim. This tradeoff—and DuckLake's lack of a public third-party pruning hook—is important context for a future technical write-up.
