# UPDATE Throughput by Transaction Shape

This scenario measures UPDATE rows-per-second landed in Iceberg, varied by
Postgres transaction shape. Unlike the INSERT benchmark, every flush here
exercises the writer's copy-on-write (CoW) merge path — the hot code path
that makes row-level mutations possible on an immutable file format.

Read [README.md](README.md) first for methodology, hardware, and caveats.

- **Tool**: [`test/integration/throughput_test.go`](../../test/integration/throughput_test.go) — `TestUpdateThroughputByShape`
- **Source commit**: run `git rev-parse HEAD` when reproducing

## What the CoW merge path does

On every flush that contains UPDATEs (or DELETEs) the writer:

1. **Reads** the existing Parquet file for the table from S3.
2. **Filters** out rows whose primary key matches a pending DELETE.
3. **Dedups** against pending UPDATEs — the newest version of each key wins.
4. **Writes** a new Parquet file containing the merged result.
5. **Commits** a new Iceberg snapshot pointing at the new file.

Step 1 is the main cost that INSERT-only workloads never pay. At 1M rows per
table, a single Parquet file is ~15–25 MB; reading it on every flush adds
meaningful overhead compared to the INSERT-only fast path which just appends
a new file.

## Protocol

Each shape runs the same protocol:

1. Insert 1,000,000 baseline rows into `bench_throughput` (one-txn / bulk —
   fastest shape, keeps setup time minimal).
2. Drain the INSERT backlog. **Not measured** — setup only.
3. UPDATE every row exactly once, partitioned into transactions per the shape.
4. Drain the UPDATE backlog. **This is the measurement** — elapsed time from
   pipeline start through the flush that lands row 1,000,000.

Each row update modifies two columns (`name`, `value`) so every UPDATE event
carries a non-trivial payload. The primary key `id` is unchanged, keeping the
CoW merge aligned on its natural dedup column.

## Results

Run the benchmark and paste the table here. Example format (replace with
your actual numbers):

| Shape  | Txn size | PG update | Sync    | **RPS**    | Flushes | Avg flush | P99 flush |
|--------|----------|-----------|---------|------------|---------|-----------|-----------|
| bulk   | 1M       | _t.b.f._  | _t.b.f._| **_t.b.f._** | _t.b.f._ | _t.b.f._  | _t.b.f._  |
| batch  | 1,000    | _t.b.f._  | _t.b.f._| **_t.b.f._** | _t.b.f._ | _t.b.f._  | _t.b.f._  |
| oltp   | 10       | _t.b.f._  | _t.b.f._| **_t.b.f._** | _t.b.f._ | _t.b.f._  | _t.b.f._  |
| single | 1        | _t.b.f._  | _t.b.f._| **_t.b.f._** | _t.b.f._ | _t.b.f._  | _t.b.f._  |

_t.b.f. = to be filled in on your first run._

## What to look for

**Compared to INSERT at the same shape:** UPDATE throughput should be lower,
often by 2–5×. The gap is the CoW read-merge-rewrite cost that INSERT never
pays. If UPDATE RPS matches INSERT RPS, the CoW path is almost certainly
broken (e.g., skipping the read and producing a file that only contains the
new rows, losing everything that was there before).

**Ratio between shapes should mirror INSERT:** if `batch` is ~2× faster than
`single` for INSERT, the same gap should show up for UPDATE. The CoW overhead
is a per-flush cost, not a per-transaction cost, so transaction shape still
matters but in the same way it does for INSERT.

**Flush duration tail:** watch P99 flush time. A single flush that reads a
large existing Parquet, does the merge, and writes a new one can be several
hundred milliseconds. If P99 grows roughly linearly with existing file size
across runs, that is the CoW cost — not a regression.

## Reproducing

```bash
docker compose -f test/integration/docker-compose.yml up -d --wait

go test -tags integration -v -timeout 1200s \
    -run TestUpdateThroughputByShape ./test/integration/...

docker compose -f test/integration/docker-compose.yml down -v
```

The benchmark takes roughly 2× the time of `TestInsertThroughputByShape`
because each shape does both an INSERT-drain (setup) and an UPDATE-drain
(measurement). Plan for ~15 minutes on the reference hardware.

## Caveats specific to this benchmark

Everything in the top-level [caveats section](README.md#caveats) applies,
plus:

1. **`REPLICA IDENTITY` defaults.** The baseline table uses the default
   `REPLICA IDENTITY` (index = primary key). UPDATE events therefore include
   the PK as the "old tuple" and the full new row as the "new tuple". Tables
   configured with `REPLICA IDENTITY FULL` emit the full old tuple too,
   doubling the WAL volume per UPDATE. A separate benchmark would be needed
   to capture that configuration's impact.

2. **One UPDATE per row, not N.** Each row is updated exactly once. Real
   workloads with repeated UPDATEs on the same key hit the writer's in-buffer
   dedup (collapsing N updates to the final value) before any CoW merge
   happens. That is a different (and faster) code path than what this test
   exercises.

3. **Single table, single file.** After the baseline INSERT, Iceberg holds
   one (or a few) Parquet files. Production tables with many files
   accumulated across dozens of snapshots have more-expensive CoW merges
   because the writer must read across all current data files.
