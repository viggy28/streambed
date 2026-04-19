# Streambed Benchmarks

This directory contains throughput benchmarks for the Streambed pipeline.
Numbers here are intended for relative comparison (this config vs. that config,
this release vs. the next), not for absolute marketing claims against other
products — see [caveats](#caveats) below.

## Results

- [initial-load.md](initial-load.md) — INSERT throughput when draining an
  accumulated WAL backlog (initial-load / backfill scenario). Varies flush
  config and transaction shape.
- [update-shape.md](update-shape.md) — UPDATE throughput by transaction
  shape. Exercises the writer's copy-on-write merge path.

Planned:
- `steady-state.md` — sustained INSERT rate under continuous load.

## Methodology

### What is measured

Each benchmark measures **rows per second (RPS)** landed in Iceberg, where
"landed" means the row is visible via an Iceberg snapshot — i.e., the parquet
is uploaded and the metadata commit has completed. This is what an end-user
querying the Iceberg table will see.

### How it is measured

The pipeline's writer emits a `flush completed` log entry after every successful
Iceberg commit. The benchmark taps this log stream via a custom `slog.Handler`
(`flushTracker` in [throughput_test.go](../../test/integration/throughput_test.go))
rather than polling the Iceberg snapshot. Advantages:

- Microsecond-accurate timestamps, no polling-interval lag.
- Zero MinIO contention from measurement traffic.
- Per-flush metrics (rows, duration, bytes) available for free.

The measurement window starts when the pipeline goroutine launches and ends on
the flush whose cumulative row count first crosses the expected total. This
includes ramp-up (replication handshake, first WAL read, first buffer fill).

> NOTE: the writer's log format is load-bearing for this benchmark. If the
> message string `"flush completed"` or the attribute keys (`rows`, `deletes`,
> `data_bytes`, `duration_ms`) change, update the `flushTracker` handler in
> `throughput_test.go`. A comment in [`internal/iceberg/writer.go`](../../internal/iceberg/writer.go)
> calls this out at the log site.

### Reproducing

Requires Docker running.

```bash
# Bring up the test stack (Postgres 16 on :5434, MinIO on :9002).
docker compose -f test/integration/docker-compose.yml up -d --wait

# Run all benchmarks.
go test -tags integration -v -timeout 1200s -run TestInsertThroughput ./test/integration/...

# Or a single scenario.
go test -tags integration -v -timeout 1200s -run TestInsertThroughputByShape ./test/integration/...

# Tear down.
docker compose -f test/integration/docker-compose.yml down -v
```

### Hardware / environment disclosure

The results under this directory were collected on:

- **Machine**: MacBook Pro, Apple M3 Max (12 performance + 4 efficiency cores), 64 GB RAM
- **OS**: macOS 14.3 (Darwin 23.3.0)
- **Docker**: Docker Desktop 27.4.0 (VZ2 backend, APFS virtiofs)
- **Go**: 1.26.1 darwin/arm64
- **Postgres**: 16 (official image, `wal_level=logical`, `synchronous_commit=on`)
- **Storage**: MinIO (official image), localhost, single-node
- **Network**: loopback only (all components co-located on one machine)

Future runs against real S3 / different hardware should add a section at the
top of each result file identifying the environment used.

## Caveats

These benchmarks are honest about what they do *not* measure:

1. **Localhost MinIO, not real S3.** MinIO on loopback has sub-millisecond PUT
   latency; AWS S3 PUTs are typically 20–50ms. The per-flush overhead on real
   S3 is substantially higher than what these numbers show. The ratio between
   configs/shapes should still be informative, but absolute RPS will not match
   production.

2. **Docker Desktop on macOS, not Linux native.** The APFS filesystem overlay
   and virtiofs shim add I/O overhead. Postgres `fsync` throughput caps
   around 2,500–3,000 commits/sec on this setup vs. 10,000–30,000/sec on
   native Linux with NVMe. Small-transaction-shape benchmarks are especially
   affected on the insert side.

3. **All components on one machine.** Zero network between Postgres, streambed,
   and MinIO. In production, Postgres and the pipeline are typically in
   different availability zones (~1ms RTT) or regions (~30ms+), and object
   storage is remote. The WAL replication path eats this latency.

4. **Proto version 1 of pgoutput.** Streambed currently requests
   `proto_version '1'` from Postgres logical replication, which means entire
   transactions are emitted only at `COMMIT`. Proto version 2 (Postgres 14+)
   with `streaming 'on'` would stream large transactions mid-commit — changing
   the performance profile of very large transactions. This is a known
   limitation, not a benchmark artifact.

5. **Initial-load, not steady-state CDC.** These benchmarks insert rows into
   Postgres first, then start the pipeline to drain the accumulated WAL
   backlog. This measures *catch-up throughput*. Real CDC workloads run the
   pipeline continuously against in-progress transactions, which has a
   different bottleneck profile (per-flush overhead, idle waits, ack cadence).
   A steady-state benchmark is planned.

6. **Single-run precision.** Each point is a single run, not an average of N.
   Expect ±1–5% variance between runs. Multi-run medians are a future
   improvement.

7. **Synthetic schema.** The benchmark uses a trivial 3-column table
   (`id SERIAL`, `name TEXT`, `value DOUBLE PRECISION`). Wide tables, JSONB
   columns, arrays, timestamps, and NULL-heavy columns all stress different
   code paths and will produce different numbers.

## Tuning knobs

The benchmarks vary two dimensions that map to CLI flags:

- `--flush-rows` (default 10,000): flush when the buffer reaches this many
  rows. Larger = better batching amortization, higher memory use, higher
  visibility latency.
- `--flush-interval` (default 2s): flush at least this often even if the row
  threshold is not met. Lower = tighter visibility latency, more per-flush
  overhead on small batches.

The configs tested:

| Config      | `flush-rows` | `flush-interval` | Use case |
|-------------|--------------|------------------|----------|
| `default`   | 500          | 5s               | Matches existing integration test defaults |
| `optimized` | 5,000        | 1s               | Balanced throughput/latency for typical workloads |
| `bulk`      | 20,000       | 30s              | Max throughput for bulk loads where visibility latency doesn't matter |

And four transaction shapes on the Postgres side:

| Shape    | Rows/txn | Scenario |
|----------|----------|----------|
| `bulk`   | all      | One giant transaction — initial load via `COPY` or `INSERT ... SELECT` |
| `batch`  | 1,000    | Batch ETL / nightly load |
| `oltp`   | 10       | Typical application workload |
| `single` | 1        | High-frequency OLTP / worst case |
