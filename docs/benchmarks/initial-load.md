# Initial-Load Throughput

This scenario measures how fast Streambed can drain a pre-accumulated WAL
backlog into Iceberg — the "catch-up" or "backfill" case. All rows are inserted
into Postgres before the pipeline starts; the benchmark then records how long
the pipeline takes to land those rows in Iceberg.

Read [README.md](README.md) first for methodology, hardware, and caveats. All
numbers below were collected on an Apple M3 Max with localhost MinIO — see
the caveats section before generalizing them to other environments.

- **Tool**: [`test/integration/throughput_test.go`](../../test/integration/throughput_test.go)
- **Source commit**: `5b1c3de` (benchmarks may drift — re-run to verify)

## Scenario 1: Transaction shape

**Setup**: 1,000,000 rows, `optimized` config (`flush-rows=5000`,
`flush-interval=1s`), single run per shape.

The same 1,000,000 rows are inserted four ways, grouped into transactions of
different sizes. The pipeline sees the same row payload but a different number
of `BEGIN`/`COMMIT` pgoutput messages, exposing per-transaction overhead.

| Shape  | Txn size | PG insert | Sync    | **RPS**     | Flushes | Avg flush | P99 flush |
|--------|----------|-----------|---------|-------------|---------|-----------|-----------|
| bulk   | 1M       | 2.28s     | 7.10s   | **140,766** | 201     | 17.5ms    | 23ms      |
| batch  | 1,000    | 2.78s     | 6.06s   | **165,069** | 201     | 17.6ms    | 27ms      |
| oltp   | 10       | 38.53s    | 6.05s   | **165,403** | 202     | 18.2ms    | 25ms      |
| single | 1        | 6m04s     | 14.03s  | **71,259**  | 207     | 21.3ms    | 28ms      |

### Findings

**Bulk is not the fastest shape.** One giant transaction is ~17% slower than
1,000-row or 10-row transactions at 1M-row scale. Root cause is not yet
profiled; possibilities include pgoutput decoder behavior on very large
transactions, Go allocation patterns, or LSN bookkeeping pinning `pendingMinLSN`
for the entire transaction duration. Practical implication: backfills that
issue a single `COPY` may underperform the same workload split into 1,000-row
transactions.

**Small transactions have the expected 2–3× penalty.** The `single` shape at
1 row/txn emits ~3M pgoutput messages (vs ~1M for `bulk`), reflecting the
`BEGIN` + `INSERT` + `COMMIT` triple per row. Sync time is 2.3× slower, not
3× — indicating the pipeline amortizes part of the commit-message cost.

**PG insert time is fsync-bound on Docker Desktop.** The `oltp` (100K commits)
and `single` (1M commits) shapes both hit ~2,600–2,700 commits/sec on the PG
side — that's the APFS/virtiofs fsync ceiling, not streambed's limit. On native
Linux with NVMe the same workloads would insert 4–10× faster.

**Flush duration is tightly clustered.** Avg flush is 17–21ms across all
shapes; p99 is 23–28ms. The p99/avg ratio of ~1.3 is unrealistic for
production S3 — localhost MinIO has no tail latency from network, throttling,
or shared tenant load.

### Reproduction confirmation

A second run immediately after the first produced (shapes in the same order):

| Shape | Run 1 RPS | Run 2 RPS | Δ |
|-------|-----------|-----------|---|
| bulk  | 140,766   | 141,203   | +0.3% |
| batch | 165,069   | 163,924   | −0.7% |
| oltp  | 165,403   | 173,036   | +4.6% |

The bulk-vs-batch gap is stable across runs and not measurement noise.

## Reading these numbers

If you are tuning streambed:

- Default to `flush-rows=5000, flush-interval=1s` unless you have a specific
  reason to deviate. It is within 15% of max throughput for realistic
  workloads.
- For dedicated bulk loads where visibility latency is irrelevant, raise
  `flush-rows` to 20,000+ and `flush-interval` to 30s+ to minimize flush
  overhead. Measure before committing to this — the marginal gain is small at
  this scale.
- If you have the option on the producer side, prefer 100–1,000-row
  transactions over one giant `COPY`. Small commits (1–10 rows) are fine at
  streambed's end; the penalty is limited to very-high-frequency OLTP patterns.

If you are comparing against other tools:

- These numbers are a **local upper bound**, not a production number. Any
  real-world deployment will be slower than what you see here, mostly because
  of S3 latency and cross-AZ network.
- Do not compare against PeerDB / OLake published numbers without matching
  their environment (cloud region, instance type, storage backend, schema).
  Benchmarks published against real AWS are not comparable to these.
