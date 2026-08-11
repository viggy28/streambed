//go:build integration

package integration

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/pipeline"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/storage"
	"github.com/viggy28/streambed/internal/wal"
)

type benchConfig struct {
	Name          string
	FlushRows     int
	FlushInterval time.Duration
}

type benchResult struct {
	Config       string
	Rows         int
	SyncDuration time.Duration
	RPS          float64
	Flushes      int
	AvgFlushMs   float64
	P99FlushMs   int64
	DataBytes    int64
	InsertTime   time.Duration
}

// flushTracker is a slog.Handler that captures "flush completed" events from
// the writer. We use it instead of polling the Iceberg snapshot because:
//   - Microsecond-accurate timestamps (vs interval granularity for polling).
//   - Zero MinIO contention (polling DuckDB COUNT(*) hits the same MinIO the
//     pipeline is writing to).
//   - Bonus per-flush metrics (rows, duration_ms, data_bytes).
//
// IMPORTANT: This handler depends on the log format in internal/iceberg/writer.go
// (message "flush completed" and attribute keys rows/deletes/data_bytes/duration_ms).
// If you change that log line, update this handler too.
type flushTracker struct {
	mu            sync.Mutex
	events        []flushEvent
	cumRows       int64
	cumDeletes    int64
	firstAt       time.Time
	lastAt        time.Time
	target        int64
	targetDeletes int64
	doneAt        time.Time
	doneCh        chan struct{}
	doneOnce      sync.Once
}

type flushEvent struct {
	At         time.Time
	Rows       int64
	Deletes    int64
	DataBytes  int64
	DurationMs int64
}

func newFlushTracker(target int64) *flushTracker {
	return newMutationFlushTracker(target, -1)
}

func newMutationFlushTracker(targetRows, targetDeletes int64) *flushTracker {
	return &flushTracker{
		target:        targetRows,
		targetDeletes: targetDeletes,
		doneCh:        make(chan struct{}),
	}
}

func (f *flushTracker) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelInfo
}

func (f *flushTracker) Handle(_ context.Context, r slog.Record) error {
	if r.Message != "flush completed" {
		return nil
	}
	ev := flushEvent{At: r.Time}
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "rows":
			ev.Rows = a.Value.Int64()
		case "deletes":
			ev.Deletes = a.Value.Int64()
		case "data_bytes":
			ev.DataBytes = a.Value.Int64()
		case "duration_ms":
			ev.DurationMs = a.Value.Int64()
		}
		return true
	})

	f.mu.Lock()
	if f.firstAt.IsZero() {
		f.firstAt = ev.At
	}
	f.cumRows += ev.Rows
	f.cumDeletes += ev.Deletes
	f.lastAt = ev.At
	f.events = append(f.events, ev)
	reached := f.cumRows >= f.target && (f.targetDeletes < 0 || f.cumDeletes >= f.targetDeletes)
	if reached && f.doneAt.IsZero() {
		f.doneAt = ev.At
	}
	f.mu.Unlock()

	if reached {
		f.doneOnce.Do(func() { close(f.doneCh) })
	}
	return nil
}

func (f *flushTracker) WithAttrs(_ []slog.Attr) slog.Handler { return f }
func (f *flushTracker) WithGroup(_ string) slog.Handler      { return f }

// snapshot returns aggregate stats. Caller must not hold f.mu.
func (f *flushTracker) snapshot() (rows int64, flushes int, syncDur time.Duration, avgMs float64, p99Ms int64, bytes int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rows = f.cumRows
	flushes = len(f.events)
	if !f.firstAt.IsZero() && !f.doneAt.IsZero() {
		syncDur = f.doneAt.Sub(f.firstAt)
	}
	if flushes == 0 {
		return
	}
	durations := make([]int64, 0, flushes)
	var sum int64
	for _, e := range f.events {
		durations = append(durations, e.DurationMs)
		sum += e.DurationMs
		bytes += e.DataBytes
	}
	avgMs = float64(sum) / float64(flushes)
	// p99: simple sort, fine for benchmark scale.
	for i := 1; i < len(durations); i++ {
		for j := i; j > 0 && durations[j-1] > durations[j]; j-- {
			durations[j-1], durations[j] = durations[j], durations[j-1]
		}
	}
	idx := (len(durations) * 99) / 100
	if idx >= len(durations) {
		idx = len(durations) - 1
	}
	p99Ms = durations[idx]
	return
}

// benchShape controls how rows are partitioned across transactions. Shape
// affects commit-message overhead: every txn emits BEGIN + COMMIT messages
// the pipeline must decode and process, separately from the row payload.
type benchShape struct {
	Name    string
	TxnSize int // rows per transaction; 0 = all rows in one txn
}

var benchShapes = []benchShape{
	{Name: "bulk", TxnSize: 0},     // all rows in one txn — initial-load / bulk copy
	{Name: "batch", TxnSize: 1000}, // 1K rows per txn — batch ETL / nightly load
	{Name: "oltp", TxnSize: 10},    // 10 rows per txn — typical application workload
	{Name: "single", TxnSize: 1},   // 1 row per txn — high-frequency OLTP / worst case
}

// insertBenchRows inserts count rows into the given table. txnSize controls
// transaction shape: 0 means "all rows in one transaction" (original behavior);
// >0 means each transaction commits after txnSize rows. Returns elapsed time.
func insertBenchRows(t *testing.T, tableName string, count, txnSize int) time.Duration {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect for bench insert: %v", err)
	}
	defer conn.Close(ctx)

	start := time.Now()

	if txnSize <= 0 {
		insertOneTxn(t, ctx, conn, tableName, count)
	} else {
		insertManyTxns(t, ctx, conn, tableName, count, txnSize)
	}

	return time.Since(start)
}

// insertOneTxn wraps all inserts in a single BEGIN/COMMIT, batching 5000 rows
// per INSERT statement for speed.
func insertOneTxn(t *testing.T, ctx context.Context, conn *pgconn.PgConn, tableName string, count int) {
	t.Helper()
	r := conn.Exec(ctx, "BEGIN")
	if _, err := r.ReadAll(); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	for i := 0; i < count; i += 5000 {
		batchSize := 5000
		if i+batchSize > count {
			batchSize = count - i
		}
		sql := buildInsertSQL(tableName, i, batchSize)
		r := conn.Exec(ctx, sql)
		if _, err := r.ReadAll(); err != nil {
			t.Fatalf("insert rows at %d: %v", i, err)
		}
	}
	r = conn.Exec(ctx, "COMMIT")
	if _, err := r.ReadAll(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
}

// insertManyTxns issues count/txnSize separate transactions. Each is a single
// BEGIN; INSERT multi-row; COMMIT round-trip (one Exec call = one network
// round-trip, but PG still records it as a distinct transaction with its own
// BEGIN/COMMIT in WAL).
func insertManyTxns(t *testing.T, ctx context.Context, conn *pgconn.PgConn, tableName string, count, txnSize int) {
	t.Helper()
	for i := 0; i < count; i += txnSize {
		size := txnSize
		if i+size > count {
			size = count - i
		}
		var sb strings.Builder
		sb.WriteString("BEGIN; ")
		sb.WriteString(buildInsertSQL(tableName, i, size))
		sb.WriteString("; COMMIT")
		r := conn.Exec(ctx, sb.String())
		if _, err := r.ReadAll(); err != nil {
			t.Fatalf("txn at row %d: %v", i, err)
		}
	}
}

// buildInsertSQL builds "INSERT INTO t (name, value) VALUES (...),(...),..."
// for rows [startIdx, startIdx+n).
func buildInsertSQL(tableName string, startIdx, n int) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (name, value) VALUES ", tableName))
	for j := 0; j < n; j++ {
		if j > 0 {
			sb.WriteString(", ")
		}
		idx := startIdx + j
		sb.WriteString(fmt.Sprintf("('bench_%d', %d.%d)", idx, idx, idx%100))
	}
	return sb.String()
}

// runSyncBench runs the pipeline with a flushTracker attached to the writer's
// logger. Returns when the tracker observes >= expectedRows worth of flushes,
// or the timeout fires. The returned tracker contains per-flush metrics.
func runSyncBench(t *testing.T, ctx context.Context, expectedRows int, flushRowsCfg int, flushInterval time.Duration, timeout time.Duration) (*flushTracker, error) {
	t.Helper()
	return runSyncBenchWithMode(t, ctx, expectedRows, flushRowsCfg, flushInterval, timeout, iceberg.MutationModeCOW, nil, false, -1)
}

func runSyncBenchWithMode(t *testing.T, ctx context.Context, expectedRows int, flushRowsCfg int, flushInterval time.Duration, timeout time.Duration, mode iceberg.MutationMode, metrics *benchStorageMetrics, waitForStandbyAck bool, expectedDeletes int, statePaths ...string) (*flushTracker, error) {
	t.Helper()

	tracker := newMutationFlushTracker(int64(expectedRows), int64(expectedDeletes))
	// Writer's logger uses ONLY the tracker — we don't care about stderr noise
	// during a benchmark run, and routing through tracker keeps the hot path lean.
	writerLogger := slog.New(tracker)
	// The pipeline gets its own discard-level logger so its INFO output doesn't
	// confuse the tracker (which only matches "flush completed").
	pipelineLogger := slog.New(slog.NewTextHandler(noopWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))

	// State store. Multi-phase benchmarks pass one path so startup dedup can
	// reconcile against the prior phase's committed Iceberg LSN.
	statePath := t.TempDir() + "/state.db"
	if len(statePaths) > 0 && statePaths[0] != "" {
		statePath = statePaths[0]
	}
	stateStore, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open state store: %v", err)
	}
	defer stateStore.Close()

	// S3 client
	baseS3Client, err := storage.NewS3Client(ctx, s3Bucket, s3Region, minioEndpoint)
	if err != nil {
		t.Fatalf("create S3 client: %v", err)
	}
	var s3Client storage.ObjectStorage = baseS3Client
	if metrics != nil {
		metrics.inner = baseS3Client
		s3Client = metrics
	}

	// Postgres replication connection
	pgConn, err := pgconn.Connect(ctx, pgReplConnStr())
	if err != nil {
		t.Fatalf("connect to postgres for replication: %v", err)
	}
	defer pgConn.Close(context.Background())

	// Create publication
	if err := wal.CreatePublication(ctx, pgConn, slotName, nil, pipelineLogger); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	// Create or reuse replication slot
	slotLSN, err := wal.CreateOrReuseSlot(ctx, pgConn, slotName, pipelineLogger)
	if err != nil {
		t.Fatalf("setup replication slot: %v", err)
	}
	startLSN := slotLSN

	// Initialize Iceberg catalog
	catalog := iceberg.NewCatalog(s3Client, s3Bucket, s3Prefix)

	// Read per-table flush LSNs from Iceberg for dedup on restart.
	tableFlushLSN := make(map[string]pglogrepl.LSN)
	registeredTables, err := stateStore.GetRegisteredTables()
	if err != nil {
		t.Fatalf("get registered tables: %v", err)
	}
	for _, rt := range registeredTables {
		exists, err := catalog.TableExists(ctx, rt.Schema, rt.Table)
		if err != nil || !exists {
			continue
		}
		lsnStr, found, err := catalog.GetSnapshotFlushLSN(ctx, rt.Schema, rt.Table)
		if err != nil || !found {
			continue
		}
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			continue
		}
		tableFlushLSN[fmt.Sprintf("%s.%s", rt.Schema, rt.Table)] = lsn
	}

	// Writer with bench config and tracker-only logger
	writer := iceberg.NewWriter(catalog, s3Client, stateStore, slotName,
		flushRowsCfg, flushInterval, writerLogger, iceberg.WithMutationMode(mode))

	metaConn, err := pgx.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect metadata: %v", err)
	}
	defer metaConn.Close(context.Background())
	metaQuerier := wal.NewMetadataQuerier(metaConn)

	p := pipeline.New(pgConn, slotName, slotName, startLSN, nil,
		pipelineLogger, stateStore, tableFlushLSN, writer, flushInterval, metaQuerier)

	syncCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Start the clock *before* the pipeline launches so ramp-up time
	// (replication start, first WAL read, first flush build) is included in
	// the sync duration. The Handle() method has `if firstAt.IsZero()` so it
	// won't overwrite this.
	tracker.mu.Lock()
	tracker.firstAt = time.Now()
	tracker.mu.Unlock()

	pipelineDone := make(chan error, 1)
	go func() { pipelineDone <- p.Run(syncCtx) }()

	select {
	case <-tracker.doneCh:
		// Multi-phase benchmarks must let the pipeline send a standby status
		// after the durable flush. Cancelling immediately leaves the slot behind
		// and makes the measured mutation phase replay the entire baseline WAL.
		if waitForStandbyAck {
			select {
			case err := <-pipelineDone:
				return tracker, fmt.Errorf("pipeline exited before standby acknowledgement: %v", err)
			case <-time.After(11 * time.Second):
			}
		}
		cancel()
		<-pipelineDone
		return tracker, nil
	case err := <-pipelineDone:
		if syncCtx.Err() != nil {
			return tracker, fmt.Errorf("timeout after %v: cumRows=%d target=%d", timeout, tracker.cumRows, expectedRows)
		}
		return tracker, fmt.Errorf("pipeline exited unexpectedly: %v", err)
	}
}

// noopWriter discards all writes. Used to silence pipeline INFO output during benchmarks.
type noopWriter struct{}

func (noopWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestInsertThroughput measures INSERT rows-per-second at multiple scale points
// and flush configurations. Throughput is measured by tapping the writer's
// "flush completed" log events (see flushTracker comment).
func TestInsertThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput benchmark in short mode")
	}
	skipUnlessPerformanceTests(t)
	skipIfNotAvailable(t)

	configs := []benchConfig{
		{Name: "default", FlushRows: 500, FlushInterval: 5 * time.Second},
		{Name: "optimized", FlushRows: 5000, FlushInterval: 1 * time.Second},
		{Name: "bulk", FlushRows: 20000, FlushInterval: 30 * time.Second},
	}

	scales := []int{10_000, 50_000, 100_000, 500_000}

	var results []benchResult

	for _, cfg := range configs {
		for _, rowCount := range scales {
			name := fmt.Sprintf("%s/%dk", cfg.Name, rowCount/1000)
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()

				cleanup(t)
				clearS3Prefix(t)
				setupNamedTable(t, "bench_throughput")
				createSlotAndPublication(t)

				t.Logf("inserting %d rows (bulk shape: one txn)...", rowCount)
				insertTime := insertBenchRows(t, "bench_throughput", rowCount, 0)
				t.Logf("insert completed in %v", insertTime)

				// Timeout: at least 60s, or 1s per 1000 rows, capped at 10min.
				timeout := time.Duration(rowCount/1000) * time.Second
				if timeout < 60*time.Second {
					timeout = 60 * time.Second
				}
				if timeout > 10*time.Minute {
					timeout = 10 * time.Minute
				}

				t.Logf("syncing with config %s (flushRows=%d, flushInterval=%v)...",
					cfg.Name, cfg.FlushRows, cfg.FlushInterval)
				tracker, err := runSyncBench(t, ctx, rowCount, cfg.FlushRows, cfg.FlushInterval, timeout)
				if err != nil {
					t.Fatalf("sync failed: %v", err)
				}

				rows, flushes, syncDur, avgMs, p99Ms, bytes := tracker.snapshot()
				if flushes == 0 {
					t.Fatal("no flush events captured — the writer log format may have changed; see flushTracker note")
				}
				rps := float64(rows) / syncDur.Seconds()

				results = append(results, benchResult{
					Config:       cfg.Name,
					Rows:         rowCount,
					SyncDuration: syncDur,
					RPS:          rps,
					Flushes:      flushes,
					AvgFlushMs:   avgMs,
					P99FlushMs:   p99Ms,
					DataBytes:    bytes,
					InsertTime:   insertTime,
				})

				t.Logf("result: %d rows in %v across %d flushes (%.0f RPS, avg %0.1fms, p99 %dms)",
					rows, syncDur.Round(time.Millisecond), flushes, rps, avgMs, p99Ms)

				execSQL(t, "DROP TABLE IF EXISTS bench_throughput")
				cleanup(t)
			})
		}
	}

	if len(results) > 0 {
		t.Logf("\n=== Streambed INSERT Throughput ===")
		t.Logf("%-10s | %7s | %10s | %7s | %8s | %10s | %9s | %9s",
			"Config", "Rows", "Sync", "RPS", "Flushes", "Avg Flush", "P99 Flush", "MB/sec")
		t.Logf("%s", strings.Repeat("-", 90))
		for _, r := range results {
			mbPerSec := float64(r.DataBytes) / (1024 * 1024) / r.SyncDuration.Seconds()
			t.Logf("%-10s | %7d | %10v | %7.0f | %8d | %8.1fms | %7dms | %9.2f",
				r.Config, r.Rows, r.SyncDuration.Round(time.Millisecond),
				r.RPS, r.Flushes, r.AvgFlushMs, r.P99FlushMs, mbPerSec)
		}
	}
}

// TestInsertThroughputByShape varies transaction shape at a fixed scale and
// config to expose commit-message overhead. One giant txn emits ~N+2 pgoutput
// messages; N one-row txns emit ~3N messages — 3× more work for the decoder
// and pipeline loop, even though the row payload is identical.
func TestInsertThroughputByShape(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping shape benchmark in short mode")
	}
	skipUnlessPerformanceTests(t)
	skipIfNotAvailable(t)

	const rowCount = 1_000_000
	cfg := benchConfig{Name: "optimized", FlushRows: 5000, FlushInterval: 1 * time.Second}

	type shapeResult struct {
		Shape        string
		TxnSize      int
		InsertTime   time.Duration
		SyncDuration time.Duration
		RPS          float64
		Flushes      int
		AvgFlushMs   float64
		P99FlushMs   int64
	}
	var results []shapeResult

	for _, shape := range benchShapes {
		t.Run(shape.Name, func(t *testing.T) {
			ctx := context.Background()

			cleanup(t)
			clearS3Prefix(t)
			setupNamedTable(t, "bench_throughput")
			createSlotAndPublication(t)

			t.Logf("inserting %d rows, shape=%s (txnSize=%d)...",
				rowCount, shape.Name, shape.TxnSize)
			insertTime := insertBenchRows(t, "bench_throughput", rowCount, shape.TxnSize)
			t.Logf("insert completed in %v", insertTime)

			// Shapes with small txns take much longer to insert; generous timeout.
			timeout := 10 * time.Minute

			tracker, err := runSyncBench(t, ctx, rowCount, cfg.FlushRows, cfg.FlushInterval, timeout)
			if err != nil {
				t.Fatalf("sync failed: %v", err)
			}

			rows, flushes, syncDur, avgMs, p99Ms, _ := tracker.snapshot()
			if flushes == 0 {
				t.Fatal("no flush events captured")
			}
			rps := float64(rows) / syncDur.Seconds()

			results = append(results, shapeResult{
				Shape:        shape.Name,
				TxnSize:      shape.TxnSize,
				InsertTime:   insertTime,
				SyncDuration: syncDur,
				RPS:          rps,
				Flushes:      flushes,
				AvgFlushMs:   avgMs,
				P99FlushMs:   p99Ms,
			})

			t.Logf("result: shape=%s %d rows in %v (%.0f RPS, %d flushes)",
				shape.Name, rows, syncDur.Round(time.Millisecond), rps, flushes)

			execSQL(t, "DROP TABLE IF EXISTS bench_throughput")
			cleanup(t)
		})
	}

	if len(results) > 0 {
		t.Logf("\n=== Streambed INSERT Throughput by Transaction Shape (rows=%d, config=%s) ===",
			rowCount, cfg.Name)
		t.Logf("%-7s | %8s | %11s | %10s | %7s | %8s | %10s | %9s",
			"Shape", "TxnSize", "PG Insert", "Sync", "RPS", "Flushes", "Avg Flush", "P99 Flush")
		t.Logf("%s", strings.Repeat("-", 90))
		for _, r := range results {
			t.Logf("%-7s | %8d | %11v | %10v | %7.0f | %8d | %8.1fms | %7dms",
				r.Shape, r.TxnSize,
				r.InsertTime.Round(time.Millisecond),
				r.SyncDuration.Round(time.Millisecond),
				r.RPS, r.Flushes, r.AvgFlushMs, r.P99FlushMs)
		}
	}
}

// updateBenchRows updates all count rows in tableName by primary key.
// txnSize controls transaction shape: 0 = one UPDATE statement touching all rows
// (one txn); >0 = commits every txnSize rows. Each updated row emits one UPDATE
// event in the WAL, which drives the writer's copy-on-write merge path.
func updateBenchRows(t *testing.T, tableName string, count, txnSize int) time.Duration {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect for bench update: %v", err)
	}
	defer conn.Close(ctx)

	start := time.Now()

	if txnSize <= 0 {
		// All rows in one transaction, single statement. Simplest shape for
		// the writer: 1M UPDATE events in a single COMMIT.
		r := conn.Exec(ctx, "BEGIN")
		if _, err := r.ReadAll(); err != nil {
			t.Fatalf("BEGIN: %v", err)
		}
		sql := fmt.Sprintf("UPDATE %s SET name = name || '_u', value = value + 1.0 WHERE id <= %d", tableName, count)
		r = conn.Exec(ctx, sql)
		if _, err := r.ReadAll(); err != nil {
			t.Fatalf("UPDATE all: %v", err)
		}
		r = conn.Exec(ctx, "COMMIT")
		if _, err := r.ReadAll(); err != nil {
			t.Fatalf("COMMIT: %v", err)
		}
	} else {
		// count/txnSize transactions, each updating a contiguous id range.
		for i := 0; i < count; i += txnSize {
			size := txnSize
			if i+size > count {
				size = count - i
			}
			lo := i + 1 // SERIAL starts at 1
			hi := i + size
			var sb strings.Builder
			sb.WriteString("BEGIN; ")
			sb.WriteString(fmt.Sprintf("UPDATE %s SET name = name || '_u', value = value + 1.0 WHERE id BETWEEN %d AND %d",
				tableName, lo, hi))
			sb.WriteString("; COMMIT")
			r := conn.Exec(ctx, sb.String())
			if _, err := r.ReadAll(); err != nil {
				t.Fatalf("txn at row %d: %v", i, err)
			}
		}
	}
	return time.Since(start)
}

// TestUpdateThroughputByShape measures UPDATE rows-per-second by transaction
// shape. Unlike TestInsertThroughputByShape, each flush here exercises the
// writer's copy-on-write merge path: read existing Parquet, filter deleted
// rows, dedup new rows by key, rewrite combined result. The CoW cost is the
// headline reason UPDATE throughput is materially lower than INSERT at the
// same WAL volume.
//
// Protocol for each shape:
//  1. Bulk-insert rowCount baseline rows (single txn, fastest shape).
//  2. Drain the INSERT backlog — setup cost, NOT measured.
//  3. UPDATE every row once, partitioned into transactions per shape.
//  4. Drain the UPDATE backlog — this is the measured phase.
func TestUpdateThroughputByShape(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping update shape benchmark in short mode")
	}
	skipUnlessPerformanceTests(t)
	skipIfNotAvailable(t)

	const rowCount = 1_000_000
	cfg := benchConfig{Name: "optimized", FlushRows: 5000, FlushInterval: 1 * time.Second}

	type shapeResult struct {
		Shape        string
		TxnSize      int
		UpdateTime   time.Duration
		SyncDuration time.Duration
		RPS          float64
		Flushes      int
		AvgFlushMs   float64
		P99FlushMs   int64
	}
	var results []shapeResult

	for _, shape := range benchShapes {
		t.Run(shape.Name, func(t *testing.T) {
			ctx := context.Background()

			cleanup(t)
			clearS3Prefix(t)
			setupNamedTable(t, "bench_throughput")
			createSlotAndPublication(t)

			// Phase 1: insert baseline rows (one txn — fastest shape).
			t.Logf("inserting baseline %d rows...", rowCount)
			if d := insertBenchRows(t, "bench_throughput", rowCount, 0); d > 0 {
				t.Logf("baseline insert completed in %v", d)
			}

			// Phase 2: drain the INSERT backlog. NOT measured — setup cost.
			t.Logf("draining INSERT backlog (setup, not measured)...")
			if _, err := runSyncBench(t, ctx, rowCount, cfg.FlushRows, cfg.FlushInterval, 10*time.Minute); err != nil {
				t.Fatalf("baseline sync failed: %v", err)
			}

			// Phase 3: UPDATE every row once, shape-partitioned.
			t.Logf("updating %d rows, shape=%s (txnSize=%d)...",
				rowCount, shape.Name, shape.TxnSize)
			updateTime := updateBenchRows(t, "bench_throughput", rowCount, shape.TxnSize)
			t.Logf("update completed in %v", updateTime)

			// Phase 4: drain the UPDATE backlog. THIS is the measurement.
			tracker, err := runSyncBench(t, ctx, rowCount, cfg.FlushRows, cfg.FlushInterval, 10*time.Minute)
			if err != nil {
				t.Fatalf("update sync failed: %v", err)
			}

			rows, flushes, syncDur, avgMs, p99Ms, _ := tracker.snapshot()
			if flushes == 0 {
				t.Fatal("no flush events captured")
			}
			rps := float64(rows) / syncDur.Seconds()

			results = append(results, shapeResult{
				Shape:        shape.Name,
				TxnSize:      shape.TxnSize,
				UpdateTime:   updateTime,
				SyncDuration: syncDur,
				RPS:          rps,
				Flushes:      flushes,
				AvgFlushMs:   avgMs,
				P99FlushMs:   p99Ms,
			})

			t.Logf("result: shape=%s %d updates in %v (%.0f RPS, %d flushes)",
				shape.Name, rows, syncDur.Round(time.Millisecond), rps, flushes)

			execSQL(t, "DROP TABLE IF EXISTS bench_throughput")
			cleanup(t)
		})
	}

	if len(results) > 0 {
		t.Logf("\n=== Streambed UPDATE Throughput by Transaction Shape (rows=%d, config=%s) ===",
			rowCount, cfg.Name)
		t.Logf("%-7s | %8s | %11s | %10s | %7s | %8s | %10s | %9s",
			"Shape", "TxnSize", "PG Update", "Sync", "RPS", "Flushes", "Avg Flush", "P99 Flush")
		t.Logf("%s", strings.Repeat("-", 90))
		for _, r := range results {
			t.Logf("%-7s | %8d | %11v | %10v | %7.0f | %8d | %8.1fms | %7dms",
				r.Shape, r.TxnSize,
				r.UpdateTime.Round(time.Millisecond),
				r.SyncDuration.Round(time.Millisecond),
				r.RPS, r.Flushes, r.AvgFlushMs, r.P99FlushMs)
		}
	}
}
