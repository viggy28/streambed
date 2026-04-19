// Package simtest ties the oracle, workloads, supervisor, and metrics
// together into a single long-lived runner.
package simtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/viggy28/streambed/internal/simtest/chaos"
	"github.com/viggy28/streambed/internal/simtest/metrics"
	"github.com/viggy28/streambed/internal/simtest/oracle"
	"github.com/viggy28/streambed/internal/simtest/supervisor"
	"github.com/viggy28/streambed/internal/simtest/workload"
)

// Options describes a simtest run.
type Options struct {
	// Postgres
	SourceURL string

	// S3 / MinIO
	S3Bucket   string
	S3Prefix   string
	S3Endpoint string // e.g. "http://localhost:9004"
	S3Region   string

	// Streambed subprocess
	StreambedBin  string   // path to streambed binary
	StreambedArgs []string // args to pass (sync subcommand + flags)
	SlotName      string   // slot name streambed uses — needed for lag queries

	// Oracle
	ValidateInterval time.Duration
	SentinelTimeout  time.Duration

	// Chaos
	ChaosEnabled     bool
	ChaosKillEvery   time.Duration
	ChaosPauseMinio  func(ctx context.Context) error // optional: pause MinIO for N seconds
	ChaosPauseEvery  time.Duration

	// Runtime
	Duration  time.Duration // 0 = forever
	LogDir    string
	Heartbeat time.Duration

	// Workloads
	Workloads []workload.Workload

	// Invariants — self-consistency checks evaluated on Iceberg alone after
	// the per-table sentinel gate has succeeded.
	Invariants []oracle.Invariant
}

// Run launches the simtest components and blocks until ctx is cancelled or
// the optional duration elapses. Returns a final summary.
type Summary struct {
	Uptime            time.Duration
	StreambedRestarts int64
	OracleChecks      int64
	OracleFailures    int64
	FinalResult       oracle.Result
}

func Run(ctx context.Context, opts Options, logger *slog.Logger) (Summary, error) {
	if err := validateOptions(&opts); err != nil {
		return Summary{}, err
	}

	if err := os.MkdirAll(opts.LogDir, 0755); err != nil {
		return Summary{}, fmt.Errorf("create log dir: %w", err)
	}

	runStamp := time.Now().UTC().Format("2006-01-02T15-04-05Z")
	metricsPath := filepath.Join(opts.LogDir, fmt.Sprintf("run-%s.jsonl", runStamp))
	streambedLogPath := filepath.Join(opts.LogDir, fmt.Sprintf("streambed-%s.log", runStamp))

	coll, err := metrics.New(metricsPath, logger)
	if err != nil {
		return Summary{}, fmt.Errorf("metrics: %w", err)
	}
	defer coll.Close()
	logger.Info("simtest metrics file", "path", metricsPath)

	runCtx, cancel := context.WithCancel(ctx)
	if opts.Duration > 0 {
		var c2 context.CancelFunc
		runCtx, c2 = context.WithTimeout(runCtx, opts.Duration)
		defer c2()
	}
	defer cancel()

	// 1. Setup DuckDB for the oracle (Iceberg reader).
	duckDB, err := openDuckDB(opts)
	if err != nil {
		return Summary{}, fmt.Errorf("duckdb: %w", err)
	}
	defer duckDB.Close()

	// 2. Setup each workload's tables via a regular pgx connection.
	if err := setupWorkloads(runCtx, opts, logger); err != nil {
		return Summary{}, fmt.Errorf("workload setup: %w", err)
	}

	// 3. Build validator spec from all workloads' tables.
	tables := collectTables(opts.Workloads)
	validator := &oracle.Validator{
		PgConnStr:       opts.SourceURL,
		DuckDB:          duckDB,
		S3Bucket:        opts.S3Bucket,
		S3Prefix:        opts.S3Prefix,
		Tables:          tables,
		Invariants:      opts.Invariants,
		SentinelTimeout: opts.SentinelTimeout,
		Logger:          logger,
	}

	// 4. Start supervisor for streambed subprocess.
	supStart := time.Now()
	sup := supervisor.New(supervisor.Config{
		Binary:    opts.StreambedBin,
		Args:      opts.StreambedArgs,
		LogPath:   streambedLogPath,
		OnRestart: coll.RecordStreambedRestart,
		Logger:    logger,
	})
	supErrCh := make(chan error, 1)
	go func() { supErrCh <- sup.Run(runCtx) }()
	_ = supStart // reserved for future "time-until-first-flush" metric

	// 5. Start workload goroutines.
	var wlWg sync.WaitGroup
	for _, w := range opts.Workloads {
		wlWg.Add(1)
		go func(w workload.Workload) {
			defer wlWg.Done()
			runWorkload(runCtx, w, opts, coll, logger)
		}(w)
	}

	// 6. Start metrics heartbeat + replication-lag sampler.
	coll.StartHeartbeat(runCtx, opts.Heartbeat)
	go sampleReplicationLag(runCtx, opts, coll, logger)

	// 7. Start oracle validator loop.
	validatorDone := make(chan struct{})
	var lastResult oracle.Result
	var resultMu sync.Mutex
	go func() {
		defer close(validatorDone)
		tickID := int64(0)
		// Small initial delay so streambed has a moment to connect.
		select {
		case <-runCtx.Done():
			return
		case <-time.After(5 * time.Second):
		}
		ticker := time.NewTicker(opts.ValidateInterval)
		defer ticker.Stop()
		// Run one oracle tick right away.
		runOracleTick(runCtx, validator, coll, logger, &resultMu, &lastResult, &tickID)
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				runOracleTick(runCtx, validator, coll, logger, &resultMu, &lastResult, &tickID)
			}
		}
	}()

	// 8. Start chaos (optional).
	if opts.ChaosEnabled {
		go chaos.Run(runCtx, chaos.Config{
			Sup:          sup,
			KillEvery:    opts.ChaosKillEvery,
			PauseMinioFn: opts.ChaosPauseMinio,
			PauseEvery:   opts.ChaosPauseEvery,
		}, logger)
	}

	// 9. Wait for shutdown.
	<-runCtx.Done()
	logger.Info("simtest shutdown initiated")

	// Wait for workloads to drain.
	wlWg.Wait()
	// Wait for supervisor (streambed subprocess teardown).
	select {
	case <-supErrCh:
	case <-time.After(10 * time.Second):
		logger.Warn("supervisor did not exit within 10s — killing streambed")
		_ = sup.Kill()
		<-supErrCh
	}
	// Wait for validator to finish its current tick.
	select {
	case <-validatorDone:
	case <-time.After(2 * opts.ValidateInterval):
	}

	// One last heartbeat to ensure final counts are written.
	coll.Heartbeat()

	resultMu.Lock()
	final := lastResult
	resultMu.Unlock()

	snap := coll.Snapshot()
	return Summary{
		Uptime:            snap.Uptime,
		StreambedRestarts: snap.StreambedRestarts,
		OracleChecks:      snap.OracleChecks,
		OracleFailures:    snap.OracleFailures,
		FinalResult:       final,
	}, nil
}

func runOracleTick(ctx context.Context, v *oracle.Validator, coll *metrics.Collector, logger *slog.Logger, mu *sync.Mutex, last *oracle.Result, tickID *int64) {
	*tickID++
	res := v.RunOnce(ctx, *tickID)

	totalDisc := 0
	for _, rep := range res.Reports {
		totalDisc += len(rep.Discrepancies)
	}
	catchUpOK := res.CatchUpErr == ""
	durationMs := res.FinishedAt.Sub(res.StartedAt).Milliseconds()

	coll.RecordOracle(*tickID, durationMs, totalDisc, catchUpOK, len(res.InvariantErr))

	if !res.OK() {
		logger.Warn("oracle tick NOT OK",
			"tick_id", *tickID,
			"catch_up_err", res.CatchUpErr,
			"discrepancies", totalDisc,
			"invariant_fails", len(res.InvariantErr))
		for _, rep := range res.Reports {
			if len(rep.Discrepancies) == 0 {
				continue
			}
			missing, extra, mismatch := rep.Counts()
			logger.Warn("oracle discrepancy",
				"table", fmt.Sprintf("%s.%s", rep.Schema, rep.Table),
				"pg_rows", rep.PgRowCount, "iceberg_rows", rep.IceRowCount,
				"missing", missing, "extra", extra, "mismatch", mismatch)
		}
		for name, msg := range res.InvariantErr {
			logger.Warn("oracle invariant failed", "name", name, "error", msg)
		}
	} else {
		logger.Info("oracle tick OK",
			"tick_id", *tickID, "duration_ms", durationMs,
			"tables", len(res.Reports))
	}

	mu.Lock()
	*last = res
	mu.Unlock()
}

func runWorkload(ctx context.Context, w workload.Workload, opts Options, coll *metrics.Collector, logger *slog.Logger) {
	wLogger := logger.With("workload", w.Name())
	conn, err := pgx.Connect(ctx, opts.SourceURL)
	if err != nil {
		wLogger.Error("connect postgres", "error", err)
		return
	}
	defer conn.Close(context.Background())

	wLogger.Info("workload starting")
	if err := w.Run(ctx, conn, coll, wLogger); err != nil && !errors.Is(err, context.Canceled) {
		wLogger.Error("workload exited with error", "error", err)
		return
	}
	wLogger.Info("workload stopped")
}

func sampleReplicationLag(ctx context.Context, opts Options, coll *metrics.Collector, _ *slog.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		func() {
			c, err := pgconn.Connect(ctx, opts.SourceURL)
			if err != nil {
				return
			}
			defer c.Close(ctx)
			lag, err := metrics.QueryReplicationLag(ctx, c, opts.SlotName)
			if err != nil {
				// Slot may not exist yet on first connect — log once in a while.
				return
			}
			coll.RecordReplicationLag(lag)
		}()
	}
}

func setupWorkloads(ctx context.Context, opts Options, logger *slog.Logger) error {
	conn, err := pgx.Connect(ctx, opts.SourceURL)
	if err != nil {
		return fmt.Errorf("setup connect: %w", err)
	}
	defer conn.Close(context.Background())
	for _, w := range opts.Workloads {
		if err := w.Setup(ctx, conn, logger); err != nil {
			return fmt.Errorf("setup %s: %w", w.Name(), err)
		}
	}
	return nil
}

func collectTables(ws []workload.Workload) []oracle.TableSpec {
	var out []oracle.TableSpec
	for _, w := range ws {
		out = append(out, w.Tables()...)
	}
	return out
}

func validateOptions(opts *Options) error {
	if opts.SourceURL == "" {
		return errors.New("source-url required")
	}
	if opts.S3Bucket == "" {
		return errors.New("s3-bucket required")
	}
	if opts.StreambedBin == "" {
		return errors.New("streambed-bin required")
	}
	if opts.SlotName == "" {
		opts.SlotName = "streambed_simtest"
	}
	if opts.ValidateInterval == 0 {
		opts.ValidateInterval = 30 * time.Second
	}
	if opts.SentinelTimeout == 0 {
		opts.SentinelTimeout = 60 * time.Second
	}
	if opts.Heartbeat == 0 {
		opts.Heartbeat = 30 * time.Second
	}
	if opts.LogDir == "" {
		opts.LogDir = "./simtest-logs"
	}
	if len(opts.Workloads) == 0 {
		return errors.New("at least one workload required")
	}
	return nil
}

// openDuckDB opens a DuckDB connection configured to read Iceberg from the
// configured S3 / MinIO endpoint.
func openDuckDB(opts Options) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	endpoint := trimScheme(opts.S3Endpoint)
	useSSL := "true"
	if opts.S3Endpoint == "" || isLocalEndpoint(opts.S3Endpoint) {
		useSSL = "false"
	}
	stmts := []string{
		"INSTALL iceberg",
		"LOAD iceberg",
		"INSTALL httpfs",
		"LOAD httpfs",
		fmt.Sprintf("SET GLOBAL s3_region = '%s'", opts.S3Region),
		fmt.Sprintf("SET GLOBAL s3_endpoint = '%s'", endpoint),
		"SET GLOBAL s3_url_style = 'path'",
		fmt.Sprintf("SET GLOBAL s3_use_ssl = %s", useSSL),
		"SET GLOBAL s3_access_key_id = 'minioadmin'",
		"SET GLOBAL s3_secret_access_key = 'minioadmin'",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			db.Close()
			return nil, fmt.Errorf("duckdb %q: %w", s, err)
		}
	}
	return db, nil
}

func trimScheme(url string) string {
	const https = "https://"
	const http = "http://"
	switch {
	case len(url) > len(https) && url[:len(https)] == https:
		return url[len(https):]
	case len(url) > len(http) && url[:len(http)] == http:
		return url[len(http):]
	}
	return url
}

func isLocalEndpoint(url string) bool {
	// MinIO tests are always http://; any http:// endpoint is treated as local.
	return len(url) > 7 && url[:7] == "http://"
}
