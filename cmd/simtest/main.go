// Package main is the simtest CLI — a long-running harness that drives
// Postgres traffic against a supervised streambed subprocess and continuously
// verifies Iceberg matches Postgres.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/viggy28/streambed/internal/simtest"
	"github.com/viggy28/streambed/internal/simtest/oracle"
	"github.com/viggy28/streambed/internal/simtest/workload"
)

type flags struct {
	streambedBin     string
	streambedState   string
	sourceURL        string
	s3Bucket         string
	s3Prefix         string
	s3Endpoint       string
	s3Region         string
	slotName         string
	flushRows        int
	flushInterval    time.Duration
	workloadsCSV     string
	eventsRate       int
	eventsBatch      int
	pgbenchScale     int
	pgbenchClients   int
	pgbenchJobs      int
	schemaAlterEvery time.Duration
	schemaWriteRate  int
	validateInterval time.Duration
	sentinelTimeout  time.Duration
	heartbeat        time.Duration
	duration         time.Duration
	logDir           string
	logLevel         string
	chaos            bool
	chaosKillEvery   time.Duration
	chaosPauseEvery  time.Duration
	chaosMinioCtnr   string
	chaosPauseDur    time.Duration
}

func main() {
	f := flags{}

	root := &cobra.Command{
		Use:   "simtest",
		Short: "Long-running simulation harness for streambed",
	}

	run := &cobra.Command{
		Use:   "run",
		Short: "Start the simulation (supervises streambed, runs workloads, runs oracle)",
		RunE:  func(cmd *cobra.Command, args []string) error { return runSim(f) },
	}

	run.Flags().StringVar(&f.streambedBin, "streambed-bin", "./streambed", "path to the streambed binary")
	run.Flags().StringVar(&f.streambedState, "streambed-state", "/tmp/simtest-streambed-state.db", "SQLite state path for streambed")
	run.Flags().StringVar(&f.sourceURL, "source-url", "postgres://postgres:test@localhost:5435/postgres", "Postgres connection URL")
	run.Flags().StringVar(&f.s3Bucket, "s3-bucket", "streambed", "S3 bucket")
	run.Flags().StringVar(&f.s3Prefix, "s3-prefix", "simtest", "S3 prefix")
	run.Flags().StringVar(&f.s3Endpoint, "s3-endpoint", "http://localhost:9004", "S3 endpoint")
	run.Flags().StringVar(&f.s3Region, "s3-region", "us-east-1", "S3 region")
	run.Flags().StringVar(&f.slotName, "slot-name", "streambed_simtest", "Replication slot name")
	run.Flags().IntVar(&f.flushRows, "flush-rows", 5000, "Streambed flush-rows")
	run.Flags().DurationVar(&f.flushInterval, "flush-interval", 1*time.Second, "Streambed flush-interval")

	run.Flags().StringVar(&f.workloadsCSV, "workloads", "events", "Comma-separated workloads (events, pgbench, schema-evolution)")
	run.Flags().IntVar(&f.eventsRate, "events-rate", 200, "events workload target rows/sec")
	run.Flags().IntVar(&f.eventsBatch, "events-batch", 10, "events workload rows per INSERT batch")
	run.Flags().IntVar(&f.pgbenchScale, "pgbench-scale", 10, "pgbench -s scale factor (10 = 1M accounts)")
	run.Flags().IntVar(&f.pgbenchClients, "pgbench-clients", 10, "pgbench -c concurrent clients")
	run.Flags().IntVar(&f.pgbenchJobs, "pgbench-jobs", 0, "pgbench -j threads (0 = use clients count)")
	run.Flags().DurationVar(&f.schemaAlterEvery, "schema-alter-every", 30*time.Second, "schema-evolution workload: ALTER TABLE interval")
	run.Flags().IntVar(&f.schemaWriteRate, "schema-write-rate", 20, "schema-evolution workload: INSERTs per second")

	run.Flags().DurationVar(&f.validateInterval, "validate-interval", 30*time.Second, "How often the oracle runs")
	run.Flags().DurationVar(&f.sentinelTimeout, "sentinel-timeout", 60*time.Second, "Max time to wait for a sentinel to appear in Iceberg")
	run.Flags().DurationVar(&f.heartbeat, "heartbeat", 30*time.Second, "Metrics heartbeat interval")

	run.Flags().DurationVar(&f.duration, "duration", 5*time.Minute, "Total run time (0 = forever)")
	run.Flags().StringVar(&f.logDir, "log-dir", "./simtest-logs", "Directory for per-run log files")
	run.Flags().StringVar(&f.logLevel, "log-level", "INFO", "Log level (DEBUG, INFO, WARN, ERROR)")

	run.Flags().BoolVar(&f.chaos, "chaos", true, "Enable chaos (SIGKILL streambed periodically)")
	run.Flags().DurationVar(&f.chaosKillEvery, "chaos-kill-every", 90*time.Second, "How often to SIGKILL streambed")
	run.Flags().DurationVar(&f.chaosPauseEvery, "chaos-pause-minio-every", 3*time.Minute, "How often to pause MinIO (0 to disable)")
	run.Flags().StringVar(&f.chaosMinioCtnr, "chaos-minio-container", "", "Docker container name to pause (required for MinIO chaos)")
	run.Flags().DurationVar(&f.chaosPauseDur, "chaos-pause-duration", 15*time.Second, "How long to pause MinIO per event")

	root.AddCommand(run)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func runSim(f flags) error {
	logger := buildLogger(f.logLevel)

	ctx, cancel := signalContext()
	defer cancel()

	workloads, err := buildWorkloads(f)
	if err != nil {
		return err
	}
	invariants := buildInvariants(f, workloads)

	opts := simtest.Options{
		SourceURL:        f.sourceURL,
		S3Bucket:         f.s3Bucket,
		S3Prefix:         f.s3Prefix,
		S3Endpoint:       f.s3Endpoint,
		S3Region:         f.s3Region,
		StreambedBin:     f.streambedBin,
		StreambedArgs:    buildStreambedArgs(f),
		SlotName:         f.slotName,
		ValidateInterval: f.validateInterval,
		SentinelTimeout:  f.sentinelTimeout,
		Heartbeat:        f.heartbeat,
		Duration:         f.duration,
		LogDir:           f.logDir,
		Workloads:        workloads,
		Invariants:       invariants,

		ChaosEnabled:    f.chaos,
		ChaosKillEvery:  f.chaosKillEvery,
		ChaosPauseEvery: f.chaosPauseEvery,
	}
	if f.chaosMinioCtnr != "" && f.chaosPauseEvery > 0 {
		opts.ChaosPauseMinio = makeMinioPauseFn(f.chaosMinioCtnr, f.chaosPauseDur, logger)
	}

	logger.Info("simtest starting",
		"duration", f.duration,
		"workloads", f.workloadsCSV,
		"chaos", f.chaos,
		"log_dir", f.logDir)

	summary, err := simtest.Run(ctx, opts, logger)
	if err != nil {
		return err
	}

	logger.Info("simtest run complete",
		"uptime", summary.Uptime.Truncate(time.Second),
		"oracle_checks", summary.OracleChecks,
		"oracle_failures", summary.OracleFailures,
		"streambed_restarts", summary.StreambedRestarts,
		"final_oracle_ok", summary.FinalResult.OK(),
		"final_tick_id", summary.FinalResult.TickID)
	return nil
}

func buildWorkloads(f flags) ([]workload.Workload, error) {
	var out []workload.Workload
	for name := range strings.SplitSeq(f.workloadsCSV, ",") {
		name = strings.TrimSpace(name)
		switch name {
		case "events":
			out = append(out, &workload.Events{
				RatePerS:  f.eventsRate,
				BatchSize: f.eventsBatch,
			})
		case "pgbench":
			out = append(out, &workload.PGBench{
				SourceURL: f.sourceURL,
				Scale:     f.pgbenchScale,
				Clients:   f.pgbenchClients,
				Jobs:      f.pgbenchJobs,
			})
		case "schema-evolution", "schema_evolution":
			out = append(out, &workload.SchemaEvolution{
				AlterEvery:    f.schemaAlterEvery,
				WriteRatePerS: f.schemaWriteRate,
			})
		case "":
			continue
		default:
			return nil, fmt.Errorf("unknown workload %q", name)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one workload required")
	}
	return out, nil
}

// buildInvariants returns the invariants that apply to the selected workloads.
// Currently only pgbench has one: the teller/branch balance check.
func buildInvariants(f flags, ws []workload.Workload) []oracle.Invariant {
	var out []oracle.Invariant
	for _, w := range ws {
		if _, ok := w.(*workload.PGBench); ok {
			out = append(out, oracle.PgbenchBalanceInvariant{
				S3Bucket: f.s3Bucket,
				S3Prefix: f.s3Prefix,
			})
		}
	}
	return out
}

// buildStreambedArgs translates simtest CLI flags into the `streambed sync`
// invocation so the supervisor can start streambed with the matching config.
func buildStreambedArgs(f flags) []string {
	return []string{
		"sync",
		"--source-url=" + f.sourceURL,
		"--s3-bucket=" + f.s3Bucket,
		"--s3-prefix=" + f.s3Prefix,
		"--s3-endpoint=" + f.s3Endpoint,
		"--s3-region=" + f.s3Region,
		"--slot-name=" + f.slotName,
		"--state-path=" + f.streambedState,
		fmt.Sprintf("--flush-rows=%d", f.flushRows),
		"--flush-interval=" + f.flushInterval.String(),
	}
}

func makeMinioPauseFn(container string, duration time.Duration, logger *slog.Logger) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if err := exec.CommandContext(ctx, "docker", "pause", container).Run(); err != nil {
			return fmt.Errorf("docker pause %s: %w", container, err)
		}
		logger.Info("MinIO paused", "container", container, "duration", duration)
		select {
		case <-ctx.Done():
		case <-time.After(duration):
		}
		if err := exec.CommandContext(context.Background(), "docker", "unpause", container).Run(); err != nil {
			return fmt.Errorf("docker unpause %s: %w", container, err)
		}
		logger.Info("MinIO unpaused", "container", container)
		return nil
	}
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()
	return ctx, cancel
}

func buildLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToUpper(level) {
	case "DEBUG":
		lvl = slog.LevelDebug
	case "WARN":
		lvl = slog.LevelWarn
	case "ERROR":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}
