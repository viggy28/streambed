package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/viggy28/streambed/config"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/storage"
	"github.com/viggy28/streambed/internal/wal"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "streambed",
		Short: "Postgres-to-Iceberg analytics engine",
	}

	syncCmd := &cobra.Command{
		Use:   "sync",
		Short: "Start syncing Postgres WAL to Iceberg on S3",
		RunE:  runSync,
	}

	cfg := config.Load()
	syncCmd.Flags().StringVar(&cfg.SourceURL, "source-url", cfg.SourceURL, "Postgres connection URL")
	syncCmd.Flags().StringVar(&cfg.S3Bucket, "s3-bucket", cfg.S3Bucket, "S3 bucket name")
	syncCmd.Flags().StringVar(&cfg.S3Prefix, "s3-prefix", cfg.S3Prefix, "S3 key prefix")
	syncCmd.Flags().StringVar(&cfg.S3Endpoint, "s3-endpoint", cfg.S3Endpoint, "Custom S3 endpoint (MinIO)")
	syncCmd.Flags().StringVar(&cfg.S3Region, "s3-region", cfg.S3Region, "AWS region")
	syncCmd.Flags().StringVar(&cfg.StatePath, "state-path", cfg.StatePath, "SQLite state file path")
	syncCmd.Flags().StringVar(&cfg.SlotName, "slot-name", cfg.SlotName, "Replication slot name")
	syncCmd.Flags().IntVar(&cfg.FlushRows, "flush-rows", cfg.FlushRows, "Row buffer flush threshold")
	syncCmd.Flags().DurationVar(&cfg.FlushInterval, "flush-interval", cfg.FlushInterval, "Time-based flush interval")
	syncCmd.Flags().StringSliceVar(&cfg.IncludeTables, "include-tables", cfg.IncludeTables, "Tables to include")
	syncCmd.Flags().StringSliceVar(&cfg.ExcludeTables, "exclude-tables", cfg.ExcludeTables, "Tables to exclude")
	syncCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level (DEBUG, INFO, WARN, ERROR)")

	rootCmd.AddCommand(syncCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runSync(cmd *cobra.Command, args []string) error {
	cfg := config.Load()

	// Override with flags that were explicitly set
	cmd.Flags().Visit(func(f *pflag.Flag) {
		switch f.Name {
		case "source-url":
			cfg.SourceURL = f.Value.String()
		case "s3-bucket":
			cfg.S3Bucket = f.Value.String()
		case "s3-prefix":
			cfg.S3Prefix = f.Value.String()
		case "s3-endpoint":
			cfg.S3Endpoint = f.Value.String()
		case "s3-region":
			cfg.S3Region = f.Value.String()
		case "state-path":
			cfg.StatePath = f.Value.String()
		case "slot-name":
			cfg.SlotName = f.Value.String()
		case "log-level":
			cfg.LogLevel = f.Value.String()
		}
	})

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config error: %w", err)
	}

	logger := setupLogger(cfg.LogLevel)

	logger.Info("streambed starting",
		"source", maskURL(cfg.SourceURL),
		"bucket", cfg.S3Bucket,
		"prefix", cfg.S3Prefix,
		"slot", cfg.SlotName,
		"flush_rows", cfg.FlushRows,
		"flush_interval", cfg.FlushInterval,
	)

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("shutdown initiated", "signal", sig)
		cancel()
	}()

	// Initialize state store
	stateStore, err := state.Open(cfg.StatePath)
	if err != nil {
		return fmt.Errorf("open state store: %w", err)
	}
	defer stateStore.Close()

	// Initialize S3 client
	s3Client, err := storage.NewS3Client(ctx, cfg.S3Bucket, cfg.S3Region, cfg.S3Endpoint)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	// Connect to Postgres for replication
	connStr := cfg.SourceURL
	if !strings.Contains(connStr, "replication=") {
		if strings.Contains(connStr, "?") {
			connStr += "&replication=database"
		} else {
			connStr += "?replication=database"
		}
	}

	pgConn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pgConn.Close(context.Background())

	// Create publication
	pubName := cfg.SlotName // use same name for publication
	if err := wal.CreatePublication(ctx, pgConn, pubName, cfg.IncludeTables, logger); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}

	// Create or reuse replication slot
	slotLSN, err := wal.CreateOrReuseSlot(ctx, pgConn, cfg.SlotName, logger)
	if err != nil {
		return fmt.Errorf("setup replication slot: %w", err)
	}

	// Check state store for last flushed LSN (may be more recent than slot)
	stateLSN, err := stateStore.GetFlushedLSN()
	if err != nil {
		return fmt.Errorf("get flushed LSN from state: %w", err)
	}
	var minStoreLsn pglogrepl.LSN
	for _, lsnStr := range stateLSN {
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			return fmt.Errorf("error parsing LSN: %s %v", lsnStr, err)
		}
		if minStoreLsn > lsn || minStoreLsn == 0 {
			minStoreLsn = lsn
		}
	}

	startLSN := slotLSN
	if minStoreLsn > startLSN {
		startLSN = minStoreLsn
		logger.Info("resuming from state store LSN", "lsn", startLSN)
	} else if minStoreLsn < startLSN && minStoreLsn != 0 {
		logger.Warn("state store is behind slot position, some data may need re-sync",
			"min_store_lsn", minStoreLsn,
			"slot_lsn", startLSN,
		)
	}

	// Initialize Iceberg catalog
	catalog := iceberg.NewCatalog(s3Client, cfg.S3Bucket, cfg.S3Prefix)

	// Initialize writer
	writer := iceberg.NewWriter(catalog, s3Client, stateStore, cfg.SlotName,
		cfg.FlushRows, cfg.FlushInterval, logger)

	// Create consumer
	consumer := wal.NewConsumer(pgConn, cfg.SlotName, pubName, startLSN, cfg.ExcludeTables, logger, stateStore)

	// Channels
	events := make(chan wal.RowEvent, 1000)
	ackCh := make(chan pglogrepl.LSN, 10)

	// Start writer in background
	writerErrCh := make(chan error, 1)
	go func() {
		err := writer.Start(ctx, events, ackCh)
		if err != nil {
			writerErrCh <- err
			cancel()
		} else {
			writerErrCh <- nil
		}
	}()

	// Start consumer (blocks until ctx cancelled)
	consumerErr := consumer.Start(ctx, events, ackCh)

	// Close events channel so writer can finish
	close(events)

	// Wait for writer to complete final flush
	select {
	case writerErr := <-writerErrCh:
		if writerErr != nil {
			logger.Error("writer error", "error", writerErr)
		}
	case <-time.After(30 * time.Second):
		logger.Warn("writer shutdown timed out")
	}

	if consumerErr != nil && ctx.Err() == nil {
		return fmt.Errorf("consumer error: %w", consumerErr)
	}

	logger.Info("streambed stopped")
	return nil
}

func setupLogger(level string) *slog.Logger {
	var logLevel slog.Level
	switch strings.ToUpper(level) {
	case "DEBUG":
		logLevel = slog.LevelDebug
	case "WARN":
		logLevel = slog.LevelWarn
	case "ERROR":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))
}

func maskURL(url string) string {
	// Mask password in connection URL for logging
	if idx := strings.Index(url, "://"); idx >= 0 {
		rest := url[idx+3:]
		if atIdx := strings.Index(rest, "@"); atIdx >= 0 {
			if colonIdx := strings.Index(rest[:atIdx], ":"); colonIdx >= 0 {
				return url[:idx+3] + rest[:colonIdx] + ":****@" + rest[atIdx+1:]
			}
		}
	}
	return url
}
