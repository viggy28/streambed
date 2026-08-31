package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/viggy28/streambed/config"
	"github.com/viggy28/streambed/internal/ducklake"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/pipeline"
	"github.com/viggy28/streambed/internal/resync"
	"github.com/viggy28/streambed/internal/server"
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
	syncCmd.Flags().StringVar(&cfg.TargetFormat, "target-format", cfg.TargetFormat, "Lakehouse target format: iceberg or ducklake")
	syncCmd.Flags().StringVar(&cfg.DuckLakeCatalog, "ducklake-catalog", cfg.DuckLakeCatalog, "DuckLake catalog path")
	syncCmd.Flags().StringVar(&cfg.DuckLakeCatalogStore, "ducklake-catalog-store", cfg.DuckLakeCatalogStore, "DuckLake catalog store: sqlite or duckdb")
	syncCmd.Flags().StringVar(&cfg.DuckLakeDataPath, "ducklake-data-path", cfg.DuckLakeDataPath, "DuckLake data path (defaults to s3://bucket/prefix/ducklake/)")
	syncCmd.Flags().StringVar(&cfg.SlotName, "slot-name", cfg.SlotName, "Replication slot name")
	syncCmd.Flags().IntVar(&cfg.FlushRows, "flush-rows", cfg.FlushRows, "Row buffer flush threshold")
	syncCmd.Flags().DurationVar(&cfg.FlushInterval, "flush-interval", cfg.FlushInterval, "Time-based flush interval")
	syncCmd.Flags().IntVar(&cfg.TargetFileSizeMB, "target-file-size-mb", cfg.TargetFileSizeMB, "Target compressed Parquet data file size in MiB")
	syncCmd.Flags().StringSliceVar(&cfg.IncludeTables, "include-tables", cfg.IncludeTables, "Tables to include")
	syncCmd.Flags().StringSliceVar(&cfg.ExcludeTables, "exclude-tables", cfg.ExcludeTables, "Tables to exclude")
	syncCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level (DEBUG, INFO, WARN, ERROR)")
	syncCmd.Flags().StringVar(&cfg.MutationMode, "mutation-mode", cfg.MutationMode, "Iceberg mutation strategy: cow or mor")
	syncCmd.Flags().StringVar(&cfg.QueryAddr, "query-addr", cfg.QueryAddr, "Listen address for query server (e.g., :5433)")

	queryCmd := &cobra.Command{
		Use:   "query",
		Short: "Start query server only (no sync)",
		RunE:  runQuery,
	}

	queryCmd.Flags().StringVar(&cfg.S3Bucket, "s3-bucket", cfg.S3Bucket, "S3 bucket name")
	queryCmd.Flags().StringVar(&cfg.S3Prefix, "s3-prefix", cfg.S3Prefix, "S3 key prefix")
	queryCmd.Flags().StringVar(&cfg.S3Endpoint, "s3-endpoint", cfg.S3Endpoint, "Custom S3 endpoint (MinIO)")
	queryCmd.Flags().StringVar(&cfg.S3Region, "s3-region", cfg.S3Region, "AWS region")
	queryCmd.Flags().StringVar(&cfg.TargetFormat, "target-format", cfg.TargetFormat, "Lakehouse target format: iceberg or ducklake")
	queryCmd.Flags().StringVar(&cfg.DuckLakeCatalog, "ducklake-catalog", cfg.DuckLakeCatalog, "DuckLake catalog path")
	queryCmd.Flags().StringVar(&cfg.DuckLakeCatalogStore, "ducklake-catalog-store", cfg.DuckLakeCatalogStore, "DuckLake catalog store: sqlite or duckdb")
	queryCmd.Flags().StringVar(&cfg.DuckLakeDataPath, "ducklake-data-path", cfg.DuckLakeDataPath, "DuckLake data path (defaults to s3://bucket/prefix/ducklake/)")
	queryCmd.Flags().StringVar(&cfg.QueryAddr, "listen-addr", cfg.QueryAddr, "Listen address for query server")
	queryCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level (DEBUG, INFO, WARN, ERROR)")

	var cleanupTables []string
	var cleanupDryRun bool
	var cleanupForce bool
	cleanupCmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Delete S3 objects and state for specified tables (destructive)",
		Long: `Delete all S3 objects (Parquet data + Iceberg metadata) and the state store
entry for the given tables. Tables must be specified as schema.table.

Use this to resync a table from scratch (e.g., after fixing a schema bug).
This command does NOT touch the Postgres replication slot.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCleanup(cmd, cleanupTables, cleanupDryRun, cleanupForce)
		},
	}
	cleanupCmd.Flags().StringSliceVar(&cleanupTables, "table", nil, "Table to clean up as schema.table (repeatable, required)")
	cleanupCmd.Flags().BoolVar(&cleanupDryRun, "dry-run", false, "List what would be deleted without touching anything")
	cleanupCmd.Flags().BoolVar(&cleanupForce, "force", false, "Skip confirmation prompt")
	cleanupCmd.Flags().StringVar(&cfg.S3Bucket, "s3-bucket", cfg.S3Bucket, "S3 bucket name")
	cleanupCmd.Flags().StringVar(&cfg.S3Prefix, "s3-prefix", cfg.S3Prefix, "S3 key prefix")
	cleanupCmd.Flags().StringVar(&cfg.S3Endpoint, "s3-endpoint", cfg.S3Endpoint, "Custom S3 endpoint (MinIO)")
	cleanupCmd.Flags().StringVar(&cfg.S3Region, "s3-region", cfg.S3Region, "AWS region")
	cleanupCmd.Flags().StringVar(&cfg.StatePath, "state-path", cfg.StatePath, "SQLite state file path")
	cleanupCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level (DEBUG, INFO, WARN, ERROR)")

	var maintenanceTables []string
	var maintenanceRetainLast int
	var maintenanceOrphans bool
	var maintenanceDryRun bool
	var maintenanceForce bool
	maintenanceCmd := &cobra.Command{
		Use:   "maintenance",
		Short: "Expire Iceberg snapshots and clean orphan files",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMaintenance(cmd, maintenanceTables, maintenanceRetainLast, maintenanceOrphans, maintenanceDryRun, maintenanceForce)
		},
	}
	maintenanceCmd.Flags().StringSliceVar(&maintenanceTables, "table", nil, "Table to maintain as schema.table (repeatable, required)")
	maintenanceCmd.Flags().IntVar(&maintenanceRetainLast, "retain-last", 1, "Number of non-current historical snapshots to retain")
	maintenanceCmd.Flags().BoolVar(&maintenanceOrphans, "orphans", false, "Also delete orphan objects unreachable from retained metadata")
	maintenanceCmd.Flags().BoolVar(&maintenanceDryRun, "dry-run", true, "Only print planned deletions")
	maintenanceCmd.Flags().BoolVar(&maintenanceForce, "force", false, "Apply deletions; required with --dry-run=false")
	maintenanceCmd.Flags().StringVar(&cfg.S3Bucket, "s3-bucket", cfg.S3Bucket, "S3 bucket name")
	maintenanceCmd.Flags().StringVar(&cfg.S3Prefix, "s3-prefix", cfg.S3Prefix, "S3 key prefix")
	maintenanceCmd.Flags().StringVar(&cfg.S3Endpoint, "s3-endpoint", cfg.S3Endpoint, "Custom S3 endpoint (MinIO)")
	maintenanceCmd.Flags().StringVar(&cfg.S3Region, "s3-region", cfg.S3Region, "AWS region")
	maintenanceCmd.Flags().StringVar(&cfg.StatePath, "state-path", cfg.StatePath, "SQLite state file path")
	maintenanceCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level (DEBUG, INFO, WARN, ERROR)")

	var compactTable string
	var compactTargetMB int
	var compactThresholdMB int
	var compactMaxInputFiles int
	var compactDryRun bool
	var compactForce bool
	compactCmd := &cobra.Command{
		Use:   "compact",
		Short: "Compact small Iceberg data files",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMaintenanceCompact(cmd, compactTable, compactTargetMB, compactThresholdMB, compactMaxInputFiles, compactDryRun, compactForce)
		},
	}
	compactCmd.Flags().StringVar(&compactTable, "table", "", "Table to compact as schema.table (required)")
	compactCmd.Flags().IntVar(&compactTargetMB, "target-file-size-mb", cfg.TargetFileSizeMB, "Target compacted Parquet file size in MiB")
	compactCmd.Flags().IntVar(&compactThresholdMB, "small-file-threshold-mb", 32, "Active data files smaller than this are candidates")
	compactCmd.Flags().IntVar(&compactMaxInputFiles, "max-input-files", 1000, "Maximum number of input files for one compaction run")
	compactCmd.Flags().BoolVar(&compactDryRun, "dry-run", true, "Only print compaction plan")
	compactCmd.Flags().BoolVar(&compactForce, "force", false, "Apply compaction; required with --dry-run=false")
	compactCmd.Flags().StringVar(&cfg.S3Bucket, "s3-bucket", cfg.S3Bucket, "S3 bucket name")
	compactCmd.Flags().StringVar(&cfg.S3Prefix, "s3-prefix", cfg.S3Prefix, "S3 key prefix")
	compactCmd.Flags().StringVar(&cfg.S3Endpoint, "s3-endpoint", cfg.S3Endpoint, "Custom S3 endpoint (MinIO)")
	compactCmd.Flags().StringVar(&cfg.S3Region, "s3-region", cfg.S3Region, "AWS region")
	compactCmd.Flags().StringVar(&cfg.StatePath, "state-path", cfg.StatePath, "SQLite state file path")
	compactCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level (DEBUG, INFO, WARN, ERROR)")
	maintenanceCmd.AddCommand(compactCmd)

	var resyncTable string
	var resyncForce bool
	resyncCmd := &cobra.Command{
		Use:   "resync",
		Short: "Re-pull a single table from Postgres via COPY under a consistent snapshot",
		Long: `Delete existing Iceberg data + state for one table, then backfill it from
Postgres using COPY TO STDOUT under an exported logical-replication snapshot.

The main sync daemon MUST be stopped before running this command. On the next
sync startup, events from the main slot whose WAL position is at or below the
snapshot LSN are silently discarded for this table to avoid duplicates.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runResync(cmd, resyncTable, resyncForce)
		},
	}
	resyncCmd.Flags().StringVar(&resyncTable, "table", "", "Table to resync as schema.table (required)")
	resyncCmd.Flags().BoolVar(&resyncForce, "force", false, "Skip confirmation prompt")
	resyncCmd.Flags().StringVar(&cfg.SourceURL, "source-url", cfg.SourceURL, "Postgres connection URL")
	resyncCmd.Flags().StringVar(&cfg.S3Bucket, "s3-bucket", cfg.S3Bucket, "S3 bucket name")
	resyncCmd.Flags().StringVar(&cfg.S3Prefix, "s3-prefix", cfg.S3Prefix, "S3 key prefix")
	resyncCmd.Flags().StringVar(&cfg.S3Endpoint, "s3-endpoint", cfg.S3Endpoint, "Custom S3 endpoint (MinIO)")
	resyncCmd.Flags().StringVar(&cfg.S3Region, "s3-region", cfg.S3Region, "AWS region")
	resyncCmd.Flags().StringVar(&cfg.StatePath, "state-path", cfg.StatePath, "SQLite state file path")
	resyncCmd.Flags().StringVar(&cfg.TargetFormat, "target-format", cfg.TargetFormat, "Lakehouse target format: iceberg or ducklake")
	resyncCmd.Flags().StringVar(&cfg.DuckLakeCatalog, "ducklake-catalog", cfg.DuckLakeCatalog, "DuckLake catalog path")
	resyncCmd.Flags().StringVar(&cfg.DuckLakeCatalogStore, "ducklake-catalog-store", cfg.DuckLakeCatalogStore, "DuckLake catalog store: sqlite or duckdb")
	resyncCmd.Flags().StringVar(&cfg.DuckLakeDataPath, "ducklake-data-path", cfg.DuckLakeDataPath, "DuckLake data path (defaults to s3://bucket/prefix/ducklake/)")
	resyncCmd.Flags().IntVar(&cfg.FlushRows, "flush-rows", cfg.FlushRows, "Rows per parquet file / Iceberg snapshot")
	resyncCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level (DEBUG, INFO, WARN, ERROR)")

	rootCmd.AddCommand(syncCmd, queryCmd, cleanupCmd, maintenanceCmd, resyncCmd)

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
		case "target-format":
			cfg.TargetFormat = strings.ToLower(f.Value.String())
		case "ducklake-catalog":
			cfg.DuckLakeCatalog = f.Value.String()
		case "ducklake-catalog-store":
			cfg.DuckLakeCatalogStore = strings.ToLower(f.Value.String())
		case "ducklake-data-path":
			cfg.DuckLakeDataPath = f.Value.String()
		case "slot-name":
			cfg.SlotName = f.Value.String()
		case "flush-rows":
			cfg.FlushRows, _ = cmd.Flags().GetInt("flush-rows")
		case "flush-interval":
			cfg.FlushInterval, _ = cmd.Flags().GetDuration("flush-interval")
		case "target-file-size-mb":
			cfg.TargetFileSizeMB, _ = cmd.Flags().GetInt("target-file-size-mb")
		case "include-tables":
			cfg.IncludeTables, _ = cmd.Flags().GetStringSlice("include-tables")
		case "exclude-tables":
			cfg.ExcludeTables, _ = cmd.Flags().GetStringSlice("exclude-tables")
		case "log-level":
			cfg.LogLevel = f.Value.String()
		case "mutation-mode":
			cfg.MutationMode = strings.ToLower(f.Value.String())
		case "query-addr":
			cfg.QueryAddr = f.Value.String()
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
		"target_format", cfg.TargetFormat,
		"flush_rows", cfg.FlushRows,
		"flush_interval", cfg.FlushInterval,
		"target_file_size_mb", cfg.TargetFileSizeMB,
		"mutation_mode", cfg.MutationMode,
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

	// Initialize S3 client and selected lakehouse target.
	s3Client, err := storage.NewS3Client(ctx, cfg.S3Bucket, cfg.S3Region, cfg.S3Endpoint)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}
	var catalog *iceberg.Catalog
	var writer pipeline.Writer
	var duckWriter *ducklake.Writer
	makeWriter := func() (pipeline.Writer, error) {
		if cfg.TargetFormat == "ducklake" {
			w, err := ducklake.NewWriter(ctx, ducklake.Config{
				CatalogPath:  cfg.DuckLakeCatalog,
				CatalogStore: cfg.DuckLakeCatalogStore,
				DataPath:     cfg.EffectiveDuckLakeDataPath(),
				S3Endpoint:   cfg.S3Endpoint,
				S3Region:     cfg.S3Region,
			}, stateStore, cfg.FlushRows, cfg.FlushInterval, logger)
			if err != nil {
				return nil, err
			}
			duckWriter = w
			return w, nil
		}
		return iceberg.NewWriter(catalog, s3Client, stateStore, cfg.SlotName,
			cfg.FlushRows, cfg.FlushInterval, logger,
			iceberg.WithMutationMode(iceberg.MutationMode(cfg.MutationMode)),
			iceberg.WithTargetFileSizeBytes(int64(cfg.TargetFileSizeMB)*1024*1024)), nil
	}
	if cfg.TargetFormat == "iceberg" {
		catalog = iceberg.NewCatalog(s3Client, cfg.S3Bucket, cfg.S3Prefix)
		if err := catalog.ValidateMutationMode(ctx, iceberg.MutationMode(cfg.MutationMode)); err != nil {
			return err
		}
	}
	writer, err = makeWriter()
	if err != nil {
		return fmt.Errorf("create %s writer: %w", cfg.TargetFormat, err)
	}
	if duckWriter != nil {
		defer duckWriter.Close()
	}

	// Start query server if --query-addr is set
	if cfg.QueryAddr != "" {
		querySrv, err := server.NewServer(server.ServerConfig{
			ListenAddr:           cfg.QueryAddr,
			S3Bucket:             cfg.S3Bucket,
			S3Prefix:             cfg.S3Prefix,
			S3Endpoint:           cfg.S3Endpoint,
			S3Region:             cfg.S3Region,
			TargetFormat:         cfg.TargetFormat,
			DuckLakeCatalog:      cfg.DuckLakeCatalog,
			DuckLakeCatalogStore: cfg.DuckLakeCatalogStore,
			DuckLakeDataPath:     cfg.EffectiveDuckLakeDataPath(),
		}, s3Client, logger)
		if err != nil {
			return fmt.Errorf("create query server: %w", err)
		}
		defer querySrv.Close()

		go func() {
			if err := querySrv.Start(ctx); err != nil {
				logger.Error("query server error", "error", err)
			}
		}()
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

	// Open a regular (non-replication) connection for metadata queries
	// (column defaults on schema changes, etc.).
	metaConn, err := pgx.Connect(ctx, cfg.SourceURL)
	if err != nil {
		return fmt.Errorf("connect to postgres (metadata): %w", err)
	}
	defer metaConn.Close(context.Background())
	metaQuerier := wal.NewMetadataQuerier(metaConn)

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

	// Read per-table flush LSNs from the lakehouse target (the sole source of truth).
	// These are used for dedup on restart and to determine startLSN.
	tableFlushLSN := make(map[string]pglogrepl.LSN)
	registeredTables, err := stateStore.GetRegisteredTables()
	if err != nil {
		return fmt.Errorf("get registered tables: %w", err)
	}
	for _, t := range registeredTables {
		lsnStr, found, err := getTargetFlushLSN(ctx, cfg.TargetFormat, catalog, duckWriter, t.Schema, t.Table)
		if err != nil {
			logger.Warn("cannot read target LSN, skipping",
				"table", fmt.Sprintf("%s.%s", t.Schema, t.Table),
				"error", err,
			)
			continue
		}
		if !found {
			continue
		}
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			return fmt.Errorf("parse target LSN %q for %s.%s: %w", lsnStr, t.Schema, t.Table, err)
		}
		tableFlushLSN[fmt.Sprintf("%s.%s", t.Schema, t.Table)] = lsn
	}

	var minTargetLSN pglogrepl.LSN
	for _, lsn := range tableFlushLSN {
		if minTargetLSN == 0 || lsn < minTargetLSN {
			minTargetLSN = lsn
		}
	}

	startLSN := slotLSN
	if minTargetLSN > startLSN {
		startLSN = minTargetLSN
		logger.Info("resuming from target LSN", "target_format", cfg.TargetFormat, "lsn", startLSN)
	} else if minTargetLSN < startLSN && minTargetLSN != 0 {
		// This is normal: cold tables retain an older flush LSN while
		// the slot advances past them (they had no events in the gap).
		// Log for visibility but don't block startup.
		logger.Info("slot ahead of coldest target LSN (cold tables expected)",
			"slot_lsn", startLSN,
			"min_target_lsn", minTargetLSN,
		)
	}

	// Create unified pipeline (single goroutine: reads WAL + writes target)
	p := pipeline.New(pgConn, cfg.SlotName, pubName, startLSN, cfg.ExcludeTables,
		logger, stateStore, tableFlushLSN, writer, cfg.FlushInterval, metaQuerier)

	// Run blocks until ctx is cancelled; does final flush internally.
	// On non-context errors (e.g. Postgres disconnect, transient S3
	// failure), reconnect and resume from the last durable position.
	const maxReconnects = 10
	reconnectBackoff := 1 * time.Second
	maxReconnectBackoff := 60 * time.Second

	for attempt := 0; ; attempt++ {
		pipelineErr := p.Run(ctx)

		// Clean shutdown.
		if ctx.Err() != nil {
			logger.Info("streambed stopped")
			return nil
		}

		// Permanent failure after exhausting retries.
		if attempt >= maxReconnects {
			return fmt.Errorf("pipeline error after %d reconnects: %w", attempt, pipelineErr)
		}

		logger.Warn("pipeline error, will reconnect",
			"error", pipelineErr,
			"attempt", attempt+1,
			"backoff", reconnectBackoff,
		)

		// Close old connection (best-effort).
		pgConn.Close(context.Background())

		// Wait before reconnecting.
		select {
		case <-time.After(reconnectBackoff):
		case <-ctx.Done():
			logger.Info("streambed stopped during reconnect backoff")
			return nil
		}
		reconnectBackoff = min(reconnectBackoff*2, maxReconnectBackoff)

		// Reconnect to Postgres.
		pgConn, err = pgconn.Connect(ctx, connStr)
		if err != nil {
			logger.Error("reconnect failed", "error", err)
			continue
		}

		// Re-read per-table flush LSNs from the target (may have advanced
		// from the last successful flush before the crash).
		registeredTables, err = stateStore.GetRegisteredTables()
		if err != nil {
			return fmt.Errorf("get registered tables on reconnect: %w", err)
		}
		tableFlushLSN = make(map[string]pglogrepl.LSN)
		for _, t := range registeredTables {
			lsnStr, found, err := getTargetFlushLSN(ctx, cfg.TargetFormat, catalog, duckWriter, t.Schema, t.Table)
			if err != nil || !found {
				continue
			}
			lsn, err := pglogrepl.ParseLSN(lsnStr)
			if err != nil {
				continue
			}
			tableFlushLSN[fmt.Sprintf("%s.%s", t.Schema, t.Table)] = lsn
		}

		// Recompute startLSN.
		slotLSN, err = wal.CreateOrReuseSlot(ctx, pgConn, cfg.SlotName, logger)
		if err != nil {
			logger.Error("reconnect: slot setup failed", "error", err)
			continue
		}
		startLSN = slotLSN
		for _, lsn := range tableFlushLSN {
			if lsn > startLSN {
				startLSN = lsn
			}
		}

		// Recreate writer and pipeline with fresh state.
		if duckWriter != nil {
			_ = duckWriter.Close()
			duckWriter = nil
		}
		writer, err = makeWriter()
		if err != nil {
			logger.Error("reconnect: writer setup failed", "error", err)
			continue
		}
		p = pipeline.New(pgConn, cfg.SlotName, pubName, startLSN, cfg.ExcludeTables,
			logger, stateStore, tableFlushLSN, writer, cfg.FlushInterval, metaQuerier)

		logger.Info("reconnected, resuming pipeline",
			"start_lsn", startLSN,
			"attempt", attempt+1,
		)

		// Reset backoff on successful reconnect.
		reconnectBackoff = 1 * time.Second
	}
}

// runQuery starts the query server in standalone mode (no Postgres sync).
func runQuery(cmd *cobra.Command, args []string) error {
	cfg := config.Load()

	cmd.Flags().Visit(func(f *pflag.Flag) {
		switch f.Name {
		case "s3-bucket":
			cfg.S3Bucket = f.Value.String()
		case "s3-prefix":
			cfg.S3Prefix = f.Value.String()
		case "s3-endpoint":
			cfg.S3Endpoint = f.Value.String()
		case "s3-region":
			cfg.S3Region = f.Value.String()
		case "target-format":
			cfg.TargetFormat = strings.ToLower(f.Value.String())
		case "ducklake-catalog":
			cfg.DuckLakeCatalog = f.Value.String()
		case "ducklake-catalog-store":
			cfg.DuckLakeCatalogStore = strings.ToLower(f.Value.String())
		case "ducklake-data-path":
			cfg.DuckLakeDataPath = f.Value.String()
		case "listen-addr":
			cfg.QueryAddr = f.Value.String()
		case "log-level":
			cfg.LogLevel = f.Value.String()
		}
	})

	// Default listen address for query command
	if cfg.QueryAddr == "" {
		cfg.QueryAddr = ":5433"
	}

	if err := cfg.ValidateQuery(); err != nil {
		return fmt.Errorf("config error: %w", err)
	}

	logger := setupLogger(cfg.LogLevel)
	logger.Info("streambed query server starting",
		"bucket", cfg.S3Bucket,
		"prefix", cfg.S3Prefix,
		"target_format", cfg.TargetFormat,
		"listen_addr", cfg.QueryAddr,
	)

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

	// Initialize S3 client
	s3Client, err := storage.NewS3Client(ctx, cfg.S3Bucket, cfg.S3Region, cfg.S3Endpoint)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	querySrv, err := server.NewServer(server.ServerConfig{
		ListenAddr:           cfg.QueryAddr,
		S3Bucket:             cfg.S3Bucket,
		S3Prefix:             cfg.S3Prefix,
		S3Endpoint:           cfg.S3Endpoint,
		S3Region:             cfg.S3Region,
		TargetFormat:         cfg.TargetFormat,
		DuckLakeCatalog:      cfg.DuckLakeCatalog,
		DuckLakeCatalogStore: cfg.DuckLakeCatalogStore,
		DuckLakeDataPath:     cfg.EffectiveDuckLakeDataPath(),
	}, s3Client, logger)
	if err != nil {
		return fmt.Errorf("create query server: %w", err)
	}
	defer querySrv.Close()

	// Start blocks until ctx is cancelled
	if err := querySrv.Start(ctx); err != nil {
		return fmt.Errorf("query server: %w", err)
	}

	logger.Info("streambed query server stopped")
	return nil
}

func getTargetFlushLSN(ctx context.Context, targetFormat string, catalog *iceberg.Catalog, duckWriter *ducklake.Writer, schema, table string) (string, bool, error) {
	if targetFormat == "ducklake" {
		if duckWriter == nil {
			return "", false, fmt.Errorf("ducklake writer is not initialized")
		}
		return duckWriter.GetTableFlushLSN(ctx, schema, table)
	}
	exists, err := catalog.TableExists(ctx, schema, table)
	if err != nil {
		return "", false, err
	}
	if !exists {
		return "", false, nil
	}
	return catalog.GetSnapshotFlushLSN(ctx, schema, table)
}

func runCleanup(cmd *cobra.Command, tables []string, dryRun, force bool) error {
	cfg := config.Load()
	cmd.Flags().Visit(func(f *pflag.Flag) {
		switch f.Name {
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
		case "log-level":
			cfg.LogLevel = f.Value.String()
		}
	})

	if len(tables) == 0 {
		return fmt.Errorf("at least one --table schema.table required")
	}

	// Parse + validate all table names up front.
	type tableRef struct{ schema, table string }
	refs := make([]tableRef, 0, len(tables))
	for _, t := range tables {
		parts := strings.SplitN(t, ".", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("invalid --table %q: expected schema.table", t)
		}
		refs = append(refs, tableRef{schema: parts[0], table: parts[1]})
	}

	if cfg.S3Bucket == "" {
		return fmt.Errorf("s3-bucket required")
	}

	logger := setupLogger(cfg.LogLevel)
	ctx := context.Background()

	s3Client, err := storage.NewS3Client(ctx, cfg.S3Bucket, cfg.S3Region, cfg.S3Endpoint)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	stateStore, err := state.Open(cfg.StatePath)
	if err != nil {
		return fmt.Errorf("open state store: %w", err)
	}
	defer stateStore.Close()

	reader := bufio.NewReader(os.Stdin)

	for _, ref := range refs {
		tablePrefix := path.Join(cfg.S3Prefix, ref.schema, ref.table) + "/"
		keys, err := s3Client.ListPrefix(ctx, tablePrefix)
		if err != nil {
			return fmt.Errorf("list %s: %w", tablePrefix, err)
		}

		fmt.Printf("\n%s.%s → s3://%s/%s\n", ref.schema, ref.table, cfg.S3Bucket, tablePrefix)
		fmt.Printf("  %d S3 objects\n", len(keys))

		if dryRun {
			for _, k := range keys {
				fmt.Printf("  would delete: %s\n", k)
			}
			fmt.Printf("  would delete state row for %s.%s\n", ref.schema, ref.table)
			continue
		}

		if len(keys) == 0 {
			fmt.Printf("  (no S3 objects)\n")
		}

		if !force {
			fmt.Printf("Delete %d objects + state row for %s.%s ? [y/N] ", len(keys), ref.schema, ref.table)
			line, _ := reader.ReadString('\n')
			line = strings.TrimSpace(strings.ToLower(line))
			if line != "y" && line != "yes" {
				fmt.Printf("  skipped\n")
				continue
			}
		}

		if len(keys) > 0 {
			if err := s3Client.DeleteObjects(ctx, keys); err != nil {
				return fmt.Errorf("delete S3 objects for %s.%s: %w", ref.schema, ref.table, err)
			}
			fmt.Printf("  deleted %d S3 objects\n", len(keys))
		}

		rowsDeleted, err := stateStore.DeleteTable(ref.schema, ref.table)
		if err != nil {
			return fmt.Errorf("delete state row for %s.%s: %w", ref.schema, ref.table, err)
		}
		fmt.Printf("  deleted %d state row(s)\n", rowsDeleted)
	}

	logger.Info("cleanup complete")
	return nil
}

// runMaintenance expires old Iceberg snapshots and optionally removes objects
// that are no longer reachable from retained metadata. It should not be run
// concurrently with a writer for the same table/prefix.
func runMaintenanceCompact(cmd *cobra.Command, tableRef string, targetMB, thresholdMB, maxInputFiles int, dryRun bool, force bool) error {
	cfg := config.Load()
	cmd.Flags().Visit(func(f *pflag.Flag) {
		switch f.Name {
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
		case "log-level":
			cfg.LogLevel = f.Value.String()
		}
	})
	if tableRef == "" {
		return fmt.Errorf("--table schema.table is required")
	}
	parts := strings.Split(tableRef, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("invalid table %q, want schema.table", tableRef)
	}
	if !dryRun && !force {
		return fmt.Errorf("--force is required when --dry-run=false")
	}
	if cfg.S3Bucket == "" {
		return fmt.Errorf("s3-bucket is required")
	}
	ctx := context.Background()
	s3Client, err := storage.NewS3Client(ctx, cfg.S3Bucket, cfg.S3Region, cfg.S3Endpoint)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}
	stateStore, err := state.Open(cfg.StatePath)
	if err != nil {
		return fmt.Errorf("open state store: %w", err)
	}
	defer stateStore.Close()
	catalog := iceberg.NewCatalog(s3Client, cfg.S3Bucket, cfg.S3Prefix)
	opts := iceberg.CompactionOptions{
		TargetFileSizeBytes:     int64(targetMB) * 1024 * 1024,
		SmallFileThresholdBytes: int64(thresholdMB) * 1024 * 1024,
		MaxInputFiles:           maxInputFiles,
		DryRun:                  dryRun,
	}
	plan, result, err := iceberg.CompactSmallFiles(ctx, catalog, stateStore, opts, parts[0], parts[1])
	if err != nil && !result.Aborted {
		return err
	}
	fmt.Printf("Compaction plan for %s\n", tableRef)
	fmt.Printf("  planned_snapshot: %d\n", plan.PlannedSnapshotID)
	fmt.Printf("  active_delete_files: %d\n", plan.ActiveDeleteFileCount)
	fmt.Printf("  input_files: %d\n", len(plan.InputFiles))
	fmt.Printf("  input_bytes: %d\n", plan.InputBytes)
	fmt.Printf("  estimated_output_files: %d\n", plan.EstimatedOutputFiles)
	fmt.Printf("  dry_run: %v\n", dryRun)
	if dryRun || len(plan.InputFiles) == 0 || plan.ActiveDeleteFileCount > 0 {
		if result.AbortReason != "" {
			fmt.Printf("  skipped: %s\n", result.AbortReason)
		}
		return err
	}
	fmt.Printf("Compacted %s\n", tableRef)
	fmt.Printf("  validated_snapshot: %d\n", result.ValidatedSnapshotID)
	fmt.Printf("  new_snapshot: %d\n", result.NewSnapshotID)
	fmt.Printf("  output_files: %d\n", result.OutputFiles)
	fmt.Printf("  output_bytes: %d\n", result.OutputBytes)
	fmt.Printf("  carried_files: %d\n", result.CarriedFiles)
	if result.Aborted {
		fmt.Printf("  aborted: %s\n", result.AbortReason)
	}
	return err
}

func runMaintenance(cmd *cobra.Command, tables []string, retainLast int, includeOrphans bool, dryRun bool, force bool) error {
	cfg := config.Load()
	cmd.Flags().Visit(func(f *pflag.Flag) {
		switch f.Name {
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
		case "log-level":
			cfg.LogLevel = f.Value.String()
		}
	})
	if len(tables) == 0 {
		return fmt.Errorf("--table schema.table is required")
	}
	if !dryRun && !force {
		return fmt.Errorf("--force is required when --dry-run=false")
	}
	if includeOrphans && !dryRun {
		return fmt.Errorf("orphan deletion currently supports dry-run only")
	}
	if cfg.S3Bucket == "" {
		return fmt.Errorf("s3-bucket is required")
	}
	ctx := context.Background()
	s3Client, err := storage.NewS3Client(ctx, cfg.S3Bucket, cfg.S3Region, cfg.S3Endpoint)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}
	catalog := iceberg.NewCatalog(s3Client, cfg.S3Bucket, cfg.S3Prefix)
	var stateStore *state.Store
	if !dryRun {
		stateStore, err = state.Open(cfg.StatePath)
		if err != nil {
			return fmt.Errorf("open state store: %w", err)
		}
		defer stateStore.Close()
	}
	for _, tableRef := range tables {
		parts := strings.Split(tableRef, ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("invalid table %q, want schema.table", tableRef)
		}
		var lock *state.TableCommitLock
		if !dryRun {
			lock, err = stateStore.AcquireTableCommitLock(ctx, parts[0], parts[1], "maintenance-expire", 5*time.Minute, 30*time.Second)
			if err != nil {
				return fmt.Errorf("acquire commit lock for %s: %w", tableRef, err)
			}
		}
		if lock != nil {
			ok, err := stateStore.RefreshTableCommitLock(ctx, lock, 5*time.Minute)
			if err != nil {
				return fmt.Errorf("refresh commit lock for %s: %w", tableRef, err)
			}
			if !ok {
				return fmt.Errorf("commit lock for %s expired or was stolen", tableRef)
			}
		}
		plan, err := catalog.ExpireSnapshots(ctx, parts[0], parts[1], retainLast, dryRun)
		if lock != nil {
			_ = stateStore.ReleaseTableCommitLock(context.Background(), lock)
		}
		if err != nil {
			return fmt.Errorf("expire snapshots for %s: %w", tableRef, err)
		}
		fmt.Printf("%s: expired_snapshots=%d delete_objects=%d dry_run=%v\n", tableRef, plan.ExpiredSnapshots, len(plan.DeleteObjects), dryRun)
		if includeOrphans {
			orphanPlan, err := catalog.DeleteOrphanFiles(ctx, parts[0], parts[1], dryRun)
			if err != nil {
				return fmt.Errorf("delete orphans for %s: %w", tableRef, err)
			}
			fmt.Printf("%s: orphan_delete_objects=%d dry_run=%v\n", tableRef, len(orphanPlan.DeleteObjects), dryRun)
		}
	}
	return nil
}

// runResync re-backfills a single table from Postgres using COPY under an
// exported logical-replication snapshot.
//
// Preconditions the user must ensure:
//   - The main `streambed sync` daemon is NOT running (we reuse the same
//     state file and don't want the consumer racing our writes).
//   - The publication is FOR ALL TABLES (so the main slot will pick up
//     subsequent writes to this table without operator intervention).
//
// Order of operations is chosen so that a mid-flight failure leaves the
// system in a recoverable state:
//  1. Open connections, validate flags.
//  2. Delete existing S3 objects + state row for the table.
//  3. Create temp slot + snapshot, run COPY, build parquet, commit Iceberg.
//  4. Record backfill_lsn in state. Until this is cleared by the sync
//     consumer, overlapping main-slot events are filtered out.
func runResync(cmd *cobra.Command, table string, force bool) error {
	cfg := config.Load()
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
		case "target-format":
			cfg.TargetFormat = strings.ToLower(f.Value.String())
		case "ducklake-catalog":
			cfg.DuckLakeCatalog = f.Value.String()
		case "ducklake-catalog-store":
			cfg.DuckLakeCatalogStore = strings.ToLower(f.Value.String())
		case "ducklake-data-path":
			cfg.DuckLakeDataPath = f.Value.String()
		case "flush-rows":
			if n, err := strconv.Atoi(f.Value.String()); err == nil && n > 0 {
				cfg.FlushRows = n
			}
		case "log-level":
			cfg.LogLevel = f.Value.String()
		}
	})

	if table == "" {
		return fmt.Errorf("--table schema.table is required")
	}
	parts := strings.SplitN(table, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("invalid --table %q: expected schema.table", table)
	}
	schemaName, tableName := parts[0], parts[1]

	if cfg.SourceURL == "" {
		return fmt.Errorf("--source-url is required")
	}
	if cfg.S3Bucket == "" {
		return fmt.Errorf("--s3-bucket is required")
	}
	if cfg.TargetFormat != "iceberg" && cfg.TargetFormat != "ducklake" {
		return fmt.Errorf("--target-format must be one of: iceberg, ducklake")
	}
	if cfg.TargetFormat == "ducklake" && cfg.DuckLakeCatalog == "" {
		return fmt.Errorf("--ducklake-catalog is required when --target-format=ducklake")
	}
	if cfg.DuckLakeCatalogStore != "sqlite" && cfg.DuckLakeCatalogStore != "duckdb" {
		return fmt.Errorf("--ducklake-catalog-store must be one of: sqlite, duckdb")
	}

	logger := setupLogger(cfg.LogLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Respond to SIGINT/SIGTERM so a long COPY can be interrupted cleanly.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("resync: interrupt received, cancelling")
		cancel()
	}()

	// Open dependencies.
	s3Client, err := storage.NewS3Client(ctx, cfg.S3Bucket, cfg.S3Region, cfg.S3Endpoint)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}
	stateStore, err := state.Open(cfg.StatePath)
	if err != nil {
		return fmt.Errorf("open state store: %w", err)
	}
	defer stateStore.Close()

	// Confirm destructive phase up front.
	tablePrefix := path.Join(cfg.S3Prefix, schemaName, tableName) + "/"
	var existingKeys []string
	if cfg.TargetFormat == "iceberg" {
		existingKeys, err = s3Client.ListPrefix(ctx, tablePrefix)
		if err != nil {
			return fmt.Errorf("list existing S3 objects: %w", err)
		}
	}

	fmt.Printf("\nResync %s.%s from Postgres:\n", schemaName, tableName)
	if cfg.TargetFormat == "ducklake" {
		fmt.Printf("  will drop DuckLake table if it exists in %s\n", cfg.DuckLakeCatalog)
		fmt.Printf("  DuckLake data path: %s\n", cfg.EffectiveDuckLakeDataPath())
	} else {
		fmt.Printf("  will delete %d S3 objects under s3://%s/%s\n", len(existingKeys), cfg.S3Bucket, tablePrefix)
	}
	fmt.Printf("  will delete state row for %s.%s\n", schemaName, tableName)
	fmt.Printf("  will re-COPY the table from %s\n", maskURL(cfg.SourceURL))
	if !force {
		fmt.Print("Proceed? [y/N] ")
		reader := bufio.NewReader(os.Stdin)
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(strings.ToLower(line))
		if line != "y" && line != "yes" {
			fmt.Println("aborted")
			return nil
		}
	}

	// 2. Delete existing target data + state row (idempotent).
	if cfg.TargetFormat == "iceberg" && len(existingKeys) > 0 {
		if err := s3Client.DeleteObjects(ctx, existingKeys); err != nil {
			return fmt.Errorf("delete S3 objects: %w", err)
		}
		logger.Info("resync: deleted existing S3 objects", "count", len(existingKeys))
	}
	if _, err := stateStore.DeleteTable(schemaName, tableName); err != nil {
		return fmt.Errorf("delete state row: %w", err)
	}

	// 3. Open two Postgres connections: one replication (for temp slot +
	//    snapshot export) and one regular (for COPY under that snapshot).
	replConnStr := cfg.SourceURL
	if !strings.Contains(replConnStr, "replication=") {
		if strings.Contains(replConnStr, "?") {
			replConnStr += "&replication=database"
		} else {
			replConnStr += "?replication=database"
		}
	}
	replConn, err := pgconn.Connect(ctx, replConnStr)
	if err != nil {
		return fmt.Errorf("connect to postgres (replication): %w", err)
	}
	defer replConn.Close(context.Background())

	dataConn, err := pgconn.Connect(ctx, cfg.SourceURL)
	if err != nil {
		return fmt.Errorf("connect to postgres (data): %w", err)
	}
	defer dataConn.Close(context.Background())

	// 4. Run the backfill.
	if cfg.TargetFormat == "ducklake" {
		writer, err := ducklake.NewWriter(ctx, ducklake.Config{
			CatalogPath:  cfg.DuckLakeCatalog,
			CatalogStore: cfg.DuckLakeCatalogStore,
			DataPath:     cfg.EffectiveDuckLakeDataPath(),
			S3Endpoint:   cfg.S3Endpoint,
			S3Region:     cfg.S3Region,
		}, stateStore, cfg.FlushRows, cfg.FlushInterval, logger)
		if err != nil {
			return fmt.Errorf("create ducklake writer: %w", err)
		}
		defer writer.Close()
		stats, err := ducklake.RunResync(ctx, ducklake.ResyncOptions{
			Schema:    schemaName,
			Table:     tableName,
			FlushRows: cfg.FlushRows,
			ReplConn:  replConn,
			DataConn:  dataConn,
			State:     stateStore,
			Writer:    writer,
			Logger:    logger,
		})
		if err != nil {
			return fmt.Errorf("resync %s.%s: %w", schemaName, tableName, err)
		}
		fmt.Printf("\nresync complete: %d rows, %d batch(es), backfill_lsn=%s\n",
			stats.Rows, stats.Batches, stats.BackfillLSN)
		fmt.Println("Restart `streambed sync` to resume CDC with the overlap filter active.")
		return nil
	}

	catalog := iceberg.NewCatalog(s3Client, cfg.S3Bucket, cfg.S3Prefix)
	stats, err := resync.Run(ctx, resync.Options{
		Schema:    schemaName,
		Table:     tableName,
		S3Prefix:  cfg.S3Prefix,
		FlushRows: cfg.FlushRows,
		ReplConn:  replConn,
		DataConn:  dataConn,
		State:     stateStore,
		S3:        s3Client,
		Catalog:   catalog,
		Logger:    logger,
	})
	if err != nil {
		return fmt.Errorf("resync %s.%s: %w", schemaName, tableName, err)
	}

	fmt.Printf("\nresync complete: %d rows, %d batch(es), backfill_lsn=%s\n",
		stats.Rows, stats.Batches, stats.BackfillLSN)
	fmt.Println("Restart `streambed sync` to resume CDC with the overlap filter active.")
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
