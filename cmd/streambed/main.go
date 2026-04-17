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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/viggy28/streambed/config"
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
	syncCmd.Flags().StringVar(&cfg.SlotName, "slot-name", cfg.SlotName, "Replication slot name")
	syncCmd.Flags().IntVar(&cfg.FlushRows, "flush-rows", cfg.FlushRows, "Row buffer flush threshold")
	syncCmd.Flags().DurationVar(&cfg.FlushInterval, "flush-interval", cfg.FlushInterval, "Time-based flush interval")
	syncCmd.Flags().StringSliceVar(&cfg.IncludeTables, "include-tables", cfg.IncludeTables, "Tables to include")
	syncCmd.Flags().StringSliceVar(&cfg.ExcludeTables, "exclude-tables", cfg.ExcludeTables, "Tables to exclude")
	syncCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level (DEBUG, INFO, WARN, ERROR)")
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
	resyncCmd.Flags().IntVar(&cfg.FlushRows, "flush-rows", cfg.FlushRows, "Rows per parquet file / Iceberg snapshot")
	resyncCmd.Flags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level (DEBUG, INFO, WARN, ERROR)")

	rootCmd.AddCommand(syncCmd, queryCmd, cleanupCmd, resyncCmd)

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

	// Start query server if --query-addr is set
	if cfg.QueryAddr != "" {
		querySrv, err := server.NewServer(server.ServerConfig{
			ListenAddr: cfg.QueryAddr,
			S3Bucket:   cfg.S3Bucket,
			S3Prefix:   cfg.S3Prefix,
			S3Endpoint: cfg.S3Endpoint,
			S3Region:   cfg.S3Region,
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

	// Initialize Iceberg catalog
	catalog := iceberg.NewCatalog(s3Client, cfg.S3Bucket, cfg.S3Prefix)

	// Read per-table flush LSNs from Iceberg (the sole source of truth).
	// These are used for dedup on restart and to determine startLSN.
	tableFlushLSN := make(map[string]pglogrepl.LSN)
	registeredTables, err := stateStore.GetRegisteredTables()
	if err != nil {
		return fmt.Errorf("get registered tables: %w", err)
	}
	for _, t := range registeredTables {
		exists, err := catalog.TableExists(ctx, t.Schema, t.Table)
		if err != nil {
			return fmt.Errorf("check table %s.%s: %w", t.Schema, t.Table, err)
		}
		if !exists {
			continue
		}
		lsnStr, found, err := catalog.GetSnapshotFlushLSN(ctx, t.Schema, t.Table)
		if err != nil {
			logger.Warn("cannot read Iceberg LSN, skipping",
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
			return fmt.Errorf("parse Iceberg LSN %q for %s.%s: %w", lsnStr, t.Schema, t.Table, err)
		}
		tableFlushLSN[fmt.Sprintf("%s.%s", t.Schema, t.Table)] = lsn
	}

	var minIcebergLSN pglogrepl.LSN
	for _, lsn := range tableFlushLSN {
		if minIcebergLSN == 0 || lsn < minIcebergLSN {
			minIcebergLSN = lsn
		}
	}

	startLSN := slotLSN
	if minIcebergLSN > startLSN {
		startLSN = minIcebergLSN
		logger.Info("resuming from Iceberg LSN", "lsn", startLSN)
	} else if minIcebergLSN < startLSN && minIcebergLSN != 0 {
		// This is normal: cold tables retain an older flush LSN while
		// the slot advances past them (they had no events in the gap).
		// Log for visibility but don't block startup.
		logger.Info("slot ahead of coldest Iceberg LSN (cold tables expected)",
			"slot_lsn", startLSN,
			"min_iceberg_lsn", minIcebergLSN,
		)
	}

	// Initialize writer
	writer := iceberg.NewWriter(catalog, s3Client, stateStore, cfg.SlotName,
		cfg.FlushRows, cfg.FlushInterval, logger)

	// Create unified pipeline (single goroutine: reads WAL + writes Iceberg)
	p := pipeline.New(pgConn, cfg.SlotName, pubName, startLSN, cfg.ExcludeTables,
		logger, stateStore, tableFlushLSN, writer, cfg.FlushInterval)

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

		// Re-read per-table flush LSNs from Iceberg (may have advanced
		// from the last successful flush before the crash).
		registeredTables, err = stateStore.GetRegisteredTables()
		if err != nil {
			return fmt.Errorf("get registered tables on reconnect: %w", err)
		}
		tableFlushLSN = make(map[string]pglogrepl.LSN)
		for _, t := range registeredTables {
			exists, err := catalog.TableExists(ctx, t.Schema, t.Table)
			if err != nil || !exists {
				continue
			}
			lsnStr, found, err := catalog.GetSnapshotFlushLSN(ctx, t.Schema, t.Table)
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
		writer = iceberg.NewWriter(catalog, s3Client, stateStore, cfg.SlotName,
			cfg.FlushRows, cfg.FlushInterval, logger)
		p = pipeline.New(pgConn, cfg.SlotName, pubName, startLSN, cfg.ExcludeTables,
			logger, stateStore, tableFlushLSN, writer, cfg.FlushInterval)

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
		ListenAddr: cfg.QueryAddr,
		S3Bucket:   cfg.S3Bucket,
		S3Prefix:   cfg.S3Prefix,
		S3Endpoint: cfg.S3Endpoint,
		S3Region:   cfg.S3Region,
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
	existingKeys, err := s3Client.ListPrefix(ctx, tablePrefix)
	if err != nil {
		return fmt.Errorf("list existing S3 objects: %w", err)
	}

	fmt.Printf("\nResync %s.%s from Postgres:\n", schemaName, tableName)
	fmt.Printf("  will delete %d S3 objects under s3://%s/%s\n", len(existingKeys), cfg.S3Bucket, tablePrefix)
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

	// 2. Delete existing S3 data + state row (idempotent).
	if len(existingKeys) > 0 {
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
