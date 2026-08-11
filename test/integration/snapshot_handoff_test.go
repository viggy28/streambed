//go:build integration

package integration

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/viggy28/streambed/internal/iceberg"
	resyncpkg "github.com/viggy28/streambed/internal/resync"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/storage"
)

func TestSnapshotToCDCHandoff_NoGapNoDuplicate(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()
	const table = "snapshot_handoff_events"

	cleanup(t)
	clearS3Prefix(t)
	setupNamedTable(t, table)
	t.Cleanup(func() { cleanup(t) })

	// Create the permanent slot before the source rows so CDC will later replay
	// WAL that overlaps the snapshot. The resync backfill_lsn must suppress those
	// duplicates while still allowing rows committed after the snapshot to apply.
	createSlotAndPublication(t)
	sharedStatePath := t.TempDir() + "/state.db"

	insertNamedRows(t, table, 20)

	stats := runResyncForIntegration(t, ctx, table, sharedStatePath, 10)
	if stats.Rows != 20 {
		t.Fatalf("resync copied %d rows, want 20", stats.Rows)
	}
	if stats.BackfillLSN == "" {
		t.Fatal("resync did not report a backfill LSN")
	}
	if got := countNamedTableSnapshotRows(t, "public", table); got != 20 {
		t.Fatalf("after resync snapshot rows = %d, want 20", got)
	}

	// These rows commit after the snapshot LSN but before the CDC pipeline starts.
	// They are the handoff window: dropping them would create a gap.
	insertNamedRows(t, table, 5)

	runSync(t, ctx, 12*time.Second, sharedStatePath)

	if got := countNamedTableSnapshotRows(t, "public", table); got != 25 {
		t.Fatalf("after CDC handoff snapshot rows = %d, want 25", got)
	}
	assertBackfillFilterCleared(t, sharedStatePath, "public."+table)

	duckDB := newTestDuckDB(t)
	// Validate the stable value columns. Timestamp rendering differs between
	// Postgres text output and DuckDB's Iceberg scan, and is covered by the
	// dedicated type-roundtrip tests.
	assertPgIcebergMatch(t, duckDB, "public", table, []string{"id"}, []string{"id", "name"})
}

func runResyncForIntegration(t *testing.T, ctx context.Context, table, statePath string, flushRows int) resyncpkg.Stats {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	stateStore, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open state store: %v", err)
	}
	defer stateStore.Close()

	s3Client, err := storage.NewS3Client(ctx, s3Bucket, s3Region, minioEndpoint)
	if err != nil {
		t.Fatalf("create S3 client: %v", err)
	}
	catalog := iceberg.NewCatalog(s3Client, s3Bucket, s3Prefix)

	replConn, err := pgconn.Connect(ctx, pgReplConnStr())
	if err != nil {
		t.Fatalf("connect resync replication conn: %v", err)
	}
	defer replConn.Close(context.Background())

	dataConn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect resync data conn: %v", err)
	}
	defer dataConn.Close(context.Background())

	stats, err := resyncpkg.Run(ctx, resyncpkg.Options{
		Schema:    "public",
		Table:     table,
		S3Prefix:  s3Prefix,
		FlushRows: flushRows,
		ReplConn:  replConn,
		DataConn:  dataConn,
		State:     stateStore,
		S3:        s3Client,
		Catalog:   catalog,
		Logger:    logger,
	})
	if err != nil {
		t.Fatalf("resync %s: %v", table, err)
	}
	return stats
}

func assertBackfillFilterCleared(t *testing.T, statePath, tableKey string) {
	t.Helper()

	stateStore, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open state store: %v", err)
	}
	defer stateStore.Close()

	filters, err := stateStore.GetBackfillLSNs()
	if err != nil {
		t.Fatalf("get backfill filters: %v", err)
	}
	if lsn, ok := filters[tableKey]; ok {
		t.Fatalf("backfill filter for %s still set after CDC handoff: %s", tableKey, lsn)
	}
}
