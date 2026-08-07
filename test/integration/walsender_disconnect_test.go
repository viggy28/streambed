//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
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

// TestWalsenderDisconnectResumeExactlyOnce verifies that an abrupt logical
// replication connection loss after a durable flush but before all received WAL
// has been flushed does not create duplicate or missing Iceberg rows after the
// next sync run resumes from the replication slot.
func TestWalsenderDisconnectResumeExactlyOnce(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	t.Cleanup(func() { cleanup(t) })
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	// Phase 1 writes more than one flush threshold. The first 500 rows should
	// flush durably, while rows 501-600 remain buffered when the walsender is
	// terminated below.
	t.Log("phase 1: inserting 600 rows before starting sync...")
	insertRows(t, 600)

	pid, done, cancel := startSyncPipelineForDisconnectTest(t, ctx, sharedStatePath, 500, time.Hour)
	defer cancel()

	flushed := waitForNamedTableSnapshotRowsAtLeast(t, "public", "test_events", 500, 30*time.Second)
	t.Logf("phase 1: observed durable Iceberg snapshot with %d rows; terminating walsender pid %d", flushed, pid)
	terminatePostgresBackend(t, pid)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected pipeline error after terminating walsender, got nil")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected walsender disconnect error, got context error: %v", err)
		}
		t.Logf("pipeline stopped after walsender disconnect: %v", err)
	case <-time.After(15 * time.Second):
		cancel()
		t.Fatal("pipeline did not stop after terminating walsender")
	}

	// Phase 2 adds more WAL while the pipeline is down. The restart must replay
	// unacked/unflushed rows without duplicating the rows already committed to
	// Iceberg in phase 1.
	t.Log("phase 2: inserting 400 more rows while sync is down...")
	insertRows(t, 400)

	t.Log("phase 2: restarting sync to resume from slot...")
	runSync(t, ctx, 20*time.Second, sharedStatePath)

	if pgCount := pgRowCount(t, "test_events"); pgCount != 1000 {
		t.Fatalf("expected 1000 rows in Postgres, got %d", pgCount)
	}

	assertPgIcebergMatch(t, duckDB, "public", "test_events", []string{"id"}, []string{"id", "name"})
}

func startSyncPipelineForDisconnectTest(t *testing.T, ctx context.Context, statePath string, flushRowCount int, flushInterval time.Duration) (uint32, <-chan error, context.CancelFunc) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	stateStore, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open state store: %v", err)
	}

	s3Client, err := storage.NewS3Client(ctx, s3Bucket, s3Region, minioEndpoint)
	if err != nil {
		stateStore.Close()
		t.Fatalf("create S3 client: %v", err)
	}

	pgConn, err := pgconn.Connect(ctx, pgReplConnStr())
	if err != nil {
		stateStore.Close()
		t.Fatalf("connect to postgres for replication: %v", err)
	}
	pid := pgConn.PID()

	if err := wal.CreatePublication(ctx, pgConn, slotName, nil, logger); err != nil {
		pgConn.Close(context.Background())
		stateStore.Close()
		t.Fatalf("create publication: %v", err)
	}
	slotLSN, err := wal.CreateOrReuseSlot(ctx, pgConn, slotName, logger)
	if err != nil {
		pgConn.Close(context.Background())
		stateStore.Close()
		t.Fatalf("setup replication slot: %v", err)
	}

	catalog := iceberg.NewCatalog(s3Client, s3Bucket, s3Prefix)
	tableFlushLSN := make(map[string]pglogrepl.LSN)
	registeredTables, err := stateStore.GetRegisteredTables()
	if err != nil {
		pgConn.Close(context.Background())
		stateStore.Close()
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

	writer := iceberg.NewWriter(catalog, s3Client, stateStore, slotName,
		flushRowCount, flushInterval, logger, iceberg.WithMutationMode(iceberg.MutationModeCOW))

	metaConn, err := pgx.Connect(ctx, pgConnStr())
	if err != nil {
		pgConn.Close(context.Background())
		stateStore.Close()
		t.Fatalf("connect metadata: %v", err)
	}
	metaQuerier := wal.NewMetadataQuerier(metaConn)

	p := pipeline.New(pgConn, slotName, slotName, slotLSN, nil,
		logger, stateStore, tableFlushLSN, writer, flushInterval, metaQuerier)

	syncCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		defer stateStore.Close()
		defer pgConn.Close(context.Background())
		defer metaConn.Close(context.Background())
		done <- p.Run(syncCtx)
	}()

	return pid, done, cancel
}

func waitForNamedTableSnapshotRowsAtLeast(t *testing.T, schema, table string, minRows int64, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var rows int64
	for time.Now().Before(deadline) {
		rows = countNamedTableSnapshotRows(t, schema, table)
		if rows >= minRows {
			return rows
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s.%s Iceberg snapshot to contain at least %d rows; last count=%d", schema, table, minRows, rows)
	return 0
}

func terminatePostgresBackend(t *testing.T, pid uint32) {
	t.Helper()
	if pid == 0 {
		t.Fatal("cannot terminate postgres backend with pid 0")
	}
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect to terminate backend: %v", err)
	}
	defer conn.Close(ctx)

	result := conn.Exec(ctx, fmt.Sprintf("SELECT pg_terminate_backend(%d)", pid))
	results, err := result.ReadAll()
	if err != nil {
		t.Fatalf("terminate backend %d: %v", pid, err)
	}
	if len(results) == 0 || len(results[0].Rows) == 0 || string(results[0].Rows[0][0]) != "t" {
		t.Fatalf("pg_terminate_backend(%d) did not return true", pid)
	}
}
