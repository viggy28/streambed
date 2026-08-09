//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/viggy28/streambed/internal/failpoint"
)

func TestAckDoesNotAdvancePastPendingBuffer(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()
	prepareAckFailureTest(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	statePath := t.TempDir() + "/state.db"
	insertRows(t, 10)
	walEnd := currentWALInsertLSN(t)

	fp := failpoint.EnableBlock("before_standby_status_update")
	pid, done, cancel := startSyncPipelineForDisconnectTest(t, ctx, statePath, 1000, time.Hour)
	defer cancel()
	defer terminateIfRunning(t, pid)

	waitCtx, waitCancel := context.WithTimeout(ctx, 20*time.Second)
	defer waitCancel()
	if err := fp.WaitHit(waitCtx); err != nil {
		t.Fatalf("wait for standby failpoint: %v", err)
	}
	fp.Release()
	waitForSlotLSN(t, func(lsn pglogrepl.LSN) bool { return lsn > 0 }, 10*time.Second)

	pendingAck := getSlotConfirmedFlushLSN(t)
	if pendingAck >= walEnd {
		t.Fatalf("slot ack advanced past pending rows: confirmed=%s wal_end=%s", pendingAck, walEnd)
	}

	cancel()
	waitPipelineDone(t, done, 20*time.Second, true)
	runSync(t, ctx, 12*time.Second, statePath)

	finalAck := getSlotConfirmedFlushLSN(t)
	if finalAck <= pendingAck {
		t.Fatalf("slot ack did not advance after durable flush: before=%s after=%s", pendingAck, finalAck)
	}
	if got := countNamedTableSnapshotRows(t, "public", "test_events"); got != 10 {
		t.Fatalf("Iceberg rows = %d, want 10", got)
	}
}

func TestParquetWriteFailureDoesNotAck(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()
	prepareAckFailureTest(t)
	setupTestTable(t)
	createSlotAndPublication(t)
	statePath := t.TempDir() + "/state.db"

	insertRows(t, 10)
	walEnd := currentWALInsertLSN(t)
	failpoint.EnableError("before_parquet_write", fmt.Errorf("forced parquet write failure"))

	_, done, cancel := startSyncPipelineForDisconnectTest(t, ctx, statePath, 5, time.Hour)
	defer cancel()
	err := waitPipelineDone(t, done, 20*time.Second, false)
	if err == nil || !strings.Contains(err.Error(), "forced parquet write failure") {
		t.Fatalf("pipeline error = %v, want forced parquet write failure", err)
	}
	assertAckHeldBefore(t, walEnd)
	if got := countNamedTableSnapshotRows(t, "public", "test_events"); got != 0 {
		t.Fatalf("Iceberg rows after failed parquet write = %d, want 0", got)
	}

	failpoint.DisableAll()
	runSync(t, ctx, 12*time.Second, statePath)
	assertTestEventsMatch(t, 10)
}

func TestIcebergMetadataCommitFailureDoesNotAck(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()
	prepareAckFailureTest(t)
	setupTestTable(t)
	createSlotAndPublication(t)
	statePath := t.TempDir() + "/state.db"

	insertRows(t, 10)
	walEnd := currentWALInsertLSN(t)
	failpoint.EnableError("before_metadata_commit", fmt.Errorf("forced metadata commit failure"))

	_, done, cancel := startSyncPipelineForDisconnectTest(t, ctx, statePath, 5, time.Hour)
	defer cancel()
	err := waitPipelineDone(t, done, 20*time.Second, false)
	if err == nil || !strings.Contains(err.Error(), "forced metadata commit failure") {
		t.Fatalf("pipeline error = %v, want forced metadata commit failure", err)
	}
	assertAckHeldBefore(t, walEnd)
	if got := countNamedTableSnapshotRows(t, "public", "test_events"); got != 0 {
		t.Fatalf("Iceberg rows after failed metadata commit = %d, want 0", got)
	}

	failpoint.DisableAll()
	runSync(t, ctx, 12*time.Second, statePath)
	assertTestEventsMatch(t, 10)
}

func TestCrashAfterIcebergCommitBeforeAckReplaysIdempotently(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()
	prepareAckFailureTest(t)
	setupTestTable(t)
	createSlotAndPublication(t)
	statePath := t.TempDir() + "/state.db"

	insertRows(t, 10)
	walEnd := currentWALInsertLSN(t)
	failpoint.EnableError("after_metadata_commit_before_state_or_ack", fmt.Errorf("forced post-commit pre-ack crash"))

	_, done, cancel := startSyncPipelineForDisconnectTest(t, ctx, statePath, 5, time.Hour)
	defer cancel()
	err := waitPipelineDone(t, done, 20*time.Second, false)
	if err == nil || !strings.Contains(err.Error(), "forced post-commit pre-ack crash") {
		t.Fatalf("pipeline error = %v, want forced post-commit pre-ack crash", err)
	}
	assertAckHeldBefore(t, walEnd)
	if got := countNamedTableSnapshotRows(t, "public", "test_events"); got != 5 {
		t.Fatalf("Iceberg rows after committed first flush = %d, want 5", got)
	}

	failpoint.DisableAll()
	runSync(t, ctx, 15*time.Second, statePath)
	assertTestEventsMatch(t, 10)
}

func TestTemporaryObjectStoreOutageHoldsAckAndRecovers(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()
	prepareAckFailureTest(t)
	setupTestTable(t)
	createSlotAndPublication(t)
	statePath := t.TempDir() + "/state.db"

	insertRows(t, 10)
	walEnd := currentWALInsertLSN(t)
	fp := failpoint.EnableBlock("before_parquet_write")

	_, done, cancel := startSyncPipelineForDisconnectTest(t, ctx, statePath, 5, time.Hour)
	defer cancel()
	waitCtx, waitCancel := context.WithTimeout(ctx, 20*time.Second)
	defer waitCancel()
	if err := fp.WaitHit(waitCtx); err != nil {
		t.Fatalf("wait for parquet write failpoint: %v", err)
	}
	assertAckHeldBefore(t, walEnd)
	fp.Release()

	waitForNamedTableSnapshotRowsAtLeast(t, "public", "test_events", 10, 20*time.Second)
	failpoint.DisableAll()
	cancel()
	waitPipelineDone(t, done, 20*time.Second, true)
	runSync(t, ctx, 12*time.Second, statePath)
	assertTestEventsMatch(t, 10)
}

func prepareAckFailureTest(t *testing.T) {
	t.Helper()
	failpoint.DisableAll()
	t.Cleanup(failpoint.DisableAll)
	cleanup(t)
	t.Cleanup(func() { cleanup(t) })
	clearS3Prefix(t)
}

func currentWALInsertLSN(t *testing.T) pglogrepl.LSN {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect for current WAL LSN: %v", err)
	}
	defer conn.Close(ctx)
	results, err := conn.Exec(ctx, "SELECT pg_current_wal_insert_lsn()").ReadAll()
	if err != nil {
		t.Fatalf("query current WAL LSN: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(string(results[0].Rows[0][0]))
	if err != nil {
		t.Fatalf("parse current WAL LSN: %v", err)
	}
	return lsn
}

func waitPipelineDone(t *testing.T, done <-chan error, timeout time.Duration, allowContext bool) error {
	t.Helper()
	select {
	case err := <-done:
		if allowContext {
			return err
		}
		return err
	case <-time.After(timeout):
		t.Fatalf("pipeline did not stop within %s", timeout)
		return nil
	}
}

func assertAckHeldBefore(t *testing.T, walEnd pglogrepl.LSN) {
	t.Helper()
	ack := getSlotConfirmedFlushLSN(t)
	if ack >= walEnd {
		t.Fatalf("slot ack advanced past non-durable WAL: confirmed=%s wal_end=%s", ack, walEnd)
	}
}

func waitForSlotLSN(t *testing.T, pred func(pglogrepl.LSN) bool, timeout time.Duration) pglogrepl.LSN {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lsn pglogrepl.LSN
	for time.Now().Before(deadline) {
		lsn = getSlotConfirmedFlushLSN(t)
		if pred(lsn) {
			return lsn
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for slot LSN predicate; last=%s", lsn)
	return 0
}

func assertTestEventsMatch(t *testing.T, want int64) {
	t.Helper()
	if pgCount := pgRowCount(t, "test_events"); pgCount != want {
		t.Fatalf("Postgres rows = %d, want %d", pgCount, want)
	}
	if got := countNamedTableSnapshotRows(t, "public", "test_events"); got != want {
		t.Fatalf("Iceberg rows = %d, want %d", got, want)
	}
	duckDB := newTestDuckDB(t)
	assertPgIcebergMatch(t, duckDB, "public", "test_events", []string{"id"}, []string{"id", "name"})
}

func terminateIfRunning(t *testing.T, pid uint32) {
	t.Helper()
	if pid == 0 {
		return
	}
	_ = pid // process exits through context cancellation in normal test flow.
}
