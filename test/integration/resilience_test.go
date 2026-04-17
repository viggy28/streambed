//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestLongRunningStability runs the pipeline for several minutes with continuous
// mixed DML and periodic oracle checks. Verifies no data drift over time.
// Skipped in short mode since it takes ~3 minutes.
func TestLongRunningStability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running stability test in short mode")
	}
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	execSQL(t, "ALTER TABLE test_events REPLICA IDENTITY FULL")
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	const rounds = 5
	const rowsPerRound = 200

	for i := 0; i < rounds; i++ {
		t.Logf("round %d/%d: inserting %d rows...", i+1, rounds, rowsPerRound)
		insertRows(t, rowsPerRound)

		// Some updates and deletes.
		if i > 0 {
			for j := 1; j <= 10; j++ {
				id := (i-1)*rowsPerRound + j
				execSQL(t, fmt.Sprintf("UPDATE test_events SET name = 'stability_%d' WHERE id = %d", i, id))
			}
			for j := 11; j <= 15; j++ {
				id := (i-1)*rowsPerRound + j
				execSQL(t, fmt.Sprintf("DELETE FROM test_events WHERE id = %d", id))
			}
		}

		t.Logf("round %d/%d: syncing for 30s...", i+1, rounds)
		runSync(t, ctx, 30*time.Second, sharedStatePath)

		// Oracle check after each round.
		assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})
		t.Logf("round %d/%d: oracle check passed", i+1, rounds)
	}

	// Final verification.
	pgCount := pgRowCount(t, "test_events")
	t.Logf("stability test complete: %d rows in PG after %d rounds", pgCount, rounds)

	cleanup(t)
}

// TestCrashMidFlush_DataRecovery inserts rows, cancels the pipeline mid-flush,
// restarts, and uses the oracle to verify all data is present.
func TestCrashMidFlush_DataRecovery(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	// Insert 200 rows.
	t.Log("inserting 200 rows...")
	insertRows(t, 200)

	// Very short sync — likely to be interrupted mid-flush.
	t.Log("short sync (3s) — simulating crash...")
	runSync(t, ctx, 3*time.Second, sharedStatePath)

	// Insert 100 more while pipeline is down.
	t.Log("inserting 100 more rows while pipeline is down...")
	insertRows(t, 100)

	// Restart with longer duration to ensure everything flushes.
	t.Log("restarting pipeline (20s)...")
	runSync(t, ctx, 20*time.Second, sharedStatePath)

	// Oracle check: all 300 rows must be present.
	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	pgCount := pgRowCount(t, "test_events")
	if pgCount != 300 {
		t.Fatalf("expected 300 rows in PG, got %d", pgCount)
	}

	cleanup(t)
}

// TestMultipleRestartsWithUpdates runs multiple restart cycles with interleaved
// updates to verify dedup correctness across restarts.
func TestMultipleRestartsWithUpdates(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	execSQL(t, "ALTER TABLE test_events REPLICA IDENTITY FULL")
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	// Phase 1: Insert base rows.
	t.Log("phase 1: inserting 50 rows...")
	insertRows(t, 50)

	runSync(t, ctx, 12*time.Second, sharedStatePath)
	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	// Phase 2: Short sync with updates (simulates crash mid-update).
	t.Log("phase 2: updating 20 rows, short sync...")
	for i := 1; i <= 20; i++ {
		execSQL(t, fmt.Sprintf("UPDATE test_events SET name = 'v2_%d' WHERE id = %d", i, i))
	}
	runSync(t, ctx, 5*time.Second, sharedStatePath)

	// Phase 3: More updates and restart.
	t.Log("phase 3: more updates, full sync...")
	for i := 21; i <= 40; i++ {
		execSQL(t, fmt.Sprintf("UPDATE test_events SET name = 'v3_%d' WHERE id = %d", i, i))
	}
	runSync(t, ctx, 15*time.Second, sharedStatePath)

	// Final oracle check.
	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	cleanup(t)
}
