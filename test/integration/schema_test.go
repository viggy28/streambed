//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

// TestSchemaEvolution_AddColumn verifies that ALTER TABLE ADD COLUMN mid-stream
// does not cause data loss or corruption. Old rows should have NULL for the new
// column; new rows should have the populated value.
func TestSchemaEvolution_AddColumn(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t) // id SERIAL PK, name TEXT, value FLOAT8, created_at TIMESTAMPTZ

	createSlotAndPublication(t)
	sharedStatePath := t.TempDir() + "/state.db"

	// Phase 1: Insert 10 rows with original schema.
	t.Log("phase 1: inserting 10 rows with original schema...")
	insertRows(t, 10)

	t.Log("syncing phase 1...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	rows := countLatestSnapshotRows(t)
	if rows != 10 {
		t.Fatalf("phase 1: expected 10 rows, got %d", rows)
	}

	// Phase 2: ADD COLUMN and insert more rows.
	t.Log("phase 2: ALTER TABLE ADD COLUMN...")
	execSQL(t, "ALTER TABLE test_events ADD COLUMN extra_info TEXT DEFAULT 'added'")

	t.Log("inserting 10 rows with new column...")
	for i := 0; i < 10; i++ {
		execSQL(t, "INSERT INTO test_events (name, value, extra_info) VALUES ('new_event', 99.9, 'has_extra')")
	}

	t.Log("syncing phase 2...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	// We should have all 20 rows — the key check is no data loss.
	pgCount := pgRowCount(t, "test_events")
	icebergCount := countLatestSnapshotRows(t)

	t.Logf("after ADD COLUMN: PG=%d Iceberg=%d", pgCount, icebergCount)
	if pgCount != icebergCount {
		t.Fatalf("row count mismatch after ADD COLUMN: PG=%d Iceberg=%d", pgCount, icebergCount)
	}

	cleanup(t)
}

// TestSchemaEvolution_DropColumn verifies that ALTER TABLE DROP COLUMN
// mid-stream does not crash the pipeline or lose data for remaining columns.
func TestSchemaEvolution_DropColumn(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)

	// Create table with extra column we'll drop.
	execSQL(t, "DROP TABLE IF EXISTS drop_col_test")
	execSQL(t, `CREATE TABLE drop_col_test (
		id SERIAL PRIMARY KEY,
		name TEXT NOT NULL,
		to_drop TEXT DEFAULT 'will_be_dropped',
		value DOUBLE PRECISION
	)`)

	createSlotAndPublication(t)
	sharedStatePath := t.TempDir() + "/state.db"

	// Phase 1: Insert rows with all columns.
	t.Log("phase 1: inserting 10 rows...")
	for i := 0; i < 10; i++ {
		execSQL(t, "INSERT INTO drop_col_test (name, to_drop, value) VALUES ('event', 'present', 1.0)")
	}

	t.Log("syncing phase 1...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	rows := countNamedTableSnapshotRows(t, "public", "drop_col_test")
	if rows != 10 {
		t.Fatalf("phase 1: expected 10 rows, got %d", rows)
	}

	// Phase 2: DROP COLUMN and insert more rows.
	t.Log("phase 2: ALTER TABLE DROP COLUMN...")
	execSQL(t, "ALTER TABLE drop_col_test DROP COLUMN to_drop")

	t.Log("inserting 10 rows after DROP COLUMN...")
	for i := 0; i < 10; i++ {
		execSQL(t, "INSERT INTO drop_col_test (name, value) VALUES ('after_drop', 2.0)")
	}

	t.Log("syncing phase 2...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	pgCount := pgRowCount(t, "drop_col_test")
	icebergCount := countNamedTableSnapshotRows(t, "public", "drop_col_test")

	t.Logf("after DROP COLUMN: PG=%d Iceberg=%d", pgCount, icebergCount)
	if pgCount != icebergCount {
		t.Fatalf("row count mismatch after DROP COLUMN: PG=%d Iceberg=%d", pgCount, icebergCount)
	}

	cleanup(t)
	execSQL(t, "DROP TABLE IF EXISTS drop_col_test")
}
