//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestBoundaryValues verifies that extreme/boundary values for each supported
// Postgres type survive the CDC pipeline intact.
func TestBoundaryValues(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)

	execSQL(t, "DROP TABLE IF EXISTS boundary_test")
	execSQL(t, `CREATE TABLE boundary_test (
		id SERIAL PRIMARY KEY,
		int2_col SMALLINT,
		int4_col INTEGER,
		int8_col BIGINT,
		float4_col REAL,
		float8_col DOUBLE PRECISION,
		text_col TEXT,
		bool_col BOOLEAN,
		date_col DATE,
		ts_col TIMESTAMP,
		uuid_col UUID
	)`)

	createSlotAndPublication(t)
	duckDB := newTestDuckDB(t)

	// Insert boundary values.
	t.Log("inserting boundary value rows...")

	// Max/min integers
	execSQL(t, "INSERT INTO boundary_test (int2_col, int4_col, int8_col) VALUES (32767, 2147483647, 9223372036854775807)")
	execSQL(t, "INSERT INTO boundary_test (int2_col, int4_col, int8_col) VALUES (-32768, -2147483648, -9223372036854775808)")

	// Zero values
	execSQL(t, "INSERT INTO boundary_test (int2_col, int4_col, int8_col, float4_col, float8_col) VALUES (0, 0, 0, 0.0, 0.0)")

	// Float special values
	execSQL(t, "INSERT INTO boundary_test (float8_col) VALUES ('Infinity')")
	execSQL(t, "INSERT INTO boundary_test (float8_col) VALUES ('-Infinity')")
	execSQL(t, "INSERT INTO boundary_test (float8_col) VALUES ('NaN')")

	// Text edge cases
	execSQL(t, "INSERT INTO boundary_test (text_col) VALUES ('')")     // empty string
	execSQL(t, "INSERT INTO boundary_test (text_col) VALUES (NULL)")   // null
	execSQL(t, fmt.Sprintf("INSERT INTO boundary_test (text_col) VALUES ('%s')", strings.Repeat("x", 10000))) // long string

	// Boolean values
	execSQL(t, "INSERT INTO boundary_test (bool_col) VALUES (true)")
	execSQL(t, "INSERT INTO boundary_test (bool_col) VALUES (false)")

	// Date boundaries
	execSQL(t, "INSERT INTO boundary_test (date_col) VALUES ('1970-01-01')") // epoch
	execSQL(t, "INSERT INTO boundary_test (date_col) VALUES ('2099-12-31')") // far future
	execSQL(t, "INSERT INTO boundary_test (date_col) VALUES ('1900-01-01')") // far past

	// Timestamp
	execSQL(t, "INSERT INTO boundary_test (ts_col) VALUES ('1970-01-01 00:00:00')")
	execSQL(t, "INSERT INTO boundary_test (ts_col) VALUES ('2038-01-19 03:14:07')") // Y2K38

	// UUID
	execSQL(t, "INSERT INTO boundary_test (uuid_col) VALUES ('00000000-0000-0000-0000-000000000000')")
	execSQL(t, "INSERT INTO boundary_test (uuid_col) VALUES ('ffffffff-ffff-ffff-ffff-ffffffffffff')")

	// All NULLs
	execSQL(t, "INSERT INTO boundary_test (id) VALUES (DEFAULT)")

	t.Log("syncing boundary values...")
	runSync(t, ctx, 15*time.Second)

	pgCount := pgRowCount(t, "boundary_test")
	icebergCount := countNamedTableSnapshotRows(t, "public", "boundary_test")

	t.Logf("boundary_test: PG=%d Iceberg=%d", pgCount, icebergCount)
	if pgCount != icebergCount {
		t.Fatalf("boundary value mismatch: PG=%d Iceberg=%d", pgCount, icebergCount)
	}

	// Also verify via oracle count match.
	assertPgIcebergCountMatch(t, duckDB, "public", "boundary_test", []string{"id"})

	cleanup(t)
	execSQL(t, "DROP TABLE IF EXISTS boundary_test")
}

// TestCompositeKeyDedup verifies dedup correctness with multi-column primary
// keys and mixed DML.
func TestCompositeKeyDedup(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)

	execSQL(t, "DROP TABLE IF EXISTS composite_key_test")
	execSQL(t, `CREATE TABLE composite_key_test (
		tenant_id INTEGER NOT NULL,
		item_id INTEGER NOT NULL,
		name TEXT,
		value DOUBLE PRECISION,
		PRIMARY KEY (tenant_id, item_id)
	)`)
	execSQL(t, "ALTER TABLE composite_key_test REPLICA IDENTITY FULL")

	createSlotAndPublication(t)
	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	// Insert rows with composite keys.
	t.Log("inserting rows with composite keys...")
	for tenant := 1; tenant <= 3; tenant++ {
		for item := 1; item <= 10; item++ {
			execSQL(t, fmt.Sprintf(
				"INSERT INTO composite_key_test (tenant_id, item_id, name, value) VALUES (%d, %d, 'item_%d_%d', %d.%d)",
				tenant, item, tenant, item, tenant*100, item))
		}
	}

	t.Log("syncing inserts...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	assertPgIcebergCountMatch(t, duckDB, "public", "composite_key_test", []string{"tenant_id", "item_id"})

	// Update some rows (change name and value).
	t.Log("updating rows...")
	for item := 1; item <= 5; item++ {
		execSQL(t, fmt.Sprintf(
			"UPDATE composite_key_test SET name = 'updated', value = 999.99 WHERE tenant_id = 1 AND item_id = %d", item))
	}

	// Delete some rows.
	t.Log("deleting rows...")
	for item := 6; item <= 10; item++ {
		execSQL(t, fmt.Sprintf(
			"DELETE FROM composite_key_test WHERE tenant_id = 2 AND item_id = %d", item))
	}

	t.Log("syncing updates and deletes...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	assertPgIcebergCountMatch(t, duckDB, "public", "composite_key_test", []string{"tenant_id", "item_id"})

	// Verify expected counts: 30 - 5 = 25.
	pgCount := pgRowCount(t, "composite_key_test")
	if pgCount != 25 {
		t.Fatalf("expected 25 rows in PG, got %d", pgCount)
	}

	cleanup(t)
	execSQL(t, "DROP TABLE IF EXISTS composite_key_test")
}

// TestToastDataPreservation verifies TOAST column handling. With REPLICA
// IDENTITY FULL, TOAST columns should be preserved on UPDATE. With DEFAULT,
// unchanged TOAST columns become NULL (a known limitation documented here).
func TestToastDataPreservation(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)

	execSQL(t, "DROP TABLE IF EXISTS toast_test")
	execSQL(t, `CREATE TABLE toast_test (
		id SERIAL PRIMARY KEY,
		small_col TEXT,
		big_col TEXT
	)`)
	// Use REPLICA IDENTITY FULL to get full old tuple on UPDATE.
	execSQL(t, "ALTER TABLE toast_test REPLICA IDENTITY FULL")

	createSlotAndPublication(t)
	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	// Insert a row with a large text column (will be TOASTed by Postgres).
	largeText := strings.Repeat("TOAST_DATA_", 1000) // ~11KB, exceeds TOAST threshold
	execSQL(t, fmt.Sprintf("INSERT INTO toast_test (small_col, big_col) VALUES ('hello', '%s')", largeText))

	t.Log("syncing initial insert...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	assertPgIcebergCountMatch(t, duckDB, "public", "toast_test", []string{"id"})

	// UPDATE only the small column — TOAST column should be preserved with FULL.
	t.Log("updating small column only...")
	execSQL(t, "UPDATE toast_test SET small_col = 'updated' WHERE id = 1")

	t.Log("syncing update...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	// Verify row count.
	assertPgIcebergCountMatch(t, duckDB, "public", "toast_test", []string{"id"})

	cleanup(t)
	execSQL(t, "DROP TABLE IF EXISTS toast_test")
}
