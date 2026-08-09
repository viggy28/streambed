//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

func TestReplicaIdentityFullCDCWithoutPrimaryKey(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	execSQL(t, "DROP TABLE IF EXISTS replica_identity_full_cdc_test")
	t.Cleanup(func() {
		cleanup(t)
		execSQL(t, "DROP TABLE IF EXISTS replica_identity_full_cdc_test")
	})

	execSQL(t, `CREATE TABLE replica_identity_full_cdc_test (
		logical_id INT NOT NULL,
		code TEXT NOT NULL,
		payload TEXT NOT NULL,
		qty INT NOT NULL
	)`)
	execSQL(t, "ALTER TABLE replica_identity_full_cdc_test REPLICA IDENTITY FULL")

	createSlotAndPublication(t)
	statePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	execSQL(t, `INSERT INTO replica_identity_full_cdc_test (logical_id, code, payload, qty) VALUES
		(1, 'A', 'alpha', 10),
		(2, 'B', 'bravo', 20),
		(3, 'C', 'charlie', 30),
		(4, 'D', 'delta', 40)`)
	runSync(t, ctx, 12*time.Second, statePath)
	assertPgIcebergMatch(t, duckDB, "public", "replica_identity_full_cdc_test",
		[]string{"logical_id"}, []string{"code", "payload", "qty"})

	// FULL identity should provide enough old-row information to delete the
	// previous image even when the table has no primary key and multiple
	// non-key-looking columns change.
	execSQL(t, "UPDATE replica_identity_full_cdc_test SET code = 'A2', payload = 'alpha-updated', qty = 11 WHERE logical_id = 1")
	execSQL(t, "UPDATE replica_identity_full_cdc_test SET logical_id = 20, code = 'B2', payload = 'bravo-moved', qty = 21 WHERE logical_id = 2")
	execSQL(t, "DELETE FROM replica_identity_full_cdc_test WHERE logical_id = 3")
	runSync(t, ctx, 12*time.Second, statePath)

	assertPgIcebergMatch(t, duckDB, "public", "replica_identity_full_cdc_test",
		[]string{"logical_id"}, []string{"code", "payload", "qty"})
}

func TestReplicaIdentityUsingIndexCDC(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	execSQL(t, "DROP TABLE IF EXISTS replica_identity_using_index_cdc_test")
	t.Cleanup(func() {
		cleanup(t)
		execSQL(t, "DROP TABLE IF EXISTS replica_identity_using_index_cdc_test")
	})

	execSQL(t, `CREATE TABLE replica_identity_using_index_cdc_test (
		id INT PRIMARY KEY,
		sku TEXT NOT NULL,
		payload TEXT NOT NULL,
		qty INT NOT NULL
	)`)
	execSQL(t, "CREATE UNIQUE INDEX replica_identity_using_index_cdc_test_sku_idx ON replica_identity_using_index_cdc_test (sku)")
	execSQL(t, "ALTER TABLE replica_identity_using_index_cdc_test REPLICA IDENTITY USING INDEX replica_identity_using_index_cdc_test_sku_idx")

	createSlotAndPublication(t)
	statePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	execSQL(t, `INSERT INTO replica_identity_using_index_cdc_test (id, sku, payload, qty) VALUES
		(1, 'SKU-1', 'one', 10),
		(2, 'SKU-2', 'two', 20),
		(3, 'SKU-3', 'three', 30),
		(4, 'SKU-4', 'four', 40)`)
	runSync(t, ctx, 12*time.Second, statePath)
	assertPgIcebergMatch(t, duckDB, "public", "replica_identity_using_index_cdc_test",
		[]string{"sku"}, []string{"id", "payload", "qty"})

	// The replica identity is the unique sku index, not the primary key. This
	// sequence catches implementations that continue to delete by id: id changes
	// with stable sku, sku changes with stable id, and a final delete by sku.
	execSQL(t, "UPDATE replica_identity_using_index_cdc_test SET payload = 'one-updated', qty = 11 WHERE sku = 'SKU-1'")
	execSQL(t, "UPDATE replica_identity_using_index_cdc_test SET id = 20, payload = 'two-id-moved', qty = 21 WHERE sku = 'SKU-2'")
	execSQL(t, "UPDATE replica_identity_using_index_cdc_test SET sku = 'SKU-3B', payload = 'three-sku-moved', qty = 31 WHERE sku = 'SKU-3'")
	execSQL(t, "DELETE FROM replica_identity_using_index_cdc_test WHERE sku = 'SKU-4'")
	runSync(t, ctx, 12*time.Second, statePath)

	assertPgIcebergMatch(t, duckDB, "public", "replica_identity_using_index_cdc_test",
		[]string{"sku"}, []string{"id", "payload", "qty"})
}
