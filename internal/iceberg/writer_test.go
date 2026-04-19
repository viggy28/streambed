package iceberg

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	pqbuilder "github.com/viggy28/streambed/internal/parquet"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/storage"
	"github.com/viggy28/streambed/internal/wal"
)

// TestComputePendingMinLSN_MultipleBuffers verifies that ComputePendingMinLSN
// returns the smallest FirstLSN across all non-empty buffers.
func TestComputePendingMinLSN_MultipleBuffers(t *testing.T) {
	w := &Writer{
		buffers: map[string]*tableBuffer{
			"public.orders": {
				FirstLSN: pglogrepl.LSN(1000),
			},
			"public.users": {
				FirstLSN: pglogrepl.LSN(500),
			},
			"public.products": {
				FirstLSN: pglogrepl.LSN(2000),
			},
		},
	}

	got := w.ComputePendingMinLSN()
	want := pglogrepl.LSN(500)
	if got != want {
		t.Errorf("ComputePendingMinLSN = %s, want %s", got, want)
	}
}

// TestComputePendingMinLSN_EmptyBuffers verifies that ComputePendingMinLSN
// returns 0 when all buffers are empty (FirstLSN == 0).
func TestComputePendingMinLSN_EmptyBuffers(t *testing.T) {
	w := &Writer{
		buffers: map[string]*tableBuffer{
			"public.orders":   {FirstLSN: 0},
			"public.users":    {FirstLSN: 0},
			"public.products": {FirstLSN: 0},
		},
	}

	got := w.ComputePendingMinLSN()
	if got != 0 {
		t.Errorf("ComputePendingMinLSN = %s, want 0 (all empty)", got)
	}
}

// TestComputePendingMinLSN_NoBuffers verifies that ComputePendingMinLSN
// returns 0 when there are no buffers at all.
func TestComputePendingMinLSN_NoBuffers(t *testing.T) {
	w := &Writer{
		buffers: map[string]*tableBuffer{},
	}

	got := w.ComputePendingMinLSN()
	if got != 0 {
		t.Errorf("ComputePendingMinLSN = %s, want 0 (no buffers)", got)
	}
}

// TestComputePendingMinLSN_AfterPartialFlush verifies that after flushing one
// table (setting its FirstLSN to 0), the minimum updates correctly to the
// next non-empty buffer.
func TestComputePendingMinLSN_AfterPartialFlush(t *testing.T) {
	w := &Writer{
		buffers: map[string]*tableBuffer{
			"public.orders": {
				FirstLSN: pglogrepl.LSN(100),
			},
			"public.users": {
				FirstLSN: pglogrepl.LSN(200),
			},
			"public.products": {
				FirstLSN: pglogrepl.LSN(300),
			},
		},
	}

	// Before flush: min should be 100.
	got := w.ComputePendingMinLSN()
	if got != pglogrepl.LSN(100) {
		t.Fatalf("before flush: ComputePendingMinLSN = %s, want 0/64", got)
	}

	// Simulate flushing "orders" (the smallest).
	w.buffers["public.orders"].FirstLSN = 0

	// After flush: min should now be 200 (users).
	got = w.ComputePendingMinLSN()
	if got != pglogrepl.LSN(200) {
		t.Errorf("after partial flush: ComputePendingMinLSN = %s, want 0/C8", got)
	}

	// Flush users too.
	w.buffers["public.users"].FirstLSN = 0

	got = w.ComputePendingMinLSN()
	if got != pglogrepl.LSN(300) {
		t.Errorf("after second flush: ComputePendingMinLSN = %s, want 0/12C", got)
	}

	// Flush all.
	w.buffers["public.products"].FirstLSN = 0

	got = w.ComputePendingMinLSN()
	if got != 0 {
		t.Errorf("after all flushed: ComputePendingMinLSN = %s, want 0", got)
	}
}

// TestComputePendingMinLSN_MixedEmptyNonEmpty verifies correct behavior when
// some buffers are empty and some are not.
func TestComputePendingMinLSN_MixedEmptyNonEmpty(t *testing.T) {
	w := &Writer{
		buffers: map[string]*tableBuffer{
			"public.orders":   {FirstLSN: 0},                     // empty
			"public.users":    {FirstLSN: pglogrepl.LSN(999)},    // non-empty
			"public.products": {FirstLSN: 0},                     // empty
			"public.logs":     {FirstLSN: pglogrepl.LSN(888)},    // non-empty
		},
	}

	got := w.ComputePendingMinLSN()
	if got != pglogrepl.LSN(888) {
		t.Errorf("mixed: ComputePendingMinLSN = %s, want 0/378", got)
	}
}

// ---------------------------------------------------------------------------
// Test helpers for Writer tests
// ---------------------------------------------------------------------------

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func testStateStore(t *testing.T) *state.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := state.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open state store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// testWriter creates a Writer backed by MemS3Client and a temp state store.
func testWriter(t *testing.T) (*Writer, *storage.MemS3Client) {
	t.Helper()
	mem := storage.NewMemS3Client("test-bucket")
	catalog := NewCatalog(mem, "test-bucket", "test-prefix")
	store := testStateStore(t)
	w := NewWriter(catalog, mem, store, "test_slot", 10000, 2*time.Second, testLogger())
	return w, mem
}

func testColumns() []wal.Column {
	return []wal.Column{
		{Name: "id", OID: 23, IsKey: true},
		{Name: "name", OID: 25},
	}
}

func insertEvent(schema, table string, lsn pglogrepl.LSN, id, name string) wal.RowEvent {
	return wal.RowEvent{
		Schema:  schema,
		Table:   table,
		Columns: testColumns(),
		KeyColumns: []int{0},
		Op:      wal.OpInsert,
		Values: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte(id)},
			{Name: "name", OID: 25, Value: []byte(name)},
		},
		WALStartLSN: lsn,
	}
}

func deleteEvent(schema, table string, lsn pglogrepl.LSN, id string) wal.RowEvent {
	return wal.RowEvent{
		Schema:     schema,
		Table:      table,
		Columns:    testColumns(),
		KeyColumns: []int{0},
		Op:         wal.OpDelete,
		OldKey: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte(id)},
		},
		WALStartLSN: lsn,
	}
}

func updateEvent(schema, table string, lsn pglogrepl.LSN, oldID, newID, newName string) wal.RowEvent {
	return wal.RowEvent{
		Schema:     schema,
		Table:      table,
		Columns:    testColumns(),
		KeyColumns: []int{0},
		Op:         wal.OpUpdate,
		Values: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte(newID)},
			{Name: "name", OID: 25, Value: []byte(newName)},
		},
		OldKey: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte(oldID)},
		},
		WALStartLSN: lsn,
	}
}

// ---------------------------------------------------------------------------
// Buffer tests
// ---------------------------------------------------------------------------

// TestBufferInsert verifies that buffer() correctly appends INSERT rows,
// tracks LSN transitions, and returns true on the empty→non-empty transition.
func TestBufferInsert(t *testing.T) {
	w, _ := testWriter(t)

	// First insert should transition empty → non-empty.
	transitioned := w.buffer(insertEvent("public", "t1", 100, "1", "alice"))
	if !transitioned {
		t.Error("first buffer should report empty→non-empty transition")
	}

	buf := w.buffers["public.t1"]
	if buf == nil {
		t.Fatal("buffer not created")
	}
	if len(buf.Rows) != 1 {
		t.Errorf("rows: got %d, want 1", len(buf.Rows))
	}
	if len(buf.Deletes) != 0 {
		t.Errorf("deletes: got %d, want 0", len(buf.Deletes))
	}
	if buf.FirstLSN != 100 {
		t.Errorf("FirstLSN: got %s, want 0/64", buf.FirstLSN)
	}
	if buf.LastLSN != 100 {
		t.Errorf("LastLSN: got %s, want 0/64", buf.LastLSN)
	}

	// Second insert should NOT re-transition.
	transitioned = w.buffer(insertEvent("public", "t1", 200, "2", "bob"))
	if transitioned {
		t.Error("second buffer should NOT report transition")
	}
	if len(buf.Rows) != 2 {
		t.Errorf("rows: got %d, want 2", len(buf.Rows))
	}
	if buf.FirstLSN != 100 {
		t.Errorf("FirstLSN should stay at first event: got %s", buf.FirstLSN)
	}
	if buf.LastLSN != 200 {
		t.Errorf("LastLSN should advance: got %s", buf.LastLSN)
	}
}

// TestBufferDelete verifies that DELETE events add to Deletes and deletedKeys,
// and that subsequent INSERTs on the same key clear the deletedKey entry.
func TestBufferDelete(t *testing.T) {
	w, _ := testWriter(t)

	// Insert then delete.
	w.buffer(insertEvent("public", "t1", 100, "1", "alice"))
	w.buffer(deleteEvent("public", "t1", 200, "1"))

	buf := w.buffers["public.t1"]
	if len(buf.Rows) != 1 {
		t.Errorf("rows: got %d, want 1 (INSERT still in buffer)", len(buf.Rows))
	}
	if len(buf.Deletes) != 1 {
		t.Errorf("deletes: got %d, want 1", len(buf.Deletes))
	}
	if len(buf.deletedKeys) != 1 {
		t.Errorf("deletedKeys: want 1 entry, got %v", buf.deletedKeys)
	}

	// INSERT same key again — should clear the deletedKey.
	w.buffer(insertEvent("public", "t1", 300, "1", "alice_v2"))
	if len(buf.deletedKeys) != 0 {
		t.Errorf("deletedKeys should be cleared after re-insert, got %d", len(buf.deletedKeys))
	}
}

// TestBufferUpdate verifies that UPDATE appends both a data row and a delete key.
func TestBufferUpdate(t *testing.T) {
	w, _ := testWriter(t)

	w.buffer(updateEvent("public", "t1", 100, "1", "1", "alice_updated"))

	buf := w.buffers["public.t1"]
	if len(buf.Rows) != 1 {
		t.Errorf("rows: got %d, want 1", len(buf.Rows))
	}
	if len(buf.Deletes) != 1 {
		t.Errorf("deletes: got %d, want 1", len(buf.Deletes))
	}
	// UPDATE should NOT set deletedKeys (only standalone DELETE does).
	if len(buf.deletedKeys) > 0 {
		t.Errorf("deletedKeys should be empty for UPDATE, got %d", len(buf.deletedKeys))
	}
}

// ---------------------------------------------------------------------------
// Flush tests
// ---------------------------------------------------------------------------

// TestFlushAppendOnly verifies the append-only flush path: creates Iceberg
// table, writes Parquet to S3, commits snapshot, and clears buffer.
func TestFlushAppendOnly(t *testing.T) {
	w, mem := testWriter(t)
	ctx := context.Background()

	// Buffer 3 inserts.
	w.buffer(insertEvent("public", "t1", 100, "1", "alice"))
	w.buffer(insertEvent("public", "t1", 200, "2", "bob"))
	w.buffer(insertEvent("public", "t1", 300, "3", "carol"))

	// Flush.
	if err := w.flush(ctx, "public.t1"); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Verify buffer is cleared.
	buf := w.buffers["public.t1"]
	if len(buf.Rows) != 0 {
		t.Errorf("rows after flush: got %d, want 0", len(buf.Rows))
	}
	if buf.FirstLSN != 0 {
		t.Errorf("FirstLSN after flush: got %s, want 0", buf.FirstLSN)
	}

	// Verify Iceberg table exists.
	exists, err := w.catalog.TableExists(ctx, "public", "t1")
	if err != nil {
		t.Fatalf("TableExists: %v", err)
	}
	if !exists {
		t.Error("Iceberg table should exist after flush")
	}

	// Verify Parquet file on S3.
	keys, err := mem.ListPrefix(ctx, "test-prefix/public/t1/data/")
	if err != nil {
		t.Fatalf("ListPrefix: %v", err)
	}
	if len(keys) != 1 {
		t.Errorf("expected 1 Parquet file, got %d", len(keys))
	}

	// Verify we can read the data back.
	paths, err := w.catalog.GetDataFilePaths(ctx, "public", "t1")
	if err != nil {
		t.Fatalf("GetDataFilePaths: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("expected 1 data file path, got %d", len(paths))
	}

	// Read parquet and verify row count.
	cols := []pqbuilder.ColumnDef{
		{Name: "id", OID: 23, FieldID: 1},
		{Name: "name", OID: 25, FieldID: 2},
	}
	s3Key := s3KeyFromURI(paths[0])
	data, err := mem.GetObject(ctx, s3Key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	rows, err := pqbuilder.ReadRows(data, cols)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("expected 3 rows in Parquet, got %d", len(rows))
	}

	// Verify snapshot LSN.
	lsn, found, err := w.catalog.GetSnapshotFlushLSN(ctx, "public", "t1")
	if err != nil {
		t.Fatalf("GetSnapshotFlushLSN: %v", err)
	}
	if !found {
		t.Error("expected to find flush LSN")
	}
	if lsn != "0/12C" { // LSN 300 in hex
		t.Errorf("flush LSN: got %s, want 0/12C", lsn)
	}
}

// TestFlushCOW verifies the copy-on-write flush path: existing data is read,
// deleted rows are filtered, new rows are deduped, and the result is written.
func TestFlushCOW(t *testing.T) {
	w, mem := testWriter(t)
	ctx := context.Background()

	// Phase 1: Insert 3 rows and flush (append-only).
	w.buffer(insertEvent("public", "t1", 100, "1", "alice"))
	w.buffer(insertEvent("public", "t1", 200, "2", "bob"))
	w.buffer(insertEvent("public", "t1", 300, "3", "carol"))
	if err := w.flush(ctx, "public.t1"); err != nil {
		t.Fatalf("initial flush: %v", err)
	}

	// Phase 2: UPDATE id=2 and DELETE id=1, then flush (COW).
	w.buffer(updateEvent("public", "t1", 400, "2", "2", "bob_updated"))
	w.buffer(deleteEvent("public", "t1", 500, "1"))
	if err := w.flush(ctx, "public.t1"); err != nil {
		t.Fatalf("COW flush: %v", err)
	}

	// Read back and verify: should have 2 rows (carol, bob_updated).
	// id=1 deleted, id=2 updated, id=3 unchanged.
	paths, err := w.catalog.GetDataFilePaths(ctx, "public", "t1")
	if err != nil {
		t.Fatalf("GetDataFilePaths: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("expected 1 data file (COW replaces), got %d", len(paths))
	}

	cols := []pqbuilder.ColumnDef{
		{Name: "id", OID: 23, FieldID: 1},
		{Name: "name", OID: 25, FieldID: 2},
	}
	s3Key := s3KeyFromURI(paths[0])
	data, err := mem.GetObject(ctx, s3Key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	rows, err := pqbuilder.ReadRows(data, cols)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after COW, got %d", len(rows))
	}

	// Verify content: should have carol and bob_updated (order may vary).
	names := map[string]bool{}
	for _, row := range rows {
		names[string(row[1].Data)] = true
	}
	if !names["carol"] {
		t.Error("expected carol to survive COW")
	}
	if !names["bob_updated"] {
		t.Error("expected bob_updated after UPDATE")
	}
	if names["alice"] {
		t.Error("alice should have been deleted")
	}
}

// TestFlushOnS3Failure verifies that when PutObject fails, the error
// propagates and the buffer is NOT cleared (data is preserved for retry).
func TestFlushOnS3Failure(t *testing.T) {
	mem := storage.NewMemS3Client("test-bucket")
	fault := storage.NewFaultS3Client(mem)
	catalog := NewCatalog(fault, "test-bucket", "test-prefix")
	store := testStateStore(t)
	w := NewWriter(catalog, fault, store, "test_slot", 10000, 2*time.Second, testLogger())
	ctx := context.Background()

	// Buffer some rows.
	w.buffer(insertEvent("public", "t1", 100, "1", "alice"))
	w.buffer(insertEvent("public", "t1", 200, "2", "bob"))

	// Fail on Parquet file writes.
	fault.FailOnKeyContaining(".parquet")

	err := w.flush(ctx, "public.t1")
	if err == nil {
		t.Fatal("expected error when S3 write fails")
	}

	// Buffer should NOT be cleared — data is preserved for retry.
	buf := w.buffers["public.t1"]
	if len(buf.Rows) != 2 {
		t.Errorf("rows should be preserved after failed flush: got %d, want 2", len(buf.Rows))
	}
	if buf.FirstLSN != 100 {
		t.Errorf("FirstLSN should be preserved: got %s, want 0/64", buf.FirstLSN)
	}

	// Clear fault and retry.
	fault.Reset()
	err = w.flush(ctx, "public.t1")
	if err != nil {
		t.Fatalf("retry flush should succeed: %v", err)
	}
	if len(buf.Rows) != 0 {
		t.Errorf("rows after successful retry: got %d, want 0", len(buf.Rows))
	}
}

// ---------------------------------------------------------------------------
// Truncate tests
// ---------------------------------------------------------------------------

// TestTruncate verifies that Truncate discards buffered rows and commits an
// empty snapshot for existing tables, and is a no-op for non-existent tables.
func TestTruncate(t *testing.T) {
	t.Run("truncate existing table with buffered rows", func(t *testing.T) {
		w, _ := testWriter(t)
		ctx := context.Background()

		// Insert and flush to create the Iceberg table.
		w.buffer(insertEvent("public", "t1", 100, "1", "alice"))
		if err := w.flush(ctx, "public.t1"); err != nil {
			t.Fatalf("initial flush: %v", err)
		}

		// Buffer more rows.
		w.buffer(insertEvent("public", "t1", 200, "2", "bob"))

		// Truncate.
		err := w.Truncate(ctx, wal.RowEvent{
			Schema:      "public",
			Table:       "t1",
			Op:          wal.OpTruncate,
			WALStartLSN: 300,
		})
		if err != nil {
			t.Fatalf("truncate: %v", err)
		}

		// Buffer should be cleared.
		buf := w.buffers["public.t1"]
		if len(buf.Rows) != 0 {
			t.Errorf("rows after truncate: got %d, want 0", len(buf.Rows))
		}

		// Iceberg should be in empty state.
		lsn, found, err := w.catalog.GetSnapshotFlushLSN(ctx, "public", "t1")
		if err != nil {
			t.Fatalf("GetSnapshotFlushLSN: %v", err)
		}
		if !found {
			t.Error("expected flush LSN to be set after truncate")
		}
		if lsn != "0/12C" { // LSN 300 in hex
			t.Errorf("flush LSN: got %s, want 0/12C", lsn)
		}
	})

	t.Run("truncate non-existent table is no-op", func(t *testing.T) {
		w, _ := testWriter(t)
		ctx := context.Background()

		err := w.Truncate(ctx, wal.RowEvent{
			Schema:      "public",
			Table:       "nonexistent",
			Op:          wal.OpTruncate,
			WALStartLSN: 100,
		})
		if err != nil {
			t.Fatalf("truncate non-existent should not error: %v", err)
		}
	})

	t.Run("insert after truncate works", func(t *testing.T) {
		w, _ := testWriter(t)
		ctx := context.Background()

		// Create table.
		w.buffer(insertEvent("public", "t1", 100, "1", "alice"))
		if err := w.flush(ctx, "public.t1"); err != nil {
			t.Fatalf("initial flush: %v", err)
		}

		// Truncate.
		if err := w.Truncate(ctx, wal.RowEvent{
			Schema: "public", Table: "t1", Op: wal.OpTruncate, WALStartLSN: 200,
		}); err != nil {
			t.Fatalf("truncate: %v", err)
		}

		// Insert new data.
		w.buffer(insertEvent("public", "t1", 300, "99", "new_row"))
		if err := w.flush(ctx, "public.t1"); err != nil {
			t.Fatalf("flush after truncate: %v", err)
		}

		// Verify table exists and has the new data.
		exists, err := w.catalog.TableExists(ctx, "public", "t1")
		if err != nil {
			t.Fatalf("TableExists: %v", err)
		}
		if !exists {
			t.Error("table should still exist after truncate+insert")
		}
	})
}

// ---------------------------------------------------------------------------
// HandleSchemaChange tests
// ---------------------------------------------------------------------------

// TestHandleSchemaChange verifies that HandleSchemaChange flushes old data,
// evolves the Iceberg schema, and updates the buffer column list.
func TestHandleSchemaChange(t *testing.T) {
	w, _ := testWriter(t)
	ctx := context.Background()

	// Insert and flush to create the Iceberg table.
	w.buffer(insertEvent("public", "t1", 100, "1", "alice"))
	if err := w.flush(ctx, "public.t1"); err != nil {
		t.Fatalf("initial flush: %v", err)
	}

	// Buffer more rows with old schema.
	w.buffer(insertEvent("public", "t1", 200, "2", "bob"))

	// Schema change: ADD "email" column.
	rel := &wal.RelationMessage{
		RelationID: 1,
		Namespace:  "public",
		Name:       "t1",
		Columns: []wal.Column{
			{Name: "id", OID: 23, IsKey: true},
			{Name: "name", OID: 25},
			{Name: "email", OID: 25},
		},
		KeyColumnIndexes: []int{0},
		Changes: []wal.SchemaChange{
			{Type: wal.SchemaChangeAdd, Column: "email", NewOID: 25},
		},
	}

	err := w.HandleSchemaChange(ctx, rel, nil)
	if err != nil {
		t.Fatalf("HandleSchemaChange: %v", err)
	}

	// Buffer should have updated columns (3 columns now).
	buf := w.buffers["public.t1"]
	if len(buf.Columns) != 3 {
		t.Errorf("columns after evolution: got %d, want 3", len(buf.Columns))
	}

	// The pre-evolution data (row "bob") should have been flushed.
	// Buffer should be empty of old rows.
	if len(buf.Rows) != 0 {
		t.Errorf("rows after schema change: got %d, want 0 (pre-evolution flush)", len(buf.Rows))
	}

	// Field IDs should be populated.
	if buf.fieldIDs == nil {
		t.Error("fieldIDs should be populated after schema evolution")
	}
	if _, ok := buf.fieldIDs["email"]; !ok {
		t.Error("fieldIDs should include new 'email' column")
	}
}

// ---------------------------------------------------------------------------
// reconcileWithIceberg tests
// ---------------------------------------------------------------------------

// TestReconcileWithIceberg verifies schema drift detection between WAL
// columns and the Iceberg schema.
func TestReconcileWithIceberg(t *testing.T) {
	w, _ := testWriter(t)
	ctx := context.Background()

	// Create Iceberg table with [id, name].
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	if _, err := w.catalog.CreateTable(ctx, "public", "t1", cols); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	t.Run("no drift", func(t *testing.T) {
		buf := &tableBuffer{
			Schema: "public",
			Table:  "t1",
			Columns: []wal.Column{
				{Name: "id", OID: 23},
				{Name: "name", OID: 25},
			},
		}
		fids, changes, err := w.reconcileWithIceberg(ctx, buf)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(changes) != 0 {
			t.Errorf("expected 0 changes, got %d", len(changes))
		}
		if fids == nil || len(fids) != 2 {
			t.Errorf("expected 2 field IDs, got %v", fids)
		}
	})

	t.Run("ADD column", func(t *testing.T) {
		buf := &tableBuffer{
			Schema: "public",
			Table:  "t1",
			Columns: []wal.Column{
				{Name: "id", OID: 23},
				{Name: "name", OID: 25},
				{Name: "email", OID: 25}, // new column in WAL but not in Iceberg
			},
		}
		_, changes, err := w.reconcileWithIceberg(ctx, buf)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(changes) != 1 {
			t.Fatalf("expected 1 change, got %d", len(changes))
		}
		if changes[0].Type != wal.SchemaChangeAdd || changes[0].Column != "email" {
			t.Errorf("unexpected change: %+v", changes[0])
		}
	})

	t.Run("DROP column", func(t *testing.T) {
		buf := &tableBuffer{
			Schema: "public",
			Table:  "t1",
			Columns: []wal.Column{
				{Name: "id", OID: 23},
				// "name" is in Iceberg but not in WAL → DROP
			},
		}
		_, changes, err := w.reconcileWithIceberg(ctx, buf)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(changes) != 1 {
			t.Fatalf("expected 1 change, got %d", len(changes))
		}
		if changes[0].Type != wal.SchemaChangeDrop || changes[0].Column != "name" {
			t.Errorf("unexpected change: %+v", changes[0])
		}
	})

	t.Run("TYPE_CHANGE", func(t *testing.T) {
		buf := &tableBuffer{
			Schema: "public",
			Table:  "t1",
			Columns: []wal.Column{
				{Name: "id", OID: 20}, // was int4 (23→int), now int8 (20→long)
				{Name: "name", OID: 25},
			},
		}
		_, changes, err := w.reconcileWithIceberg(ctx, buf)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(changes) != 1 {
			t.Fatalf("expected 1 change, got %d", len(changes))
		}
		if changes[0].Type != wal.SchemaChangeTypeChange || changes[0].Column != "id" {
			t.Errorf("unexpected change: %+v", changes[0])
		}
	})
}

// ---------------------------------------------------------------------------
// HandleEvent integration tests
// ---------------------------------------------------------------------------

// TestHandleEventTruncate verifies that HandleEvent correctly routes
// TRUNCATE operations to Truncate().
func TestHandleEventTruncate(t *testing.T) {
	w, _ := testWriter(t)
	ctx := context.Background()

	// Create table first.
	w.buffer(insertEvent("public", "t1", 100, "1", "alice"))
	if err := w.flush(ctx, "public.t1"); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Send truncate via HandleEvent.
	transitioned, err := w.HandleEvent(ctx, wal.RowEvent{
		Schema:      "public",
		Table:       "t1",
		Op:          wal.OpTruncate,
		WALStartLSN: 200,
	})
	if err != nil {
		t.Fatalf("HandleEvent(TRUNCATE): %v", err)
	}
	if transitioned {
		t.Error("TRUNCATE should not report transition")
	}
}

// TestHandleEventThresholdFlush verifies that HandleEvent auto-flushes
// when the buffer reaches the flush row threshold.
func TestHandleEventThresholdFlush(t *testing.T) {
	mem := storage.NewMemS3Client("test-bucket")
	catalog := NewCatalog(mem, "test-bucket", "test-prefix")
	store := testStateStore(t)
	// Set threshold to 3 rows.
	w := NewWriter(catalog, mem, store, "test_slot", 3, 2*time.Second, testLogger())
	ctx := context.Background()

	// Insert 3 rows — should trigger auto-flush on the 3rd.
	for i := 1; i <= 3; i++ {
		if _, err := w.HandleEvent(ctx, insertEvent("public", "t1", pglogrepl.LSN(i*100), strconv.Itoa(i), "name")); err != nil {
			t.Fatalf("HandleEvent %d: %v", i, err)
		}
	}

	// Buffer should be empty (auto-flushed).
	buf := w.buffers["public.t1"]
	if len(buf.Rows) != 0 {
		t.Errorf("rows after threshold flush: got %d, want 0", len(buf.Rows))
	}

	// Table should exist on S3.
	exists, err := catalog.TableExists(ctx, "public", "t1")
	if err != nil {
		t.Fatalf("TableExists: %v", err)
	}
	if !exists {
		t.Error("table should exist after threshold flush")
	}
}
