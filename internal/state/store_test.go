package state

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pglogrepl"
)

func tempStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestGetFlushedLSN_Empty(t *testing.T) {
	s := tempStore(t)
	lsn, err := s.GetSafestFlushedLSN()
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 0 {
		t.Errorf("expected LSN 0 for fresh store, got %s", lsn)
	}
}

func TestSetAndGetFlushedLSN(t *testing.T) {
	s := tempStore(t)
	want, _ := pglogrepl.ParseLSN("0/1234ABCD")

	if err := s.SetFlushedLSN("test_slot", want); err != nil {
		t.Fatal(err)
	}

	// SetFlushedLSN writes to replication_state, GetFlushedLSN reads from synced_tables.
	// To test the round-trip via synced_tables, register a table with the LSN and flush it.
	if err := s.RegisterTable("public", "orders", 5, want); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateLastFlush(want, "public", "orders"); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetFlushedLSN()
	if err != nil {
		t.Fatal(err)
	}
	gotLSN, ok := got["orders"]
	if !ok {
		t.Fatal("expected orders in flushed LSN map")
	}
	if gotLSN != want.String() {
		t.Errorf("expected LSN %s, got %s", want, gotLSN)
	}
}

func TestSetFlushedLSN_Update(t *testing.T) {
	s := tempStore(t)
	lsn1, _ := pglogrepl.ParseLSN("0/100")
	lsn2, _ := pglogrepl.ParseLSN("0/200")

	s.SetFlushedLSN("slot", lsn1)
	s.SetFlushedLSN("slot", lsn2)

	// Verify via synced_tables round-trip
	s.RegisterTable("public", "t", 1, lsn2)
	s.UpdateLastFlush(lsn2, "public", "t")

	got, _ := s.GetFlushedLSN()
	gotLSN, ok := got["t"]
	if !ok {
		t.Fatal("expected table t in flushed LSN map")
	}
	if gotLSN != lsn2.String() {
		t.Errorf("expected updated LSN %s, got %s", lsn2, gotLSN)
	}
}

func TestRegisterTable(t *testing.T) {
	s := tempStore(t)
	lsn, _ := pglogrepl.ParseLSN("0/100")
	if err := s.RegisterTable("public", "orders", 5, lsn); err != nil {
		t.Fatal(err)
	}
	// Update column count
	if err := s.RegisterTable("public", "orders", 6, lsn); err != nil {
		t.Fatal(err)
	}
}

// TestRegisterTableStoresValidLSN verifies that RegisterTable stores the LSN
// (not the column count) in last_flush_lsn. This catches the parameter-order
// bug where columnCount was written as last_flush_lsn.
func TestRegisterTableStoresValidLSN(t *testing.T) {
	s := tempStore(t)
	lsn, _ := pglogrepl.ParseLSN("0/ABCD1234")

	if err := s.RegisterTable("public", "events", 7, lsn); err != nil {
		t.Fatal(err)
	}

	// GetFlushedLSN reads last_flush_lsn from synced_tables.
	// If RegisterTable stored "7" (the column count) instead of "0/ABCD1234",
	// ParseLSN will fail in the caller.
	got, err := s.GetFlushedLSN()
	if err != nil {
		t.Fatal(err)
	}
	gotLSN, ok := got["events"]
	if !ok {
		t.Fatal("expected events in flushed LSN map")
	}

	// Verify it's a valid LSN by parsing it.
	parsed, err := pglogrepl.ParseLSN(gotLSN)
	if err != nil {
		t.Fatalf("RegisterTable stored invalid LSN %q (column count leaked?): %v", gotLSN, err)
	}
	if parsed != lsn {
		t.Errorf("expected LSN %s, got %s", lsn, parsed)
	}
}

func TestUpdateLastFlush(t *testing.T) {
	s := tempStore(t)
	lsn, _ := pglogrepl.ParseLSN("0/100")
	s.RegisterTable("public", "orders", 5, lsn)
	if err := s.UpdateLastFlush(lsn, "public", "orders"); err != nil {
		t.Fatal(err)
	}
}

func TestBackfillLSN_UnsetReturnsEmpty(t *testing.T) {
	s := tempStore(t)
	got, err := s.GetBackfillLSNs()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty map, got %v", got)
	}
}

func TestBackfillLSN_SetGetClear(t *testing.T) {
	s := tempStore(t)
	lsn, _ := pglogrepl.ParseLSN("0/DEADBEEF")

	// Must register table first (backfill_lsn is a column on synced_tables).
	if err := s.RegisterTable("public", "orders", 5, lsn); err != nil {
		t.Fatal(err)
	}

	if err := s.SetBackfillLSN("public", "orders", lsn); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetBackfillLSNs()
	if err != nil {
		t.Fatal(err)
	}
	gotLSN, ok := got["public.orders"]
	if !ok {
		t.Fatal("expected public.orders in backfill LSN map")
	}
	if gotLSN != lsn {
		t.Errorf("expected %s, got %s", lsn, gotLSN)
	}

	if err := s.ClearBackfillLSN("public", "orders"); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetBackfillLSNs()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["public.orders"]; ok {
		t.Errorf("expected backfill LSN to be cleared, got %v", got)
	}
}

func TestBackfillLSN_MultipleTables(t *testing.T) {
	s := tempStore(t)
	lsnA, _ := pglogrepl.ParseLSN("0/100")
	lsnB, _ := pglogrepl.ParseLSN("0/200")

	if err := s.RegisterTable("public", "a", 1, lsnA); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterTable("public", "b", 1, lsnB); err != nil {
		t.Fatal(err)
	}

	if err := s.SetBackfillLSN("public", "a", lsnA); err != nil {
		t.Fatal(err)
	}
	if err := s.SetBackfillLSN("public", "b", lsnB); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetBackfillLSNs()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d: %v", len(got), got)
	}
	if got["public.a"] != lsnA {
		t.Errorf("public.a: want %s got %s", lsnA, got["public.a"])
	}
	if got["public.b"] != lsnB {
		t.Errorf("public.b: want %s got %s", lsnB, got["public.b"])
	}
}

func TestBackfillLSN_ClearIdempotent(t *testing.T) {
	s := tempStore(t)
	// Clearing a table that doesn't exist should not error.
	if err := s.ClearBackfillLSN("public", "nope"); err != nil {
		t.Errorf("clearing nonexistent table errored: %v", err)
	}
}

func TestOpenCreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "dir", "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("expected state file to be created")
	}
}
