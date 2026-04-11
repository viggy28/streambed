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

func TestGetLastFlushLSNs_Empty(t *testing.T) {
	s := tempStore(t)
	got, err := s.GetLastFlushLSNs()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty map for fresh store, got %v", got)
	}
}

// RegisterTable must not populate last_flush_lsn — that column is owned
// by UpdateLastFlush and lying about it would corrupt the dedup filter
// for a registered-but-never-flushed table.
func TestRegisterTableDoesNotSeedLastFlushLSN(t *testing.T) {
	s := tempStore(t)
	if err := s.RegisterTable("public", "events", 5); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetLastFlushLSNs()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["public.events"]; ok {
		t.Errorf("RegisterTable should not populate last_flush_lsn, got %v", got)
	}
}

func TestRegisterTableIdempotentUpdatesColumnCount(t *testing.T) {
	s := tempStore(t)
	if err := s.RegisterTable("public", "orders", 5); err != nil {
		t.Fatal(err)
	}
	// Second call with a new column count must succeed (ON CONFLICT path).
	if err := s.RegisterTable("public", "orders", 6); err != nil {
		t.Fatal(err)
	}
}

// GetLastFlushLSNs must return keys as "schema.table" so two tables with
// the same bare name in different schemas don't collide. This guards the
// dedup filter against the latent multi-schema correctness bug.
func TestGetLastFlushLSNs_SchemaQualified(t *testing.T) {
	s := tempStore(t)
	lsnA, _ := pglogrepl.ParseLSN("0/AAAA")
	lsnB, _ := pglogrepl.ParseLSN("0/BBBB")

	if err := s.RegisterTable("public", "events", 3); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterTable("audit", "events", 3); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateLastFlush(lsnA, "public", "events"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateLastFlush(lsnB, "audit", "events"); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetLastFlushLSNs()
	if err != nil {
		t.Fatal(err)
	}
	if got["public.events"] != lsnA {
		t.Errorf("public.events: want %s got %s", lsnA, got["public.events"])
	}
	if got["audit.events"] != lsnB {
		t.Errorf("audit.events: want %s got %s", lsnB, got["audit.events"])
	}
}

func TestUpdateLastFlush(t *testing.T) {
	s := tempStore(t)
	lsn, _ := pglogrepl.ParseLSN("0/100")
	if err := s.RegisterTable("public", "orders", 5); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateLastFlush(lsn, "public", "orders"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetLastFlushLSNs()
	if err != nil {
		t.Fatal(err)
	}
	if got["public.orders"] != lsn {
		t.Errorf("want %s, got %s", lsn, got["public.orders"])
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
	if err := s.RegisterTable("public", "orders", 5); err != nil {
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

	if err := s.RegisterTable("public", "a", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterTable("public", "b", 1); err != nil {
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
