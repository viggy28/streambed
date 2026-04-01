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
	lsn, err := s.GetFlushedLSN("test_slot")
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

	got, err := s.GetFlushedLSN("test_slot")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("expected LSN %s, got %s", want, got)
	}
}

func TestSetFlushedLSN_Update(t *testing.T) {
	s := tempStore(t)
	lsn1, _ := pglogrepl.ParseLSN("0/100")
	lsn2, _ := pglogrepl.ParseLSN("0/200")

	s.SetFlushedLSN("slot", lsn1)
	s.SetFlushedLSN("slot", lsn2)

	got, _ := s.GetFlushedLSN("slot")
	if got != lsn2 {
		t.Errorf("expected updated LSN %s, got %s", lsn2, got)
	}
}

func TestRegisterTable(t *testing.T) {
	s := tempStore(t)
	if err := s.RegisterTable("public", "orders", 5); err != nil {
		t.Fatal(err)
	}
	// Update column count
	if err := s.RegisterTable("public", "orders", 6); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateLastFlush(t *testing.T) {
	s := tempStore(t)
	s.RegisterTable("public", "orders", 5)
	if err := s.UpdateLastFlush("public", "orders"); err != nil {
		t.Fatal(err)
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
