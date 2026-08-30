package ducklake

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/viggy28/streambed/internal/wal"
)

func newTestWriter(t *testing.T) *Writer {
	t.Helper()
	return newTestWriterWithCatalogStore(t, "sqlite")
}

func newTestWriterWithCatalogStore(t *testing.T, catalogStore string) *Writer {
	t.Helper()
	dir := t.TempDir()
	catalogPath := filepath.Join(dir, "catalog.sqlite")
	if catalogStore == "duckdb" {
		catalogPath = filepath.Join(dir, "catalog.ducklake")
	}
	w, err := NewWriter(context.Background(), Config{
		CatalogPath:  catalogPath,
		CatalogStore: catalogStore,
		DataPath:     filepath.Join(dir, "data") + "/",
	}, nil, 100, time.Second, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func TestWriterDuckDBCatalog(t *testing.T) {
	ctx := context.Background()
	w := newTestWriterWithCatalogStore(t, "duckdb")
	cols := []wal.Column{{Name: "id", OID: 23, IsKey: true}, {Name: "name", OID: 25}}
	if _, err := w.HandleEvent(ctx, wal.RowEvent{
		Schema:     "public",
		Table:      "duckdb_catalog_orders",
		Columns:    cols,
		KeyColumns: []int{0},
		Op:         wal.OpInsert,
		Values: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte("1")},
			{Name: "name", OID: 25, Value: []byte("alice")},
		},
		WALStartLSN: mustLSN(t, "0/10"),
	}); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	var got string
	if err := w.db.QueryRowContext(ctx, `SELECT name FROM "streambed"."public"."duckdb_catalog_orders" WHERE id = 1`).Scan(&got); err != nil {
		t.Fatalf("query duckdb-catalog ducklake table: %v", err)
	}
	if got != "alice" {
		t.Fatalf("got %q, want alice", got)
	}
}

func TestWriterInsertUpdateDeleteAndLSN(t *testing.T) {
	ctx := context.Background()
	w := newTestWriter(t)
	cols := []wal.Column{
		{Name: "id", OID: 23, IsKey: true},
		{Name: "name", OID: 25},
		{Name: "amount", OID: 20},
	}
	lsn1 := mustLSN(t, "0/10")
	for _, row := range [][]string{{"1", "alice", "100"}, {"2", "bob", "200"}} {
		if _, err := w.HandleEvent(ctx, wal.RowEvent{
			Schema:     "public",
			Table:      "orders",
			Columns:    cols,
			KeyColumns: []int{0},
			Op:         wal.OpInsert,
			Values: []wal.ColumnValue{
				{Name: "id", OID: 23, Value: []byte(row[0])},
				{Name: "name", OID: 25, Value: []byte(row[1])},
				{Name: "amount", OID: 20, Value: []byte(row[2])},
			},
			WALStartLSN: lsn1,
		}); err != nil {
			t.Fatalf("insert event: %v", err)
		}
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatalf("flush inserts: %v", err)
	}

	lsn2 := mustLSN(t, "0/20")
	if _, err := w.HandleEvent(ctx, wal.RowEvent{
		Schema:     "public",
		Table:      "orders",
		Columns:    cols,
		KeyColumns: []int{0},
		Op:         wal.OpUpdate,
		OldKey:     []wal.ColumnValue{{Name: "id", OID: 23, Value: []byte("1")}},
		Values: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte("1")},
			{Name: "name", OID: 25, Value: []byte("ann")},
			{Name: "amount", OID: 20, Value: []byte("125")},
		},
		WALStartLSN: lsn2,
	}); err != nil {
		t.Fatalf("update event: %v", err)
	}
	if _, err := w.HandleEvent(ctx, wal.RowEvent{
		Schema:      "public",
		Table:       "orders",
		Columns:     cols,
		KeyColumns:  []int{0},
		Op:          wal.OpDelete,
		OldKey:      []wal.ColumnValue{{Name: "id", OID: 23, Value: []byte("2")}},
		WALStartLSN: lsn2,
	}); err != nil {
		t.Fatalf("delete event: %v", err)
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatalf("flush mutations: %v", err)
	}

	var count int
	var name string
	var amount int64
	if err := w.db.QueryRowContext(ctx, `SELECT count(*), min(name), min(amount) FROM "streambed"."public"."orders"`).Scan(&count, &name, &amount); err != nil {
		t.Fatalf("query ducklake table: %v", err)
	}
	if count != 1 || name != "ann" || amount != 125 {
		t.Fatalf("got count=%d name=%q amount=%d, want 1 ann 125", count, name, amount)
	}
	lsn, found, err := w.GetTableFlushLSN(ctx, "public", "orders")
	if err != nil {
		t.Fatalf("GetTableFlushLSN: %v", err)
	}
	if !found || lsn != lsn2.String() {
		t.Fatalf("got lsn=%q found=%v, want %q true", lsn, found, lsn2.String())
	}
}

func TestWriterSchemaChangeAndTruncate(t *testing.T) {
	ctx := context.Background()
	w := newTestWriter(t)
	cols := []wal.Column{{Name: "id", OID: 23, IsKey: true}}
	if _, err := w.HandleEvent(ctx, wal.RowEvent{
		Schema:      "public",
		Table:       "events",
		Columns:     cols,
		KeyColumns:  []int{0},
		Op:          wal.OpInsert,
		Values:      []wal.ColumnValue{{Name: "id", OID: 23, Value: []byte("1")}},
		WALStartLSN: mustLSN(t, "0/30"),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	evolved := &wal.RelationMessage{
		Namespace:        "public",
		Name:             "events",
		Columns:          []wal.Column{{Name: "id", OID: 23, IsKey: true}, {Name: "payload", OID: 25}},
		KeyColumnIndexes: []int{0},
		Changes:          []wal.SchemaChange{{Type: wal.SchemaChangeAdd, Column: "payload", NewOID: 25}},
	}
	if err := w.HandleSchemaChange(ctx, evolved, nil); err != nil {
		t.Fatalf("schema change: %v", err)
	}
	if _, err := w.HandleEvent(ctx, wal.RowEvent{
		Schema:     "public",
		Table:      "events",
		Columns:    evolved.Columns,
		KeyColumns: []int{0},
		Op:         wal.OpInsert,
		Values: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte("2")},
			{Name: "payload", OID: 25, Value: []byte("hello")},
		},
		WALStartLSN: mustLSN(t, "0/40"),
	}); err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatalf("flush evolved: %v", err)
	}
	if err := w.Truncate(ctx, wal.RowEvent{Schema: "public", Table: "events", WALStartLSN: mustLSN(t, "0/50")}); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	var count int
	if err := w.db.QueryRowContext(ctx, `SELECT count(*) FROM "streambed"."public"."events"`).Scan(&count); err != nil {
		t.Fatalf("query events: %v", err)
	}
	if count != 0 {
		t.Fatalf("got %d rows after truncate, want 0", count)
	}
}

func TestWriterFlushAllBatchesTableLSNs(t *testing.T) {
	ctx := context.Background()
	w := newTestWriter(t)
	cols := []wal.Column{
		{Name: "id", OID: 23, IsKey: true},
		{Name: "name", OID: 25},
	}
	lsnA := mustLSN(t, "0/60")
	lsnB := mustLSN(t, "0/70")
	for _, tc := range []struct {
		table string
		lsn   pglogrepl.LSN
		id    string
		name  string
	}{
		{table: "accounts", lsn: lsnA, id: "1", name: "ann"},
		{table: "orders", lsn: lsnB, id: "2", name: "book"},
	} {
		if _, err := w.HandleEvent(ctx, wal.RowEvent{
			Schema:     "public",
			Table:      tc.table,
			Columns:    cols,
			KeyColumns: []int{0},
			Op:         wal.OpInsert,
			Values: []wal.ColumnValue{
				{Name: "id", OID: 23, Value: []byte(tc.id)},
				{Name: "name", OID: 25, Value: []byte(tc.name)},
			},
			WALStartLSN: tc.lsn,
		}); err != nil {
			t.Fatalf("insert %s: %v", tc.table, err)
		}
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatalf("batched flush: %v", err)
	}
	for _, tc := range []struct {
		table string
		lsn   pglogrepl.LSN
	}{
		{table: "accounts", lsn: lsnA},
		{table: "orders", lsn: lsnB},
	} {
		lsn, found, err := w.GetTableFlushLSN(ctx, "public", tc.table)
		if err != nil {
			t.Fatalf("GetTableFlushLSN(%s): %v", tc.table, err)
		}
		if !found || lsn != tc.lsn.String() {
			t.Fatalf("%s lsn=%q found=%v, want %q true", tc.table, lsn, found, tc.lsn.String())
		}
	}
}

func mustLSN(t *testing.T, s string) pglogrepl.LSN {
	t.Helper()
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatalf("parse LSN %q: %v", s, err)
	}
	return lsn
}
