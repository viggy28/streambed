package server

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/viggy28/streambed/internal/ducklake"
	"github.com/viggy28/streambed/internal/wal"
)

func TestDuckLakeServerRegistersViews(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	catalogPath := filepath.Join(dir, "catalog.sqlite")
	dataPath := filepath.Join(dir, "data") + "/"
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	writer, err := ducklake.NewWriter(ctx, ducklake.Config{
		CatalogPath: catalogPath,
		DataPath:    dataPath,
	}, nil, 10, time.Second, logger)
	if err != nil {
		t.Fatalf("ducklake writer: %v", err)
	}
	_, err = writer.HandleEvent(ctx, wal.RowEvent{
		Schema:     "public",
		Table:      "orders",
		Columns:    []wal.Column{{Name: "id", OID: 23, IsKey: true}, {Name: "name", OID: 25}},
		KeyColumns: []int{0},
		Op:         wal.OpInsert,
		Values: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte("1")},
			{Name: "name", OID: 25, Value: []byte("alice")},
		},
	})
	if err != nil {
		t.Fatalf("handle event: %v", err)
	}
	if err := writer.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	srv, err := NewServer(ServerConfig{
		ListenAddr:       ":0",
		TargetFormat:     "ducklake",
		DuckLakeCatalog:  catalogPath,
		DuckLakeDataPath: dataPath,
	}, nil, logger)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer srv.Close()
	if err := srv.refreshAndRegister(ctx); err != nil {
		t.Fatalf("refreshAndRegister: %v", err)
	}
	var count int
	if err := srv.duckDB.QueryRow(`SELECT count(*) FROM orders`).Scan(&count); err != nil {
		t.Fatalf("query unqualified view: %v", err)
	}
	if count != 1 {
		t.Fatalf("got count=%d, want 1", count)
	}
	if err := srv.duckDB.QueryRow(`SELECT count(*) FROM public_orders`).Scan(&count); err != nil {
		t.Fatalf("query qualified view: %v", err)
	}
	if count != 1 {
		t.Fatalf("got qualified count=%d, want 1", count)
	}
}
