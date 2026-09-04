package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/viggy28/streambed/internal/ducklake"
	"github.com/viggy28/streambed/internal/wal"
)

func TestDuckLakeServerUsesAttachedCatalogDirectly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	catalogPath := filepath.Join(dir, "catalog.ducklake")
	dataPath := filepath.Join(dir, "data") + "/"
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	writer, err := ducklake.NewWriter(ctx, ducklake.Config{
		CatalogPath:  catalogPath,
		CatalogStore: "duckdb",
		DataPath:     dataPath,
	}, nil, 100, time.Hour, logger)
	if err != nil {
		t.Fatalf("ducklake writer: %v", err)
	}
	defer writer.Close()

	writeDuckLakeRow(t, writer, "orders", 1, "alice")
	if err := writer.FlushAll(ctx); err != nil {
		t.Fatalf("initial flush: %v", err)
	}

	srv, err := NewServer(ServerConfig{
		ListenAddr:           ":0",
		TargetFormat:         "ducklake",
		DuckLakeCatalog:      catalogPath,
		DuckLakeCatalogStore: "duckdb",
		DuckLakeDataPath:     dataPath,
	}, nil, logger)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer srv.Close()

	assertDuckLakeCount(t, srv, `streambed.public.orders`, 1) // fully qualified
	assertDuckLakeCount(t, srv, `public.orders`, 1)           // current catalog
	assertDuckLakeCount(t, srv, `orders`, 1)                  // current catalog and schema

	if _, err := srv.duckDB.Exec(`INSERT INTO streambed.public.orders VALUES (99, 'mallory')`); err == nil {
		t.Fatal("query-side DuckLake attachment accepted a write")
	}
	// Mutating the query session must not disturb the writer's attachment. The
	// next query also gets a fresh session, so this DETACH is self-contained.
	if _, err := srv.duckDB.Exec(`USE memory; DETACH streambed`); err != nil {
		t.Fatalf("detach query-side catalog: %v", err)
	}

	// Existing tables expose newly committed rows without recreating a view.
	writeDuckLakeRow(t, writer, "orders", 2, "bob")
	if err := writer.FlushAll(ctx); err != nil {
		t.Fatalf("existing-table flush: %v", err)
	}
	assertDuckLakeCount(t, srv, `public.orders`, 2)

	// Newly committed tables are visible without waiting for catalog discovery.
	writeDuckLakeRow(t, writer, "customers", 1, "carol")
	if err := writer.FlushAll(ctx); err != nil {
		t.Fatalf("new-table flush: %v", err)
	}
	assertDuckLakeCount(t, srv, `public.customers`, 1)

	var viewCount int
	if err := srv.duckDB.QueryRow(`
		SELECT count(*)
		FROM information_schema.tables
		WHERE table_type = 'VIEW' AND table_name IN ('orders', 'public_orders', 'customers', 'public_customers')
	`).Scan(&viewCount); err != nil {
		t.Fatalf("list compatibility views: %v", err)
	}
	if viewCount != 0 {
		t.Fatalf("got %d DuckLake compatibility views, want none", viewCount)
	}
}

func TestDuckLakeServerStartsBeforePublicSchemaExists(t *testing.T) {
	for _, store := range []string{"duckdb", "sqlite"} {
		t.Run(store, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			catalogPath := filepath.Join(dir, "catalog."+store)
			dataPath := filepath.Join(dir, "data") + "/"
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

			writer, err := ducklake.NewWriter(ctx, ducklake.Config{
				CatalogPath:  catalogPath,
				CatalogStore: store,
				DataPath:     dataPath,
			}, nil, 100, time.Hour, logger)
			if err != nil {
				t.Fatalf("ducklake writer: %v", err)
			}
			defer writer.Close()

			// The catalog has main at this point, but no PostgreSQL public schema.
			srv, err := NewServer(ServerConfig{
				ListenAddr:           ":0",
				TargetFormat:         "ducklake",
				DuckLakeCatalog:      catalogPath,
				DuckLakeCatalogStore: store,
				DuckLakeDataPath:     dataPath,
			}, nil, logger)
			if err != nil {
				t.Fatalf("NewServer with empty catalog: %v", err)
			}
			defer srv.Close()

			var currentSchema string
			if err := srv.duckDB.QueryRow(`SELECT current_schema()`).Scan(&currentSchema); err != nil {
				t.Fatalf("current schema: %v", err)
			}
			if currentSchema != "main" {
				t.Fatalf("current schema = %q, want main", currentSchema)
			}

			writeDuckLakeRow(t, writer, "orders", 1, "alice")
			if err := writer.FlushAll(ctx); err != nil {
				t.Fatalf("flush first public table: %v", err)
			}

			// Resetting the per-query session must notice public and make the new
			// table available without restarting the server.
			assertDuckLakeCount(t, srv, `orders`, 1)
		})
	}
}

func TestDuckLakeServerAttachesCatalogInStandaloneMode(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	catalogPath := filepath.Join(dir, "catalog.ducklake")
	dataPath := filepath.Join(dir, "data") + "/"
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	writer, err := ducklake.NewWriter(ctx, ducklake.Config{
		CatalogPath:  catalogPath,
		CatalogStore: "duckdb",
		DataPath:     dataPath,
	}, nil, 100, time.Hour, logger)
	if err != nil {
		t.Fatalf("ducklake writer: %v", err)
	}
	writeDuckLakeRow(t, writer, "orders", 1, "alice")
	if err := writer.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	srv, err := NewServer(ServerConfig{
		ListenAddr:           ":0",
		TargetFormat:         "ducklake",
		DuckLakeCatalog:      catalogPath,
		DuckLakeCatalogStore: "duckdb",
		DuckLakeDataPath:     dataPath,
	}, nil, logger)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer srv.Close()

	assertDuckLakeCount(t, srv, `streambed.public.orders`, 1)
	assertDuckLakeCount(t, srv, `public.orders`, 1)
	assertDuckLakeCount(t, srv, `orders`, 1)
}

func writeDuckLakeRow(t *testing.T, writer *ducklake.Writer, table string, id int, name string) {
	t.Helper()
	_, err := writer.HandleEvent(context.Background(), wal.RowEvent{
		Schema:     "public",
		Table:      table,
		Columns:    []wal.Column{{Name: "id", OID: 23, IsKey: true}, {Name: "name", OID: 25}},
		KeyColumns: []int{0},
		Op:         wal.OpInsert,
		Values: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte(fmt.Sprint(id))},
			{Name: "name", OID: 25, Value: []byte(name)},
		},
	})
	if err != nil {
		t.Fatalf("write %s row: %v", table, err)
	}
}

func assertDuckLakeCount(t *testing.T, srv *Server, table string, want int) {
	t.Helper()
	srv.duckDBMu.Lock()
	defer srv.duckDBMu.Unlock()
	if err := srv.resetDuckLakeQueryDB(context.Background()); err != nil {
		t.Fatalf("reset DuckLake query DB: %v", err)
	}
	var got int
	if err := srv.duckDB.QueryRow(`SELECT count(*) FROM ` + table).Scan(&got); err != nil {
		t.Fatalf("query %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("query %s: got count=%d, want %d", table, got, want)
	}
}
