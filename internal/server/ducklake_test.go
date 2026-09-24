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

func TestDuckLakeServerHistoricalJoinAtTimestamp(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	catalogPath := filepath.Join(dir, "catalog.ducklake")
	dataPath := filepath.Join(dir, "data") + "/"
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	writer, err := ducklake.NewWriter(ctx, ducklake.Config{
		CatalogPath: catalogPath, CatalogStore: "duckdb", DataPath: dataPath,
	}, nil, 100, time.Hour, logger)
	if err != nil {
		t.Fatalf("ducklake writer: %v", err)
	}
	defer writer.Close()

	writeDuckLakeRow(t, writer, "orders", 1, "old-order")
	writeDuckLakeRow(t, writer, "customers", 1, "old-customer")
	if err := writer.FlushAll(ctx); err != nil {
		t.Fatalf("initial flush: %v", err)
	}

	srv, err := NewServer(ServerConfig{
		TargetFormat: "ducklake", DuckLakeCatalog: catalogPath,
		DuckLakeCatalogStore: "duckdb", DuckLakeDataPath: dataPath,
	}, nil, logger)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer srv.Close()

	var firstSnapshot time.Time
	if err := srv.duckDB.QueryRow(`SELECT max(snapshot_time) FROM streambed.snapshots()`).Scan(&firstSnapshot); err != nil {
		t.Fatalf("read initial snapshot time: %v", err)
	}

	updateDuckLakeRow(t, writer, "orders", 1, "new-order")
	updateDuckLakeRow(t, writer, "customers", 1, "new-customer")
	if err := writer.FlushAll(ctx); err != nil {
		t.Fatalf("mutation flush: %v", err)
	}
	if err := srv.resetDuckLakeQueryDB(ctx); err != nil {
		t.Fatalf("refresh query catalog: %v", err)
	}
	var secondSnapshot time.Time
	if err := srv.duckDB.QueryRow(`SELECT max(snapshot_time) FROM streambed.snapshots()`).Scan(&secondSnapshot); err != nil {
		t.Fatalf("read mutation snapshot time: %v", err)
	}
	if !secondSnapshot.After(firstSnapshot) {
		t.Fatalf("mutation snapshot %s is not after initial snapshot %s", secondSnapshot, firstSnapshot)
	}

	// Use a point strictly between commits so the test does not depend on
	// DuckLake's exact-boundary timestamp convention.
	historicalTime := firstSnapshot.Add(secondSnapshot.Sub(firstSnapshot) / 2)
	timestamp := historicalTime.UTC().Format("2006-01-02 15:04:05.999999999")
	query := fmt.Sprintf(`
		SELECT o.name, c.name
		FROM public.orders AS o AT (TIMESTAMP => TIMESTAMPTZ '%s')
		JOIN public.customers AS c AT (TIMESTAMP => TIMESTAMPTZ '%s') ON c.id = o.id`, timestamp, timestamp)
	prepared, err := prepareTimeTravelQuery(ctx, query, "ducklake", nil)
	if err != nil {
		t.Fatalf("validate historical join: %v", err)
	}
	if _, err := srv.handleParse(ctx, query); err != nil {
		t.Fatalf("historical join through query server: %v", err)
	}
	var orderName, customerName string
	if err := srv.duckDB.QueryRow(prepared).Scan(&orderName, &customerName); err != nil {
		t.Fatalf("historical join: %v", err)
	}
	if orderName != "old-order" || customerName != "old-customer" {
		t.Fatalf("historical names = %q, %q; want old-order, old-customer", orderName, customerName)
	}

	if err := srv.duckDB.QueryRow(`SELECT o.name, c.name FROM public.orders o JOIN public.customers c ON c.id = o.id`).Scan(&orderName, &customerName); err != nil {
		t.Fatalf("latest join: %v", err)
	}
	if orderName != "new-order" || customerName != "new-customer" {
		t.Fatalf("latest names = %q, %q; want new-order, new-customer", orderName, customerName)
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

func updateDuckLakeRow(t *testing.T, writer *ducklake.Writer, table string, id int, name string) {
	t.Helper()
	_, err := writer.HandleEvent(context.Background(), wal.RowEvent{
		Schema:     "public",
		Table:      table,
		Columns:    []wal.Column{{Name: "id", OID: 23, IsKey: true}, {Name: "name", OID: 25}},
		KeyColumns: []int{0},
		Op:         wal.OpUpdate,
		OldKey:     []wal.ColumnValue{{Name: "id", OID: 23, Value: []byte(fmt.Sprint(id))}},
		Values: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte(fmt.Sprint(id))},
			{Name: "name", OID: 25, Value: []byte(name)},
		},
	})
	if err != nil {
		t.Fatalf("update %s row: %v", table, err)
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
