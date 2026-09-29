package ducklake

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/viggy28/streambed/internal/wal"
)

func TestParseLogicalIndexSpec(t *testing.T) {
	t.Parallel()
	got, err := parseLogicalIndexSpec("public.events:user_id")
	if err != nil {
		t.Fatal(err)
	}
	if got.Schema != "public" || got.Table != "events" || got.Column != "user_id" {
		t.Fatalf("unexpected spec: %+v", got)
	}
	for _, invalid := range []string{"events:user_id", "public.events", "public.events:", ".events:id", "a.b.c:id"} {
		if _, err := parseLogicalIndexSpec(invalid); err == nil {
			t.Errorf("parseLogicalIndexSpec(%q) unexpectedly succeeded", invalid)
		}
	}
}

func TestBigIntLogicalIndexBackfillMaintenanceAndPruning(t *testing.T) {
	extensionPath := os.Getenv("STREAMBED_TEST_DUCKLAKE_EXTENSION")
	if extensionPath == "" {
		t.Skip("set STREAMBED_TEST_DUCKLAKE_EXTENSION to the Streambed DuckLake extension binary")
	}
	if _, err := os.Stat(extensionPath); err != nil {
		t.Fatalf("DuckLake extension: %v", err)
	}

	ctx := context.Background()
	dir := t.TempDir()
	catalogPath := filepath.Join(dir, "catalog.duckdb")
	writer, err := NewWriter(ctx, Config{
		CatalogPath:    catalogPath,
		CatalogStore:   "duckdb",
		DataPath:       filepath.Join(dir, "data") + "/",
		ExtensionPath:  extensionPath,
		LogicalIndexes: []string{"public.events:user_id"},
	}, nil, 100, time.Hour, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}

	columns := []wal.Column{{Name: "user_id", OID: 20}, {Name: "payload", OID: 25}}
	for batch, values := range [][]int64{{1, 100}, {20, 200}, {42, 300}} {
		for row, value := range values {
			_, err := writer.HandleEvent(ctx, wal.RowEvent{
				Schema: "public", Table: "events", Columns: columns, Op: wal.OpInsert,
				Values: []wal.ColumnValue{
					{Name: "user_id", OID: 20, Value: []byte(fmt.Sprint(value))},
					{Name: "payload", OID: 25, Value: []byte(fmt.Sprintf("b%d-r%d", batch, row))},
				},
				WALStartLSN: mustLSN(t, fmt.Sprintf("0/%X", 16+batch)),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.FlushAll(ctx); err != nil {
			t.Fatalf("flush batch %d: %v", batch, err)
		}
	}

	var count int
	if err := writer.db.QueryRowContext(ctx, `SELECT count(*) FROM streambed.public.events WHERE user_id = 42`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}
	rows, err := writer.db.QueryContext(ctx, `EXPLAIN ANALYZE SELECT * FROM streambed.public.events WHERE user_id = 42`)
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(value)
	}
	rows.Close()
	if !strings.Contains(plan.String(), "Total Files Read: 1") {
		t.Fatalf("logical index did not prune to one file:\n%s", plan.String())
	}

	// A CDC key update leaves a safe stale mapping for 42 and atomically maps
	// the replacement file for 43.
	if _, err := writer.HandleEvent(ctx, wal.RowEvent{
		Schema: "public", Table: "events", Columns: columns, KeyColumns: []int{0}, Op: wal.OpUpdate,
		OldKey: []wal.ColumnValue{{Name: "user_id", OID: 20, Value: []byte("42")}},
		Values: []wal.ColumnValue{
			{Name: "user_id", OID: 20, Value: []byte("43")},
			{Name: "payload", OID: 25, Value: []byte("updated")},
		}, WALStartLSN: mustLSN(t, "0/25"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := writer.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := writer.db.QueryRowContext(ctx, `SELECT count(*) FROM streambed.public.events WHERE user_id = 42`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("old value count=%d after update, want 0", count)
	}
	if err := writer.db.QueryRowContext(ctx, `SELECT count(*) FROM streambed.public.events WHERE user_id = 43`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("new value count=%d after update, want 1", count)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupting the definition lookup must abort file publication rather than
	// silently commit a file with missing mappings.
	rawMetadata, err := sql.Open("duckdb", catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rawMetadata.Exec(`ALTER TABLE streambed_equality_index_definition RENAME TO streambed_equality_index_definition_hidden`); err != nil {
		t.Fatal(err)
	}
	if err := rawMetadata.Close(); err != nil {
		t.Fatal(err)
	}
	brokenWriter, err := NewWriter(ctx, Config{
		CatalogPath: catalogPath, CatalogStore: "duckdb", DataPath: filepath.Join(dir, "data") + "/",
		ExtensionPath: extensionPath,
	}, nil, 100, time.Hour, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := brokenWriter.HandleEvent(ctx, wal.RowEvent{
		Schema: "public", Table: "events", Columns: columns, Op: wal.OpInsert,
		Values: []wal.ColumnValue{
			{Name: "user_id", OID: 20, Value: []byte("555")},
			{Name: "payload", OID: 25, Value: []byte("must-not-commit")},
		}, WALStartLSN: mustLSN(t, "0/26"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := brokenWriter.FlushAll(ctx); err == nil {
		t.Fatal("flush unexpectedly succeeded with missing logical-index definitions")
	}
	_ = brokenWriter.Close()
	rawMetadata, err = sql.Open("duckdb", catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rawMetadata.Exec(`ALTER TABLE streambed_equality_index_definition_hidden RENAME TO streambed_equality_index_definition`); err != nil {
		t.Fatal(err)
	}
	if err := rawMetadata.Close(); err != nil {
		t.Fatal(err)
	}
	verificationWriter, err := NewWriter(ctx, Config{
		CatalogPath: catalogPath, CatalogStore: "duckdb", DataPath: filepath.Join(dir, "data") + "/",
		ExtensionPath: extensionPath,
	}, nil, 100, time.Hour, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := verificationWriter.db.QueryRowContext(ctx, `SELECT count(*) FROM streambed.public.events WHERE user_id = 555`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed publication left %d rows for value 555, want 0", count)
	}
	if err := verificationWriter.Close(); err != nil {
		t.Fatal(err)
	}

	// The stock extension cannot maintain persistent mappings and must fail closed.
	if stockWriter, stockErr := NewWriter(ctx, Config{
		CatalogPath: catalogPath, CatalogStore: "duckdb", DataPath: filepath.Join(dir, "data") + "/",
	}, nil, 100, time.Hour, slog.New(slog.NewTextHandler(os.Stderr, nil))); stockErr == nil {
		_ = stockWriter.Close()
		t.Fatal("reopening an indexed catalog without the Streambed extension unexpectedly succeeded")
	} else if !strings.Contains(stockErr.Error(), "ducklake-extension is required") {
		t.Fatalf("unexpected missing-extension error: %v", stockErr)
	}

	// A READY definition is persistent: omitting --logical-index on restart must
	// still maintain mappings for newly committed files when the pinned extension remains loaded.
	writer, err = NewWriter(ctx, Config{
		CatalogPath: catalogPath, CatalogStore: "duckdb", DataPath: filepath.Join(dir, "data") + "/",
		ExtensionPath: extensionPath,
	}, nil, 100, time.Hour, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []int64{777, 1000} {
		if _, err := writer.HandleEvent(ctx, wal.RowEvent{
			Schema: "public", Table: "events", Columns: columns, Op: wal.OpInsert,
			Values: []wal.ColumnValue{
				{Name: "user_id", OID: 20, Value: []byte(fmt.Sprint(value))},
				{Name: "payload", OID: 25, Value: []byte("restart")},
			}, WALStartLSN: mustLSN(t, "0/30"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	metadata, err := sql.Open("duckdb", catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	var state string
	if err := metadata.QueryRow(`SELECT state FROM streambed_equality_index_definition`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "READY" {
		t.Fatalf("state=%q, want READY", state)
	}
	var marker string
	if err := metadata.QueryRow(`SELECT value FROM ducklake_metadata WHERE key = 'streambed_logical_indexes'`).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if marker != "true" {
		t.Fatalf("logical index capability marker=%q, want true", marker)
	}
	var mappings int
	if err := metadata.QueryRow(`SELECT count(*) FROM streambed_equality_index_bigint`).Scan(&mappings); err != nil {
		t.Fatal(err)
	}
	if mappings < 9 {
		t.Fatalf("mappings=%d, want at least 9 after update and restart maintenance", mappings)
	}
	var restartMappings int
	if err := metadata.QueryRow(`SELECT count(*) FROM streambed_equality_index_bigint WHERE value = 777`).Scan(&restartMappings); err != nil {
		t.Fatal(err)
	}
	if restartMappings != 1 {
		t.Fatalf("restart mappings for 777=%d, want 1", restartMappings)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}

	// Type evolution invalidates the definition atomically before ALTER COLUMN.
	writer, err = NewWriter(ctx, Config{
		CatalogPath: catalogPath, CatalogStore: "duckdb", DataPath: filepath.Join(dir, "data") + "/",
		ExtensionPath: extensionPath,
	}, nil, 100, time.Hour, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.HandleSchemaChange(ctx, &wal.RelationMessage{
		Namespace: "public", Name: "events",
		Columns: []wal.Column{{Name: "user_id", OID: 701}, {Name: "payload", OID: 25}},
		Changes: []wal.SchemaChange{{Type: wal.SchemaChangeTypeChange, Column: "user_id", OldOID: 20, NewOID: 701}},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	metadata, err = sql.Open("duckdb", catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.QueryRow(`SELECT state FROM streambed_equality_index_definition`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "INVALID" {
		t.Fatalf("state=%q after type change, want INVALID", state)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}

	// Dropping a source table removes both the definition and its mappings.
	writer, err = NewWriter(ctx, Config{
		CatalogPath: catalogPath, CatalogStore: "duckdb", DataPath: filepath.Join(dir, "data") + "/",
		ExtensionPath: extensionPath,
	}, nil, 100, time.Hour, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.DropTable(ctx, "public", "events"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	metadata, err = sql.Open("duckdb", catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	if err := metadata.QueryRow(`SELECT count(*) FROM streambed_equality_index_definition`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("definitions after drop=%d, want 0", count)
	}
	if err := metadata.QueryRow(`SELECT count(*) FROM streambed_equality_index_bigint`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("mappings after drop=%d, want 0", count)
	}
}
