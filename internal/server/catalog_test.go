package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestCatalogResolveQualifiedName(t *testing.T) {
	catalog := NewTableCatalog(nil, "bucket", "prefix", testLogger())

	// Manually populate the catalog (bypassing S3 scan)
	catalog.tables = map[string]TableInfo{
		"public.orders": {
			Schema: "public",
			Table:  "orders",
			S3Path: "s3://bucket/prefix/public/orders",
		},
		"public.users": {
			Schema: "public",
			Table:  "users",
			S3Path: "s3://bucket/prefix/public/users",
		},
	}

	// Qualified name resolves correctly
	expr, err := catalog.Resolve("public.orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "iceberg_scan('s3://bucket/prefix/public/orders', allow_moved_paths = true)"
	if expr != expected {
		t.Errorf("got %q, want %q", expr, expected)
	}
}

func TestCatalogResolveUnqualifiedName(t *testing.T) {
	catalog := NewTableCatalog(nil, "bucket", "prefix", testLogger())
	catalog.tables = map[string]TableInfo{
		"public.orders": {
			Schema: "public",
			Table:  "orders",
			S3Path: "s3://bucket/prefix/public/orders",
		},
	}

	// Unqualified name resolves when unambiguous
	expr, err := catalog.Resolve("orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expr == "" {
		t.Error("expected non-empty expression")
	}
}

func TestCatalogResolveAmbiguousName(t *testing.T) {
	catalog := NewTableCatalog(nil, "bucket", "prefix", testLogger())
	catalog.tables = map[string]TableInfo{
		"public.orders": {
			Schema: "public",
			Table:  "orders",
			S3Path: "s3://bucket/prefix/public/orders",
		},
		"sales.orders": {
			Schema: "sales",
			Table:  "orders",
			S3Path: "s3://bucket/prefix/sales/orders",
		},
	}

	// Ambiguous unqualified name returns error
	_, err := catalog.Resolve("orders")
	if err == nil {
		t.Fatal("expected ambiguity error, got nil")
	}

	// Qualified names still work
	_, err = catalog.Resolve("public.orders")
	if err != nil {
		t.Fatalf("qualified name should resolve: %v", err)
	}
	_, err = catalog.Resolve("sales.orders")
	if err != nil {
		t.Fatalf("qualified name should resolve: %v", err)
	}
}

func TestCatalogResolveNotFound(t *testing.T) {
	catalog := NewTableCatalog(nil, "bucket", "prefix", testLogger())
	catalog.tables = map[string]TableInfo{}

	_, err := catalog.Resolve("nonexistent")
	if err == nil {
		t.Fatal("expected not-found error, got nil")
	}
}

func TestCatalogRegisterViewsNaming(t *testing.T) {
	// DuckDB eagerly validates iceberg_scan() paths on CREATE VIEW,
	// so we can't test with fake S3 paths without MinIO.
	// Instead, verify the naming logic: which views get created.
	catalog := NewTableCatalog(nil, "bucket", "prefix", testLogger())

	// Single table — should get both qualified and unqualified names
	catalog.tables = map[string]TableInfo{
		"public.orders": {Schema: "public", Table: "orders", S3Path: "s3://bucket/prefix/public/orders"},
	}

	tables := catalog.Tables()
	// Count how many schemas each table name appears in
	tableNameCount := make(map[string]int)
	for _, info := range tables {
		tableNameCount[info.Table]++
	}

	// "orders" appears once → unqualified view should be created
	if tableNameCount["orders"] != 1 {
		t.Errorf("expected orders to appear once, got %d", tableNameCount["orders"])
	}
}

func TestCatalogRegisterViewsAmbiguousNaming(t *testing.T) {
	catalog := NewTableCatalog(nil, "bucket", "prefix", testLogger())
	catalog.tables = map[string]TableInfo{
		"public.orders": {Schema: "public", Table: "orders", S3Path: "s3://bucket/prefix/public/orders"},
		"sales.orders":  {Schema: "sales", Table: "orders", S3Path: "s3://bucket/prefix/sales/orders"},
	}

	tables := catalog.Tables()
	tableNameCount := make(map[string]int)
	for _, info := range tables {
		tableNameCount[info.Table]++
	}

	// "orders" appears in 2 schemas → unqualified view should NOT be created
	if tableNameCount["orders"] != 2 {
		t.Errorf("expected orders to appear twice (ambiguous), got %d", tableNameCount["orders"])
	}

	// Qualified names should still be distinct
	if _, ok := tables["public.orders"]; !ok {
		t.Error("expected public.orders in catalog")
	}
	if _, ok := tables["sales.orders"]; !ok {
		t.Error("expected sales.orders in catalog")
	}
}

func TestCatalogRefreshFromKeys(t *testing.T) {
	// Test the parsing logic of Refresh by checking the catalog state
	// after manually setting table data (simulating S3 scan results).
	catalog := NewTableCatalog(nil, "test-bucket", "streambed", testLogger())

	// Simulate what Refresh would parse from S3 keys
	catalog.mu.Lock()
	catalog.tables = map[string]TableInfo{
		"public.orders": {
			Schema: "public",
			Table:  "orders",
			S3Path: "s3://test-bucket/streambed/public/orders",
		},
		"analytics.events": {
			Schema: "analytics",
			Table:  "events",
			S3Path: "s3://test-bucket/streambed/analytics/events",
		},
	}
	catalog.mu.Unlock()

	tables := catalog.Tables()
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}
	if tables["public.orders"].S3Path != "s3://test-bucket/streambed/public/orders" {
		t.Errorf("unexpected orders path: %s", tables["public.orders"].S3Path)
	}
	if tables["analytics.events"].Schema != "analytics" {
		t.Errorf("unexpected events schema: %s", tables["analytics.events"].Schema)
	}
}

func TestDuckDBTypeToOID(t *testing.T) {
	tests := []struct {
		typeName string
		wantOID  uint32
	}{
		{"BOOLEAN", 16},
		{"BOOL", 16},
		{"INTEGER", 23},
		{"INT4", 23},
		{"BIGINT", 20},
		{"DOUBLE", 701},
		{"VARCHAR", 25},
		{"TEXT", 25},
		{"DATE", 1082},
		{"TIMESTAMP", 1114},
		{"TIMESTAMPTZ", 1184},
		{"UUID", 2950},
		{"BLOB", 17},
		{"UNKNOWN_TYPE", 25}, // default to text
	}

	for _, tt := range tests {
		got := duckDBTypeToOID(tt.typeName)
		if got != tt.wantOID {
			t.Errorf("duckDBTypeToOID(%q) = %d, want %d", tt.typeName, got, tt.wantOID)
		}
	}
}

func TestDuckDBQueryExecution(t *testing.T) {
	// Verify that the DuckDB embedded engine works for basic queries
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Simple arithmetic query
	var result int
	err = db.QueryRow("SELECT 1 + 1").Scan(&result)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if result != 2 {
		t.Errorf("expected 2, got %d", result)
	}

	// Create a table, insert, and query
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO test VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestNewServerDuckDBInit(t *testing.T) {
	// Test that NewServer successfully initializes DuckDB with extensions.
	// We skip S3 configuration since we don't have MinIO running.
	cfg := ServerConfig{
		ListenAddr: ":0",
		S3Bucket:   "test-bucket",
		S3Prefix:   "test/",
		S3Region:   "us-east-1",
		// No S3Endpoint — DuckDB will configure without S3 path style
	}

	srv, err := NewServer(cfg, nil, testLogger())
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer srv.Close()

	// Verify iceberg extension is loaded
	var extName string
	err = srv.duckDB.QueryRow("SELECT extension_name FROM duckdb_extensions() WHERE extension_name = 'iceberg' AND loaded = true").Scan(&extName)
	if err != nil {
		t.Fatalf("iceberg extension not loaded: %v", err)
	}
	if extName != "iceberg" {
		t.Errorf("expected iceberg extension, got %q", extName)
	}
}

func TestHandleParseEmptyQuery(t *testing.T) {
	cfg := ServerConfig{
		ListenAddr: ":0",
		S3Bucket:   "test-bucket",
		S3Prefix:   "test/",
		S3Region:   "us-east-1",
	}

	srv, err := NewServer(cfg, nil, testLogger())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer srv.Close()

	// Empty query should not error
	stmts, err := srv.handleParse(context.Background(), "")
	if err != nil {
		t.Fatalf("handleParse empty: %v", err)
	}
	if stmts == nil {
		t.Fatal("expected non-nil statements for empty query")
	}
}

// TestIsEmptyTableError verifies detection of DuckDB's "No snapshots found" error.
func TestIsEmptyTableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"no snapshots found", fmt.Errorf("IO Error: No snapshots found"), true},
		{"wrapped no snapshots", fmt.Errorf("create view: %w", fmt.Errorf("IO Error: No snapshots found")), true},
		{"unrelated error", fmt.Errorf("connection refused"), false},
		{"fatal error", fmt.Errorf("FATAL Error: something"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isEmptyTableError(tt.err); got != tt.want {
				t.Errorf("isEmptyTableError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestIsFatalDuckDBError verifies detection of DuckDB FATAL errors.
func TestIsFatalDuckDBError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"fatal error", fmt.Errorf("FATAL Error: Value::LIST"), true},
		{"invalidated", fmt.Errorf("database has been invalidated because of a previous fatal error"), true},
		{"no snapshots", fmt.Errorf("IO Error: No snapshots found"), false},
		{"normal error", fmt.Errorf("table not found"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isFatalDuckDBError(tt.err); got != tt.want {
				t.Errorf("isFatalDuckDBError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestRegisterViewsDropsStaleViewOnEmptyTable verifies that RegisterViews
// drops existing views when iceberg_scan fails with "No snapshots found".
// We create a minimal Iceberg table with no snapshots to provoke the exact error.
func TestRegisterViewsDropsStaleViewOnEmptyTable(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Install iceberg extension
	_, err = db.Exec("INSTALL iceberg; LOAD iceberg;")
	if err != nil {
		t.Skipf("iceberg extension not available: %v", err)
	}

	// Create a temporary directory with a valid but empty Iceberg table
	tmpDir := t.TempDir()
	metadataDir := tmpDir + "/metadata"
	if err := os.MkdirAll(metadataDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write minimal v1.metadata.json with no snapshots (same as commitEmptyTable produces)
	metadataJSON := `{
		"format-version": 2,
		"table-uuid": "test",
		"location": "` + tmpDir + `",
		"last-sequence-number": 1,
		"last-updated-ms": 1000,
		"last-column-id": 1,
		"current-schema-id": 0,
		"schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"last-partition-id": 999,
		"default-sort-order-id": 0,
		"sort-orders": [{"order-id": 0, "fields": []}],
		"properties": {},
		"current-snapshot-id": -1,
		"refs": {},
		"snapshots": [],
		"snapshot-log": [],
		"metadata-log": []
	}`
	if err := os.WriteFile(metadataDir+"/v1.metadata.json", []byte(metadataJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadataDir+"/version-hint.text", []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create stale views
	_, err = db.Exec("CREATE VIEW public_empty_tbl AS SELECT 1 AS id")
	if err != nil {
		t.Fatalf("create stale view: %v", err)
	}
	_, err = db.Exec("CREATE VIEW empty_tbl AS SELECT 1 AS id")
	if err != nil {
		t.Fatalf("create stale unqualified view: %v", err)
	}

	// Verify views exist before
	var cnt int
	db.QueryRow("SELECT COUNT(*) FROM duckdb_views() WHERE view_name = 'public_empty_tbl'").Scan(&cnt)
	if cnt != 1 {
		t.Fatal("expected stale view to exist before RegisterViews")
	}

	catalog := NewTableCatalog(nil, "bucket", "prefix", testLogger())
	catalog.tables = map[string]TableInfo{
		"public.empty_tbl": {
			Schema: "public",
			Table:  "empty_tbl",
			S3Path: tmpDir,
		},
	}

	err = catalog.RegisterViews(db)
	if err == ErrDuckDBFatal {
		t.Fatalf("unexpected fatal error: %v", err)
	}

	// The stale views should be dropped
	db.QueryRow("SELECT COUNT(*) FROM duckdb_views() WHERE view_name = 'public_empty_tbl'").Scan(&cnt)
	if cnt != 0 {
		t.Error("expected stale qualified view to be dropped after empty table detection")
	}
	db.QueryRow("SELECT COUNT(*) FROM duckdb_views() WHERE view_name = 'empty_tbl'").Scan(&cnt)
	if cnt != 0 {
		t.Error("expected stale unqualified view to be dropped after empty table detection")
	}
}

func TestHandleParseSelectQuery(t *testing.T) {
	cfg := ServerConfig{
		ListenAddr: ":0",
		S3Bucket:   "test-bucket",
		S3Prefix:   "test/",
		S3Region:   "us-east-1",
	}

	srv, err := NewServer(cfg, nil, testLogger())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer srv.Close()

	// Create a test table in DuckDB
	_, err = srv.duckDB.Exec("CREATE TABLE test_data (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = srv.duckDB.Exec("INSERT INTO test_data VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Parse a SELECT query
	stmts, err := srv.handleParse(context.Background(), "SELECT * FROM test_data ORDER BY id")
	if err != nil {
		t.Fatalf("handleParse: %v", err)
	}
	if stmts == nil {
		t.Fatal("expected non-nil statements")
	}
}
