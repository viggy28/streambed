package server

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestValidateReadOnlyQuery(t *testing.T) {
	t.Parallel()

	accepted := []string{
		"SELECT 1",
		"  /* demo */ SELECT * FROM front_page;",
		"SELECT 'a;b' AS value",
		"-- comment\nSELECT 1",
		"WITH selected AS (SELECT 1 AS id) SELECT id FROM selected",
	}
	for _, query := range accepted {
		if err := validateReadOnlyQuery(query); err != nil {
			t.Errorf("validateReadOnlyQuery(%q): %v", query, err)
		}
	}

	rejected := []string{
		"INSERT INTO stories VALUES (1)",
		"WITH selected AS (SELECT 1) DELETE FROM stories",
		"WITH changed AS (UPDATE stories SET title = 'nope' RETURNING *) SELECT * FROM changed",
		"SELECT 1; DROP TABLE stories",
		"PRAGMA version",
		strings.Repeat(" ", maxQueryBytes) + "SELECT 1",
	}
	for _, query := range rejected {
		if err := validateReadOnlyQuery(query); err == nil {
			t.Errorf("validateReadOnlyQuery(%q) unexpectedly succeeded", query)
		}
	}
}

func TestDuckDBConfigUsesEndpointTLSAndLocksExternalAccess(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")

	stmts := strings.Join(duckDBConfigStatements(ServerConfig{
		S3Bucket:   "streambed-hn-demo",
		S3Prefix:   "hn-demo",
		S3Endpoint: "https://t3.storage.dev",
		S3Region:   "auto",
	}), "\n")

	for _, expected := range []string{
		"ENDPOINT 't3.storage.dev'",
		"USE_SSL true",
		"s3://streambed-hn-demo/hn-demo/",
		"SET enable_external_access = false",
		"SET lock_configuration = true",
	} {
		if !strings.Contains(stmts, expected) {
			t.Errorf("DuckDB configuration missing %q:\n%s", expected, stmts)
		}
	}
	if strings.Contains(stmts, "SET GLOBAL s3_secret_access_key") {
		t.Fatalf("S3 secret must not be stored in a query-visible global setting")
	}
}

func TestDuckDBBlocksLocalFileReads(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-that-must-stay-hidden")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if err := configureDuckDB(db, ServerConfig{
		S3Bucket:   "demo",
		S3Prefix:   "hn",
		S3Endpoint: "https://t3.storage.dev",
		S3Region:   "auto",
	}); err != nil {
		t.Fatalf("configureDuckDB: %v", err)
	}
	if _, err := db.Query(`SELECT * FROM read_text('/etc/passwd')`); err == nil {
		t.Fatal("local file read unexpectedly succeeded")
	}
	var renderedSecret string
	if err := db.QueryRow(`SELECT secret_string FROM duckdb_secrets() WHERE name = 'streambed_s3'`).Scan(&renderedSecret); err != nil {
		t.Fatalf("query redacted DuckDB secret: %v", err)
	}
	if strings.Contains(renderedSecret, "test-secret-that-must-stay-hidden") {
		t.Fatalf("DuckDB exposed the S3 secret: %s", renderedSecret)
	}
}

func TestHandleParseEnforcesResultRowLimit(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	srv := &Server{
		cfg: ServerConfig{
			TargetFormat:   "iceberg",
			QueryTimeout:   time.Second,
			MaxResultRows:  2,
			MaxResultBytes: 1024,
		},
		duckDB: db,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	_, err = srv.handleParse(context.Background(), "SELECT * FROM range(3)")
	if err == nil || !strings.Contains(err.Error(), "row limit") {
		t.Fatalf("handleParse error = %v, want row limit error", err)
	}
}
