package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/viggy28/streambed/internal/storage"
)

func timeTravelTestCatalog(t *testing.T) *TableCatalog {
	t.Helper()
	mem := storage.NewMemS3Client("bucket")
	catalog := NewTableCatalog(mem, "bucket", "prefix", testLogger())
	catalog.tables = map[string]TableInfo{
		"public.orders": {
			Schema: "public", Table: "orders", S3Path: "s3://bucket/prefix/public/orders",
		},
		"public.customers": {
			Schema: "public", Table: "customers", S3Path: "s3://bucket/prefix/public/customers",
		},
	}
	timestampMS := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC).UnixMilli()
	metadata := []byte(fmt.Sprintf(`{"current-snapshot-id":42,"snapshots":[{"snapshot-id":42,"sequence-number":1,"timestamp-ms":%d}]}`, timestampMS))
	for _, table := range []string{"orders", "customers"} {
		prefix := "prefix/public/" + table + "/metadata/"
		if err := mem.PutObject(context.Background(), prefix+"version-hint.text", []byte("2"), "text/plain"); err != nil {
			t.Fatalf("write version hint: %v", err)
		}
		if err := mem.PutObject(context.Background(), prefix+"v2.metadata.json", metadata, "application/json"); err != nil {
			t.Fatalf("write metadata: %v", err)
		}
	}
	return catalog
}

func TestPrepareTimeTravelDuckLakeKeepsNativeQuery(t *testing.T) {
	query := `SELECT o.id, c.name
		FROM public.orders AS o AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00')
		JOIN public.customers AS c AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00.000000')
		ON c.id = o.customer_id`

	got, err := prepareTimeTravelQuery(context.Background(), query, "ducklake", nil)
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}
	if got != query {
		t.Fatalf("DuckLake query changed:\n%s", got)
	}
}

func TestPrepareTimeTravelRejectsUnsupportedTargetFormat(t *testing.T) {
	query := `SELECT * FROM orders AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00')`
	_, err := prepareTimeTravelQuery(context.Background(), query, "delta", nil)
	if err == nil || !strings.Contains(err.Error(), `unsupported target format "delta"`) {
		t.Fatalf("got error %v, want unsupported-target error", err)
	}
}

func TestPrepareTimeTravelSupportsCatalogQualifiedDuckLakeTable(t *testing.T) {
	query := `SELECT * FROM streambed.public.orders AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00')`
	got, err := prepareTimeTravelQuery(context.Background(), query, "ducklake", nil)
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}
	if got != query {
		t.Fatalf("DuckLake query changed: %s", got)
	}

	got, err = prepareTimeTravelQuery(context.Background(), query, "iceberg", timeTravelTestCatalog(t))
	if err != nil {
		t.Fatalf("prepare Iceberg query: %v", err)
	}
	if !strings.Contains(got, "s3://bucket/prefix/public/orders") {
		t.Fatalf("catalog-qualified table was not resolved: %s", got)
	}
}

func TestPrepareTimeTravelRejectsDifferentTimestamps(t *testing.T) {
	query := `SELECT *
		FROM orders AS o AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00')
		JOIN customers AS c AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:01') ON true`

	_, err := prepareTimeTravelQuery(context.Background(), query, "ducklake", nil)
	if err == nil || !strings.Contains(err.Error(), "same timestamp") {
		t.Fatalf("got error %v, want same-timestamp error", err)
	}
}

func TestPrepareTimeTravelRewritesIcebergHistoricalJoin(t *testing.T) {
	query := `WITH selected AS (
		SELECT * FROM public.orders AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00')
	)
	SELECT o.id, c.name
	FROM selected o
	JOIN public.customers AS c AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00') ON c.id = o.customer_id`

	got, err := prepareTimeTravelQuery(context.Background(), query, "iceberg", timeTravelTestCatalog(t))
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}
	for _, table := range []string{"orders", "customers"} {
		want := "iceberg_scan('s3://bucket/prefix/public/" + table + "', allow_moved_paths = true, version = '2')"
		if !strings.Contains(got, want) {
			t.Errorf("rewritten query missing %s scan:\n%s", table, got)
		}
	}
	if strings.Contains(got, " AT (") {
		t.Fatalf("rewritten query still contains AT clause:\n%s", got)
	}
}

func TestPrepareTimeTravelSupportsQuotedIdentifiers(t *testing.T) {
	query := `SELECT * FROM "public"."orders" AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10T12:00:00')`
	got, err := prepareTimeTravelQuery(context.Background(), query, "iceberg", timeTravelTestCatalog(t))
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}
	if !strings.Contains(got, "version = '2'") {
		t.Fatalf("timestamp was not resolved to the expected snapshot: %s", got)
	}
}

func TestPrepareTimeTravelIgnoresCommentsAndStrings(t *testing.T) {
	query := `SELECT 'orders AT (TIMESTAMP => TIMESTAMPTZ ''2020-01-01 00:00:00'')' AS example,
		$$customers AT (TIMESTAMP => TIMESTAMPTZ '2020-01-01 00:00:00')$$ AS dollar_example
		-- FROM orders AT (TIMESTAMP => TIMESTAMPTZ '2020-01-01 00:00:00')
		FROM orders`
	got, err := prepareTimeTravelQuery(context.Background(), query, "ducklake", nil)
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}
	if got != query {
		t.Fatalf("query changed:\n%s", got)
	}
}

func TestPrepareTimeTravelRejectsSnapshotVersion(t *testing.T) {
	query := `SELECT * FROM orders AT (VERSION => 3)`
	_, err := prepareTimeTravelQuery(context.Background(), query, "ducklake", nil)
	if err == nil || !strings.Contains(err.Error(), "V1 requires") {
		t.Fatalf("got error %v, want unsupported-clause error", err)
	}
}

func TestPrepareTimeTravelTreatsNaiveTimestampAsUTCAndAcceptsOffsets(t *testing.T) {
	query := `SELECT *
		FROM orders AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00')
		JOIN customers AT (TIMESTAMP => TIMESTAMPTZ '2026-08-10 14:00:00+02:00') ON true`
	if _, err := prepareTimeTravelQuery(context.Background(), query, "ducklake", nil); err != nil {
		t.Fatalf("equivalent timestamps: %v", err)
	}
}
