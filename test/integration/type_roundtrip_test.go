//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/storage"
)

// TestPostgresTypeRoundTripToIceberg verifies that representative Postgres
// scalar types are mapped to the expected Iceberg schema types and that their
// inserted values can be read back through DuckDB's Iceberg reader.
func TestPostgresTypeRoundTripToIceberg(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	t.Cleanup(func() {
		cleanup(t)
		execSQL(t, "DROP TABLE IF EXISTS type_roundtrip_test")
	})

	execSQL(t, "DROP TABLE IF EXISTS type_roundtrip_test")
	execSQL(t, `CREATE TABLE type_roundtrip_test (
		id INTEGER PRIMARY KEY,
		bool_col BOOLEAN,
		int2_col SMALLINT,
		int4_col INTEGER,
		int8_col BIGINT,
		float4_col REAL,
		float8_col DOUBLE PRECISION,
		text_col TEXT,
		varchar_col VARCHAR(32),
		numeric_col NUMERIC(12,2),
		date_col DATE,
		timestamp_col TIMESTAMP,
		timestamptz_col TIMESTAMPTZ,
		uuid_col UUID,
		bytea_col BYTEA,
		json_col JSON
	)`)

	createSlotAndPublication(t)

	execSQL(t, `INSERT INTO type_roundtrip_test (
		id, bool_col, int2_col, int4_col, int8_col, float4_col, float8_col,
		text_col, varchar_col, numeric_col, date_col, timestamp_col,
		timestamptz_col, uuid_col, bytea_col, json_col
	) VALUES (
		1, true, -12345, 123456789, 922337203685477000, 123.5, -9876.54321,
		'hello iceberg', 'varchar value', 12345.67, DATE '2024-02-29',
		TIMESTAMP '2024-01-02 03:04:05.123456',
		TIMESTAMPTZ '2024-01-02 03:04:05.123456+00',
		'550e8400-e29b-41d4-a716-446655440000', decode('DEADBEEF00FF', 'hex'),
		'{"a":1,"b":"two"}'::json
	)`)
	// A second row verifies nullability for every non-key column.
	execSQL(t, "INSERT INTO type_roundtrip_test (id) VALUES (2)")

	runSync(t, ctx, 12*time.Second)

	if pgCount := pgRowCount(t, "type_roundtrip_test"); pgCount != 2 {
		t.Fatalf("expected 2 Postgres rows, got %d", pgCount)
	}
	if icebergCount := countNamedTableSnapshotRows(t, "public", "type_roundtrip_test"); icebergCount != 2 {
		t.Fatalf("expected 2 Iceberg rows, got %d", icebergCount)
	}

	assertTypeRoundTripIcebergSchema(t, ctx)
	assertTypeRoundTripValues(t)
}

func assertTypeRoundTripIcebergSchema(t *testing.T, ctx context.Context) {
	t.Helper()

	s3Client, err := storage.NewS3Client(ctx, s3Bucket, s3Region, minioEndpoint)
	if err != nil {
		t.Fatalf("create s3 client: %v", err)
	}
	catalog := iceberg.NewCatalog(s3Client, s3Bucket, s3Prefix)
	fields, err := catalog.GetSchemaFields(ctx, "public", "type_roundtrip_test")
	if err != nil {
		t.Fatalf("get iceberg schema fields: %v", err)
	}

	got := make(map[string]iceberg.IcebergType, len(fields))
	for _, field := range fields {
		got[field.Name] = field.Type
	}
	expected := map[string]iceberg.IcebergType{
		"id":              iceberg.TypeInt,
		"bool_col":        iceberg.TypeBoolean,
		"int2_col":        iceberg.TypeInt,
		"int4_col":        iceberg.TypeInt,
		"int8_col":        iceberg.TypeLong,
		"float4_col":      iceberg.TypeFloat,
		"float8_col":      iceberg.TypeDouble,
		"text_col":        iceberg.TypeString,
		"varchar_col":     iceberg.TypeString,
		"numeric_col":     iceberg.TypeString,
		"date_col":        iceberg.TypeDate,
		"timestamp_col":   iceberg.TypeTimestamp,
		"timestamptz_col": iceberg.TypeTimestampTZ,
		"uuid_col":        iceberg.TypeUUID,
		"bytea_col":       iceberg.TypeBinary,
		"json_col":        iceberg.TypeString,
	}
	for name, want := range expected {
		if got[name] != want {
			t.Errorf("iceberg field %s: got %q, want %q", name, got[name], want)
		}
	}
	if len(got) != len(expected) {
		t.Errorf("iceberg schema field count: got %d, want %d (fields=%v)", len(got), len(expected), got)
	}
}

func assertTypeRoundTripValues(t *testing.T) {
	t.Helper()

	duckDB := newTestDuckDB(t)
	icebergTable := fmt.Sprintf(
		"iceberg_scan('s3://%s/%s/public/type_roundtrip_test', allow_moved_paths = true)",
		s3Bucket, s3Prefix)

	valueMatchQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE
		id = 1
		AND bool_col = true
		AND int2_col = -12345
		AND int4_col = 123456789
		AND int8_col = 922337203685477000
		AND abs(CAST(float4_col AS DOUBLE) - 123.5) < 0.00001
		AND abs(float8_col - -9876.54321) < 0.0000001
		AND text_col = 'hello iceberg'
		AND varchar_col = 'varchar value'
		AND numeric_col = '12345.67'
		AND date_col = DATE '2024-02-29'
		AND timestamp_col = TIMESTAMP '2024-01-02 03:04:05.123456'
		AND timestamptz_col = TIMESTAMPTZ '2024-01-02 03:04:05.123456+00'
		AND CAST(uuid_col AS VARCHAR) = '550e8400-e29b-41d4-a716-446655440000'
		AND lower(hex(bytea_col)) = 'deadbeef00ff'
		AND json_col = '{"a":1,"b":"two"}'`, icebergTable)
	var valueMatches int64
	if err := duckDB.QueryRow(valueMatchQuery).Scan(&valueMatches); err != nil {
		t.Fatalf("query typed Iceberg values: %v", err)
	}
	if valueMatches != 1 {
		t.Fatalf("expected exactly one typed value row match, got %d", valueMatches)
	}

	nullMatchQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE
		id = 2
		AND bool_col IS NULL
		AND int2_col IS NULL
		AND int4_col IS NULL
		AND int8_col IS NULL
		AND float4_col IS NULL
		AND float8_col IS NULL
		AND text_col IS NULL
		AND varchar_col IS NULL
		AND numeric_col IS NULL
		AND date_col IS NULL
		AND timestamp_col IS NULL
		AND timestamptz_col IS NULL
		AND uuid_col IS NULL
		AND bytea_col IS NULL
		AND json_col IS NULL`, icebergTable)
	var nullMatches int64
	if err := duckDB.QueryRow(nullMatchQuery).Scan(&nullMatches); err != nil {
		t.Fatalf("query null Iceberg values: %v", err)
	}
	if nullMatches != 1 {
		t.Fatalf("expected exactly one null row match, got %d", nullMatches)
	}
}
