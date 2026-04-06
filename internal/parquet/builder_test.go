package parquet

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestBuildSimpleParquet(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},       // int4
		{Name: "name", OID: 25},     // text
		{Name: "active", OID: 16},   // bool
		{Name: "score", OID: 701},   // float8
	}

	rows := [][]Value{
		{val("1"), val("alice"), val("t"), val("3.14")},
		{val("2"), val("bob"), val("f"), val("2.72")},
		{val("3"), null(), val("t"), val("1.0")},
	}

	b := &Builder{}
	data, err := b.Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) == 0 {
		t.Fatal("expected non-empty parquet data")
	}

	// Read it back using parquet-go reader
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	if file.NumRows() != 3 {
		t.Errorf("expected 3 rows, got %d", file.NumRows())
	}
}

func TestBuildWithTimestamp(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "created_at", OID: 1184}, // timestamptz
	}

	rows := [][]Value{
		{val("1"), val("2024-01-15 10:30:00+00")},
		{val("2"), val("2024-06-15 14:00:00+00")},
	}

	b := &Builder{}
	data, err := b.Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}

	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	if file.NumRows() != 2 {
		t.Errorf("expected 2 rows, got %d", file.NumRows())
	}
}

func TestBuildWithDate(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "birth_date", OID: 1082}, // date
	}

	rows := [][]Value{
		{val("1"), val("2024-01-15")},
		{val("2"), val("1990-06-30")},
	}

	b := &Builder{}
	data, err := b.Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}

	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if file.NumRows() != 2 {
		t.Errorf("expected 2 rows, got %d", file.NumRows())
	}
}

func TestBuildNoRows(t *testing.T) {
	cols := []ColumnDef{{Name: "id", OID: 23}}
	b := &Builder{}
	data, err := b.Build(cols, nil)
	if err != nil {
		t.Fatalf("unexpected error for 0 rows: %v", err)
	}
	// Should produce a valid parquet file with 0 rows.
	if len(data) == 0 {
		t.Fatal("expected non-empty parquet data for schema-only file")
	}
	// Verify it can be read back.
	rows, err := ReadRows(data, cols)
	if err != nil {
		t.Fatalf("ReadRows failed on empty parquet: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestParseValue(t *testing.T) {
	tests := []struct {
		name string
		oid  uint32
		data string
	}{
		{"bool true", 16, "t"},
		{"bool false", 16, "f"},
		{"int2", 21, "42"},
		{"int4", 23, "12345"},
		{"int8", 20, "9876543210"},
		{"float4", 700, "3.14"},
		{"float8", 701, "2.718281828"},
		{"text", 25, "hello world"},
		{"date", 1082, "2024-01-15"},
		{"timestamp", 1114, "2024-01-15 10:30:00"},
		{"timestamptz", 1184, "2024-01-15 10:30:00+00"},
		{"uuid", 2950, "550e8400-e29b-41d4-a716-446655440000"},
		{"json", 114, `{"key": "value"}`},
		{"numeric", 1700, "123.45"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := parseValue(tt.oid, []byte(tt.data))
			if err != nil {
				t.Errorf("parseValue(%d, %q) error: %v", tt.oid, tt.data, err)
			}
			if v == nil {
				t.Errorf("parseValue(%d, %q) returned nil", tt.oid, tt.data)
			}
		})
	}
}

func val(s string) Value {
	return Value{Data: []byte(s)}
}

func null() Value {
	return Value{IsNull: true}
}

// TestBuildFieldIDs verifies that each Parquet column is tagged with an Iceberg
// field ID matching its position (1-indexed). Without field IDs, DuckDB's
// iceberg_scan returns NULL for every value — this is the regression test for
// that bug.
func TestBuildFieldIDs(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
		{Name: "created_at", OID: 1114},
		{Name: "active", OID: 16},
	}
	rows := [][]Value{{val("1"), val("alice"), val("2024-01-15 10:30:00"), val("t")}}

	data, err := (&Builder{}).Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}

	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// Walk the root's child columns and verify each has the expected field ID.
	// Map iteration order in schemaToGroup is non-deterministic, so we check by name.
	wantIDs := map[string]int{
		"id":         1,
		"name":       2,
		"created_at": 3,
		"active":     4,
	}

	children := file.Root().Columns()
	if len(children) != len(wantIDs) {
		t.Fatalf("got %d columns, want %d", len(children), len(wantIDs))
	}

	for _, child := range children {
		want, ok := wantIDs[child.Name()]
		if !ok {
			t.Errorf("unexpected column %q", child.Name())
			continue
		}
		if got := child.ID(); got != want {
			t.Errorf("column %q: got field_id=%d, want %d", child.Name(), got, want)
		}
	}
}

// TestBuildValuesRoundTrip writes typed values and reads them back, asserting
// that each column's value survives the pgoutput-text → parsed-Go → Parquet
// → read-back pipeline intact.
func TestBuildValuesRoundTrip(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},     // int4
		{Name: "big", OID: 20},    // int8
		{Name: "small", OID: 21},  // int2
		{Name: "name", OID: 25},   // text
		{Name: "active", OID: 16}, // bool
		{Name: "score", OID: 701}, // float8
		{Name: "ratio", OID: 700}, // float4
	}
	rows := [][]Value{
		{val("42"), val("9876543210"), val("7"), val("alice"), val("t"), val("3.14159"), val("2.5")},
	}

	data, err := (&Builder{}).Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewReader(bytes.NewReader(data))
	defer reader.Close()

	readRow := make(map[string]any)
	if err := reader.Read(&readRow); err != nil {
		t.Fatal(err)
	}

	assertEq(t, "id", readRow["id"], int32(42))
	assertEq(t, "big", readRow["big"], int64(9876543210))
	assertEq(t, "small", readRow["small"], int32(7))
	assertEq(t, "name", readRow["name"], "alice")
	assertEq(t, "active", readRow["active"], true)
	assertEq(t, "score", readRow["score"], 3.14159)
	// float4 round-trips through float32, allow small delta
	if got, ok := readRow["ratio"].(float32); !ok || roundFloat32(got, 2) != 2.5 {
		t.Errorf("ratio: got %v (%T), want 2.5", readRow["ratio"], readRow["ratio"])
	}
}

// TestBuildNullsRoundTrip verifies that IsNull=true values come back as
// absent/nil, and that non-null values in the same row survive alongside them.
func TestBuildNullsRoundTrip(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
		{Name: "score", OID: 701},
	}
	rows := [][]Value{
		{val("1"), null(), val("3.14")},
		{val("2"), val("bob"), null()},
	}

	data, err := (&Builder{}).Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewReader(bytes.NewReader(data))
	defer reader.Close()

	out := make([]map[string]any, 2)
	for i := range out {
		out[i] = make(map[string]any)
		if err := reader.Read(&out[i]); err != nil {
			t.Fatalf("read row %d: %v", i, err)
		}
	}

	// Row 0: id=1, name=NULL, score=3.14
	assertEq(t, "row0.id", out[0]["id"], int32(1))
	if v := out[0]["name"]; v != nil {
		t.Errorf("row0.name: got %v, want nil", v)
	}
	assertEq(t, "row0.score", out[0]["score"], 3.14)

	// Row 1: id=2, name=bob, score=NULL
	assertEq(t, "row1.id", out[1]["id"], int32(2))
	assertEq(t, "row1.name", out[1]["name"], "bob")
	if v := out[1]["score"]; v != nil {
		t.Errorf("row1.score: got %v, want nil", v)
	}
}

// TestBuildMultipleRows writes a larger batch and verifies every value
// round-trips in order.
func TestBuildMultipleRows(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "label", OID: 25},
	}
	const n = 100
	rows := make([][]Value, n)
	for i := range n {
		rows[i] = []Value{
			val(fmt.Sprintf("%d", i)),
			val(fmt.Sprintf("row-%d", i)),
		}
	}

	data, err := (&Builder{}).Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewReader(bytes.NewReader(data))
	defer reader.Close()

	for i := range n {
		row := make(map[string]any)
		if err := reader.Read(&row); err != nil {
			t.Fatalf("read row %d: %v", i, err)
		}
		if got := row["id"]; got != int32(i) {
			t.Errorf("row %d: id=%v, want %d", i, got, i)
		}
		want := fmt.Sprintf("row-%d", i)
		if got := row["label"]; got != want {
			t.Errorf("row %d: label=%v, want %q", i, got, want)
		}
	}
}

// TestBuildTimestampRoundTrip verifies timestamp parsing + round-trip.
// Timestamps are stored as microseconds since epoch.
func TestBuildTimestampRoundTrip(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "ts", OID: 1114},    // timestamp
		{Name: "tstz", OID: 1184},  // timestamptz
		{Name: "birth", OID: 1082}, // date
	}
	rows := [][]Value{
		{val("1"), val("2024-01-15 10:30:00"), val("2024-06-15 14:00:00+00"), val("2024-01-15")},
	}

	data, err := (&Builder{}).Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewReader(bytes.NewReader(data))
	defer reader.Close()

	row := make(map[string]any)
	if err := reader.Read(&row); err != nil {
		t.Fatal(err)
	}

	// Timestamp stored as int64 microseconds since epoch.
	wantTS := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC).UnixMicro()
	assertEq(t, "ts", row["ts"], wantTS)

	wantTSTZ := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC).UnixMicro()
	assertEq(t, "tstz", row["tstz"], wantTSTZ)

	// Date stored as int32 days since epoch.
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	wantDays := int32(time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).Sub(epoch).Hours() / 24)
	assertEq(t, "birth", row["birth"], wantDays)
}

// TestBuildColumnOrder verifies that field IDs are assigned by declaration
// order, not by alphabetical or map iteration order.
func TestBuildColumnOrder(t *testing.T) {
	cols := []ColumnDef{
		{Name: "zzz", OID: 23}, // would be last alphabetically
		{Name: "aaa", OID: 25},
		{Name: "mmm", OID: 701},
	}
	rows := [][]Value{{val("1"), val("x"), val("1.0")}}

	data, err := (&Builder{}).Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}

	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// Declaration order → field IDs: zzz=1, aaa=2, mmm=3.
	wantIDs := map[string]int{"zzz": 1, "aaa": 2, "mmm": 3}
	for _, child := range file.Root().Columns() {
		if got, want := child.ID(), wantIDs[child.Name()]; got != want {
			t.Errorf("column %q: field_id=%d, want %d", child.Name(), got, want)
		}
	}
}

// TestReadRowsRoundTrip verifies the full COW cycle: Build → ReadRows → filter → Build.
// This is the integration test for the copy-on-write delete path.
func TestReadRowsRoundTrip(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
		{Name: "score", OID: 701},
	}
	original := [][]Value{
		{val("1"), val("alice"), val("3.14")},
		{val("2"), val("bob"), val("2.72")},
		{val("3"), val("carol"), val("1.0")},
		{val("4"), null(), val("0.5")},
	}

	// Step 1: Build parquet from original rows.
	data, err := (&Builder{}).Build(cols, original)
	if err != nil {
		t.Fatal(err)
	}

	// Step 2: Read rows back using ReadRows.
	readBack, err := ReadRows(data, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(readBack) != 4 {
		t.Fatalf("ReadRows returned %d rows, want 4", len(readBack))
	}

	// Verify values survived the round-trip.
	assertVal(t, "row0.id", readBack[0][0], "1")
	assertVal(t, "row0.name", readBack[0][1], "alice")
	assertVal(t, "row1.id", readBack[1][0], "2")
	assertVal(t, "row1.name", readBack[1][1], "bob")
	assertVal(t, "row2.id", readBack[2][0], "3")
	assertVal(t, "row3.id", readBack[3][0], "4")
	if !readBack[3][1].IsNull {
		t.Errorf("row3.name: expected null, got %q", readBack[3][1].Data)
	}

	// Step 3: Simulate COW delete — remove rows with id=2 and id=4.
	deleteKeys := [][]Value{
		{val("2")},
		{val("4")},
	}
	keyColumns := []int{0} // "id" is at position 0
	filtered := filterForTest(readBack, deleteKeys, keyColumns)
	if len(filtered) != 2 {
		t.Fatalf("after delete: got %d rows, want 2", len(filtered))
	}
	assertVal(t, "filtered[0].id", filtered[0][0], "1")
	assertVal(t, "filtered[1].id", filtered[1][0], "3")

	// Step 4: Add a new row (simulating an UPDATE's insert half).
	newRow := []Value{val("5"), val("eve"), val("9.99")}
	combined := append(filtered, newRow)

	// Step 5: Build a new parquet from the combined set.
	data2, err := (&Builder{}).Build(cols, combined)
	if err != nil {
		t.Fatal(err)
	}

	// Step 6: Read back again and verify.
	final, err := ReadRows(data2, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(final) != 3 {
		t.Fatalf("final ReadRows returned %d rows, want 3", len(final))
	}
	assertVal(t, "final[0].id", final[0][0], "1")
	assertVal(t, "final[0].name", final[0][1], "alice")
	assertVal(t, "final[1].id", final[1][0], "3")
	assertVal(t, "final[1].name", final[1][1], "carol")
	assertVal(t, "final[2].id", final[2][0], "5")
	assertVal(t, "final[2].name", final[2][1], "eve")
	assertVal(t, "final[2].score", final[2][2], "9.99")
}

// TestReadRowsAllTypes verifies the ReadRows round-trip for every supported type.
func TestReadRowsAllTypes(t *testing.T) {
	cols := []ColumnDef{
		{Name: "b", OID: 16},    // bool
		{Name: "i2", OID: 21},   // int2
		{Name: "i4", OID: 23},   // int4
		{Name: "i8", OID: 20},   // int8
		{Name: "f4", OID: 700},  // float4
		{Name: "f8", OID: 701},  // float8
		{Name: "s", OID: 25},    // text
		{Name: "d", OID: 1082},  // date
		{Name: "ts", OID: 1114}, // timestamp
	}
	rows := [][]Value{{
		val("t"), val("7"), val("42"), val("9876543210"),
		val("2.5"), val("3.14159"),
		val("hello"),
		val("2024-01-15"),
		val("2024-01-15 10:30:00"),
	}}

	data, err := (&Builder{}).Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}

	readBack, err := ReadRows(data, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(readBack) != 1 {
		t.Fatalf("got %d rows, want 1", len(readBack))
	}
	r := readBack[0]

	assertVal(t, "bool", r[0], "t")
	assertVal(t, "int2", r[1], "7")
	assertVal(t, "int4", r[2], "42")
	assertVal(t, "int8", r[3], "9876543210")
	assertVal(t, "float4", r[4], "2.5")
	assertVal(t, "float8", r[5], "3.14159")
	assertVal(t, "text", r[6], "hello")
	assertVal(t, "date", r[7], "2024-01-15")
	assertVal(t, "timestamp", r[8], "2024-01-15 10:30:00")
}

// filterForTest mirrors the filterDeletedRows logic from writer.go
// so we can test the full pipeline without importing iceberg package.
func filterForTest(rows [][]Value, deletes [][]Value, keyColumns []int) [][]Value {
	type keyStr = string
	deleteSet := make(map[keyStr]bool, len(deletes))
	for _, del := range deletes {
		deleteSet[testKeyString(del)] = true
	}
	var result [][]Value
	for _, row := range rows {
		keyVals := make([]Value, len(keyColumns))
		for i, idx := range keyColumns {
			if idx < len(row) {
				keyVals[i] = row[idx]
			}
		}
		if !deleteSet[testKeyString(keyVals)] {
			result = append(result, row)
		}
	}
	return result
}

func testKeyString(vals []Value) string {
	var s string
	for i, v := range vals {
		if i > 0 {
			s += "\x01"
		}
		if v.IsNull {
			s += "\x00"
		} else {
			s += string(v.Data)
		}
	}
	return s
}

func assertVal(t *testing.T, label string, v Value, want string) {
	t.Helper()
	if v.IsNull {
		t.Errorf("%s: got null, want %q", label, want)
		return
	}
	if got := string(v.Data); got != want {
		t.Errorf("%s: got %q, want %q", label, got, want)
	}
}

func assertEq(t *testing.T, label string, got, want any) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v (%T), want %v (%T)", label, got, got, want, want)
	}
}

// TestCOWDeleteAllRows verifies that deleting every row produces a valid
// 0-row parquet that round-trips cleanly (DuckDB requires a data file even
// for empty tables).
func TestCOWDeleteAllRows(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	original := [][]Value{
		{val("1"), val("alice")},
		{val("2"), val("bob")},
		{val("3"), val("carol")},
	}

	data, err := (&Builder{}).Build(cols, original)
	if err != nil {
		t.Fatal(err)
	}
	readBack, err := ReadRows(data, cols)
	if err != nil {
		t.Fatal(err)
	}

	// Delete all 3 rows.
	deleteKeys := [][]Value{{val("1")}, {val("2")}, {val("3")}}
	filtered := filterForTest(readBack, deleteKeys, []int{0})
	if len(filtered) != 0 {
		t.Fatalf("expected 0 rows after delete-all, got %d", len(filtered))
	}

	// Build a 0-row parquet — must not error.
	data2, err := (&Builder{}).Build(cols, filtered)
	if err != nil {
		t.Fatalf("Build 0 rows: %v", err)
	}
	final, err := ReadRows(data2, cols)
	if err != nil {
		t.Fatalf("ReadRows 0-row file: %v", err)
	}
	if len(final) != 0 {
		t.Errorf("expected 0 rows, got %d", len(final))
	}
}

// TestCOWCompositeKey tests deletes with a multi-column primary key.
func TestCOWCompositeKey(t *testing.T) {
	cols := []ColumnDef{
		{Name: "tenant_id", OID: 23},
		{Name: "user_id", OID: 23},
		{Name: "email", OID: 25},
	}
	original := [][]Value{
		{val("1"), val("10"), val("a@x.com")},
		{val("1"), val("20"), val("b@x.com")},
		{val("2"), val("10"), val("a@y.com")},
		{val("2"), val("20"), val("b@y.com")},
	}

	data, err := (&Builder{}).Build(cols, original)
	if err != nil {
		t.Fatal(err)
	}
	readBack, err := ReadRows(data, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(readBack) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(readBack))
	}

	// Delete (tenant_id=1, user_id=20) and (tenant_id=2, user_id=10).
	// Key columns are positions 0 and 1.
	deleteKeys := [][]Value{
		{val("1"), val("20")},
		{val("2"), val("10")},
	}
	filtered := filterForTest(readBack, deleteKeys, []int{0, 1})
	if len(filtered) != 2 {
		t.Fatalf("expected 2 surviving rows, got %d", len(filtered))
	}

	assertVal(t, "survivor[0].tenant", filtered[0][0], "1")
	assertVal(t, "survivor[0].user", filtered[0][1], "10")
	assertVal(t, "survivor[1].tenant", filtered[1][0], "2")
	assertVal(t, "survivor[1].user", filtered[1][1], "20")
}

// TestCOWUpdateSimulation simulates a full UPDATE cycle:
// old row deleted + new row inserted, then combined and rewritten.
func TestCOWUpdateSimulation(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
		{Name: "score", OID: 701},
	}
	original := [][]Value{
		{val("1"), val("alice"), val("10.0")},
		{val("2"), val("bob"), val("20.0")},
		{val("3"), val("carol"), val("30.0")},
	}

	// Build and read back.
	data, err := (&Builder{}).Build(cols, original)
	if err != nil {
		t.Fatal(err)
	}
	existing, err := ReadRows(data, cols)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate UPDATE id=2: delete old key, insert new row.
	deleteKeys := [][]Value{{val("2")}}
	filtered := filterForTest(existing, deleteKeys, []int{0})
	if len(filtered) != 2 {
		t.Fatalf("after delete id=2: got %d rows, want 2", len(filtered))
	}

	newRow := []Value{val("2"), val("bob_updated"), val("99.0")}
	combined := append(filtered, newRow)

	// Build replacement and verify.
	data2, err := (&Builder{}).Build(cols, combined)
	if err != nil {
		t.Fatal(err)
	}
	final, err := ReadRows(data2, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(final) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(final))
	}

	// Row order: alice, carol, bob_updated
	assertVal(t, "alice.id", final[0][0], "1")
	assertVal(t, "alice.name", final[0][1], "alice")
	assertVal(t, "carol.id", final[1][0], "3")
	assertVal(t, "carol.name", final[1][1], "carol")
	assertVal(t, "bob.id", final[2][0], "2")
	assertVal(t, "bob.name", final[2][1], "bob_updated")
	assertVal(t, "bob.score", final[2][2], "99")
}

// TestCOWMixedOps simulates a flush with multiple deletes and inserts
// (as would happen with several UPDATEs + DELETEs in one batch).
func TestCOWMixedOps(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "val", OID: 25},
	}
	original := [][]Value{
		{val("1"), val("a")},
		{val("2"), val("b")},
		{val("3"), val("c")},
		{val("4"), val("d")},
		{val("5"), val("e")},
	}

	data, err := (&Builder{}).Build(cols, original)
	if err != nil {
		t.Fatal(err)
	}
	existing, err := ReadRows(data, cols)
	if err != nil {
		t.Fatal(err)
	}

	// DELETE id=1, DELETE id=3, UPDATE id=5 (delete+insert)
	deleteKeys := [][]Value{{val("1")}, {val("3")}, {val("5")}}
	filtered := filterForTest(existing, deleteKeys, []int{0})
	if len(filtered) != 2 { // id=2, id=4
		t.Fatalf("after deletes: got %d rows, want 2", len(filtered))
	}

	// New rows: updated id=5 + fresh insert id=6
	newRows := [][]Value{
		{val("5"), val("e_updated")},
		{val("6"), val("f_new")},
	}
	combined := append(filtered, newRows...)

	data2, err := (&Builder{}).Build(cols, combined)
	if err != nil {
		t.Fatal(err)
	}
	final, err := ReadRows(data2, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(final) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(final))
	}

	// Survivors + new rows
	assertVal(t, "row0.id", final[0][0], "2")
	assertVal(t, "row1.id", final[1][0], "4")
	assertVal(t, "row2.id", final[2][0], "5")
	assertVal(t, "row2.val", final[2][1], "e_updated")
	assertVal(t, "row3.id", final[3][0], "6")
	assertVal(t, "row3.val", final[3][1], "f_new")
}

// TestCOWWithNullKeys verifies that null key values are handled correctly
// in the delete filter (they should not match non-null keys).
func TestCOWWithNullKeys(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	original := [][]Value{
		{val("1"), val("alice")},
		{null(), val("ghost")}, // null key
		{val("2"), val("bob")},
	}

	data, err := (&Builder{}).Build(cols, original)
	if err != nil {
		t.Fatal(err)
	}
	existing, err := ReadRows(data, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(existing) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(existing))
	}

	// Delete the null-keyed row.
	deleteKeys := [][]Value{{null()}}
	filtered := filterForTest(existing, deleteKeys, []int{0})
	if len(filtered) != 2 {
		t.Fatalf("after null-key delete: got %d rows, want 2", len(filtered))
	}
	assertVal(t, "survivor[0].id", filtered[0][0], "1")
	assertVal(t, "survivor[1].id", filtered[1][0], "2")
}

// TestCOWMultipleFlushCycles simulates two successive COW flush cycles
// to verify that the read-back → filter → rewrite pipeline works when
// chained (i.e., reading a file that was itself a COW result).
func TestCOWMultipleFlushCycles(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}

	// Cycle 1: start with 3 rows.
	rows1 := [][]Value{
		{val("1"), val("alice")},
		{val("2"), val("bob")},
		{val("3"), val("carol")},
	}
	data1, err := (&Builder{}).Build(cols, rows1)
	if err != nil {
		t.Fatal(err)
	}

	// Cycle 1 COW: delete id=2, add id=4.
	existing1, err := ReadRows(data1, cols)
	if err != nil {
		t.Fatal(err)
	}
	filtered1 := filterForTest(existing1, [][]Value{{val("2")}}, []int{0})
	combined1 := append(filtered1, []Value{val("4"), val("dave")})
	data2, err := (&Builder{}).Build(cols, combined1)
	if err != nil {
		t.Fatal(err)
	}

	// Cycle 2 COW: read the cycle-1 output, delete id=1, update id=3.
	existing2, err := ReadRows(data2, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(existing2) != 3 { // alice, carol, dave
		t.Fatalf("cycle 2 read: got %d rows, want 3", len(existing2))
	}

	filtered2 := filterForTest(existing2, [][]Value{{val("1")}, {val("3")}}, []int{0})
	combined2 := append(filtered2, []Value{val("3"), val("carol_v2")})
	data3, err := (&Builder{}).Build(cols, combined2)
	if err != nil {
		t.Fatal(err)
	}

	final, err := ReadRows(data3, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(final) != 2 { // dave, carol_v2
		t.Fatalf("final: got %d rows, want 2", len(final))
	}
	assertVal(t, "final[0].id", final[0][0], "4")
	assertVal(t, "final[0].name", final[0][1], "dave")
	assertVal(t, "final[1].id", final[1][0], "3")
	assertVal(t, "final[1].name", final[1][1], "carol_v2")
}

// TestCOWDeleteNonExistentKey verifies that deleting a key that doesn't
// exist in the data is a no-op (no panic, no data loss).
func TestCOWDeleteNonExistentKey(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	original := [][]Value{
		{val("1"), val("alice")},
		{val("2"), val("bob")},
	}

	data, err := (&Builder{}).Build(cols, original)
	if err != nil {
		t.Fatal(err)
	}
	existing, err := ReadRows(data, cols)
	if err != nil {
		t.Fatal(err)
	}

	// Delete id=999 which doesn't exist.
	deleteKeys := [][]Value{{val("999")}}
	filtered := filterForTest(existing, deleteKeys, []int{0})
	if len(filtered) != 2 {
		t.Fatalf("expected 2 rows (no-op delete), got %d", len(filtered))
	}
	assertVal(t, "row0", filtered[0][0], "1")
	assertVal(t, "row1", filtered[1][0], "2")
}

// TestReadRowsTimestamptzRoundTrip verifies that timestamptz values
// survive the full Build → ReadRows round-trip.
func TestReadRowsTimestamptzRoundTrip(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "ts", OID: 1114},   // timestamp
		{Name: "tstz", OID: 1184}, // timestamptz
		{Name: "d", OID: 1082},    // date
	}
	rows := [][]Value{{
		val("1"),
		val("2024-01-15 10:30:00"),
		val("2024-06-15 14:00:00+00"),
		val("2024-01-15"),
	}}

	data, err := (&Builder{}).Build(cols, rows)
	if err != nil {
		t.Fatal(err)
	}
	readBack, err := ReadRows(data, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(readBack) != 1 {
		t.Fatalf("got %d rows, want 1", len(readBack))
	}

	assertVal(t, "id", readBack[0][0], "1")
	assertVal(t, "timestamp", readBack[0][1], "2024-01-15 10:30:00")
	assertVal(t, "timestamptz", readBack[0][2], "2024-06-15 14:00:00+00")
	assertVal(t, "date", readBack[0][3], "2024-01-15")
}
