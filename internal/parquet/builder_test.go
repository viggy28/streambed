package parquet

import (
	"bytes"
	"testing"

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
	_, err := b.Build(cols, nil)
	if err == nil {
		t.Error("expected error for no rows")
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
