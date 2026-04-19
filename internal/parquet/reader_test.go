package parquet

import (
	"testing"
)

// TestReadRowsExtendedTypes verifies the ReadRows round-trip for types
// not covered by TestReadRowsAllTypes: uuid, json, jsonb, numeric, bytea.
// These all go through the default string path in both parseValue and
// goValueToText, but exercising them catches regressions in type dispatch.
func TestReadRowsExtendedTypes(t *testing.T) {
	cols := []ColumnDef{
		{Name: "u", OID: 2950},  // uuid
		{Name: "j", OID: 114},   // json
		{Name: "jb", OID: 3802}, // jsonb
		{Name: "n", OID: 1700},  // numeric
		{Name: "by", OID: 17},   // bytea
	}
	rows := [][]Value{{
		val("550e8400-e29b-41d4-a716-446655440000"),
		val(`{"key":"value"}`),
		val(`{"nested":{"a":1}}`),
		val("12345.6789"),
		val(`\x48656c6c6f`),
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

	assertVal(t, "uuid", r[0], "550e8400-e29b-41d4-a716-446655440000")
	assertVal(t, "json", r[1], `{"key":"value"}`)
	assertVal(t, "jsonb", r[2], `{"nested":{"a":1}}`)
	assertVal(t, "numeric", r[3], "12345.6789")
	assertVal(t, "bytea", r[4], `\x48656c6c6f`)
}

// TestReadRowsBoundaryValues verifies edge cases: epoch date, zero values,
// max int64, empty string, and all-null rows.
func TestReadRowsBoundaryValues(t *testing.T) {
	t.Run("epoch and zero values", func(t *testing.T) {
		cols := []ColumnDef{
			{Name: "d", OID: 1082},  // date
			{Name: "i8", OID: 20},   // int8
			{Name: "f8", OID: 701},  // float8
			{Name: "s", OID: 25},    // text
		}
		rows := [][]Value{{
			val("1970-01-01"),
			val("0"),
			val("0"),
			val(""),
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
		assertVal(t, "epoch_date", r[0], "1970-01-01")
		assertVal(t, "zero_int8", r[1], "0")
		assertVal(t, "zero_float8", r[2], "0")
		assertVal(t, "empty_string", r[3], "")
	})

	t.Run("large int64", func(t *testing.T) {
		cols := []ColumnDef{
			{Name: "big", OID: 20}, // int8
		}
		rows := [][]Value{{
			val("9223372036854775807"), // max int64
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
		assertVal(t, "max_int64", readBack[0][0], "9223372036854775807")
	})

	t.Run("all null row", func(t *testing.T) {
		cols := []ColumnDef{
			{Name: "a", OID: 23},
			{Name: "b", OID: 25},
			{Name: "c", OID: 701},
		}
		rows := [][]Value{{null(), null(), null()}}

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
		for i, v := range readBack[0] {
			if !v.IsNull {
				t.Errorf("column %d: expected null, got %q", i, v.Data)
			}
		}
	})
}
