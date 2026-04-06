package wal

import (
	"bytes"
	"strings"
	"testing"
)

func TestUnescapeCopyField_Null(t *testing.T) {
	got, isNull, err := unescapeCopyField([]byte(`\N`))
	if err != nil {
		t.Fatal(err)
	}
	if !isNull {
		t.Fatal("expected NULL")
	}
	if got != nil {
		t.Errorf("expected nil data for NULL, got %v", got)
	}
}

func TestUnescapeCopyField_Plain(t *testing.T) {
	got, isNull, err := unescapeCopyField([]byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if isNull {
		t.Fatal("unexpected NULL")
	}
	if string(got) != "hello world" {
		t.Errorf("got %q", got)
	}
}

func TestUnescapeCopyField_Escapes(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{`a\tb`, "a\tb"},
		{`line1\nline2`, "line1\nline2"},
		{`\\slash`, `\slash`},
		{`tab\there`, "tab\there"},
		{`\r\n`, "\r\n"},
		{`\b\f\v`, "\b\f\v"},
		{`lit \N not null`, "lit N not null"}, // \N only acts as NULL for full-field match
	}
	for _, c := range cases {
		got, isNull, err := unescapeCopyField([]byte(c.in))
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		if isNull {
			t.Errorf("%q: unexpected NULL", c.in)
			continue
		}
		if string(got) != c.want {
			t.Errorf("%q: got %q want %q", c.in, got, c.want)
		}
	}
}

func TestUnescapeCopyField_HexEscape(t *testing.T) {
	got, _, err := unescapeCopyField([]byte(`\x41\x42\x43`))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "ABC" {
		t.Errorf("got %q", got)
	}
}

func TestUnescapeCopyField_OctalEscape(t *testing.T) {
	// 101 oct = 65 = 'A'
	got, _, err := unescapeCopyField([]byte(`\101`))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "A" {
		t.Errorf("got %q", got)
	}
}

func TestUnescapeCopyField_UnknownEscapeIsLiteral(t *testing.T) {
	got, _, err := unescapeCopyField([]byte(`\q`))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "q" {
		t.Errorf("got %q", got)
	}
}

func TestParseCopyTextLine_Basic(t *testing.T) {
	fields, err := parseCopyTextLine([]byte("1\talice\t\\N\t42"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 4 {
		t.Fatalf("expected 4 fields, got %d", len(fields))
	}
	if string(fields[0].Data) != "1" || fields[0].IsNull {
		t.Errorf("field 0: %+v", fields[0])
	}
	if string(fields[1].Data) != "alice" || fields[1].IsNull {
		t.Errorf("field 1: %+v", fields[1])
	}
	if !fields[2].IsNull {
		t.Errorf("field 2: expected NULL, got %+v", fields[2])
	}
	if string(fields[3].Data) != "42" || fields[3].IsNull {
		t.Errorf("field 3: %+v", fields[3])
	}
}

func TestParseCopyTextLine_EmptyFields(t *testing.T) {
	// empty string (non-null) → two consecutive tabs
	fields, err := parseCopyTextLine([]byte("a\t\tb"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(fields))
	}
	if fields[1].IsNull || len(fields[1].Data) != 0 {
		t.Errorf("middle field should be empty non-null, got %+v", fields[1])
	}
}

func TestParseCopyTextLine_EscapedTabStaysInField(t *testing.T) {
	// An escaped \t inside a field must not be treated as a separator.
	fields, err := parseCopyTextLine([]byte(`a\tb` + "\t" + "c"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d: %v", len(fields), fields)
	}
	if string(fields[0].Data) != "a\tb" {
		t.Errorf("field 0: got %q", fields[0].Data)
	}
	if string(fields[1].Data) != "c" {
		t.Errorf("field 1: got %q", fields[1].Data)
	}
}

func TestParseCopyTextStream_MultiRow(t *testing.T) {
	input := strings.Join([]string{
		"1\talice\t\\N",
		"2\tbob\thi",
		`3\tx` + "\t" + `esc\nline` + "\t" + `\N`, // row 3 has literal \t in first col
	}, "\n")
	var rows [][]copyTextField
	err := parseCopyTextStream(bytes.NewReader([]byte(input)), func(f []copyTextField) error {
		// Copy because parser may share memory across rows.
		clone := make([]copyTextField, len(f))
		for i, fld := range f {
			if fld.IsNull {
				clone[i] = fld
				continue
			}
			b := make([]byte, len(fld.Data))
			copy(b, fld.Data)
			clone[i] = copyTextField{Data: b}
		}
		rows = append(rows, clone)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// Row 0: "1","alice", NULL
	if string(rows[0][0].Data) != "1" || string(rows[0][1].Data) != "alice" || !rows[0][2].IsNull {
		t.Errorf("row 0: %v", rows[0])
	}
	// Row 2: first field has an escaped \t → "3\tx"
	if string(rows[2][0].Data) != "3\tx" {
		t.Errorf("row 2 col 0: got %q", rows[2][0].Data)
	}
	// Row 2: second field has escaped \n → "esc\nline"
	if string(rows[2][1].Data) != "esc\nline" {
		t.Errorf("row 2 col 1: got %q", rows[2][1].Data)
	}
	if !rows[2][2].IsNull {
		t.Errorf("row 2 col 2: expected NULL")
	}
}

func TestParseCopyTextStream_Empty(t *testing.T) {
	var calls int
	err := parseCopyTextStream(bytes.NewReader(nil), func(f []copyTextField) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Errorf("expected 0 rows, got %d", calls)
	}
}
