package wal

import (
	"bufio"
	"fmt"
	"io"
)

// Postgres COPY TO STDOUT (default TEXT format) emits one row per line, with
// columns separated by TAB. Special characters are backslash-escaped and the
// token \N represents NULL. See:
// https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.2
//
// This parser is deliberately small: it only needs to handle what Postgres
// emits, not every edge case a COPY FROM producer might generate.

// copyTextField is the decoded value for a single column in one COPY row.
type copyTextField struct {
	// Data holds the raw bytes as they would have appeared in pgoutput's
	// text format — i.e. the unescaped column payload. For NULL, Data is
	// nil and IsNull is true.
	Data   []byte
	IsNull bool
}

// parseCopyTextStream reads COPY TO STDOUT (text format) output line-by-line
// and invokes rowFn for each parsed row. It stops at EOF or the first parse
// error. The number of fields per row is not checked here — callers should
// validate against their expected column count.
func parseCopyTextStream(r io.Reader, rowFn func(fields []copyTextField) error) error {
	scanner := bufio.NewScanner(r)
	// COPY output lines can be large (wide rows, long text columns).
	// Start with 64 KiB but allow growth up to 16 MiB per line.
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 16*1024*1024)

	var rowNum int
	for scanner.Scan() {
		rowNum++
		line := scanner.Bytes()
		fields, err := parseCopyTextLine(line)
		if err != nil {
			return fmt.Errorf("parse row %d: %w", rowNum, err)
		}
		if err := rowFn(fields); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// parseCopyTextLine splits one COPY TEXT-format line into its fields and
// unescapes each one.
func parseCopyTextLine(line []byte) ([]copyTextField, error) {
	// Count TABs to size the output slice (fields = tabs + 1).
	fieldCount := 1
	for _, b := range line {
		if b == '\t' {
			fieldCount++
		}
	}

	out := make([]copyTextField, 0, fieldCount)
	var (
		cur   []byte
		start = 0
	)
	// Pre-allocate cur with reasonable capacity; grown on demand.
	cur = make([]byte, 0, 64)

	flush := func() error {
		decoded, isNull, err := unescapeCopyField(cur)
		if err != nil {
			return err
		}
		out = append(out, copyTextField{Data: decoded, IsNull: isNull})
		cur = cur[:0]
		return nil
	}

	for i := 0; i < len(line); i++ {
		b := line[i]
		if b == '\t' {
			if err := flush(); err != nil {
				return nil, err
			}
			start = i + 1
			continue
		}
		cur = append(cur, b)
	}
	_ = start
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

// unescapeCopyField decodes a single COPY TEXT field. Returns (data, isNull, err).
//
// The literal two-byte sequence `\N` is NULL. Any other backslash sequence is
// an escape:
//
//	\b \f \n \r \t \v → control bytes
//	\\                 → single backslash
//	\x<hex hex>        → one byte from two hex digits
//	\<digit digit digit> → one byte from three octal digits
//
// A backslash followed by anything else is treated as a literal of the
// following character (Postgres permissive behaviour).
func unescapeCopyField(raw []byte) ([]byte, bool, error) {
	if len(raw) == 2 && raw[0] == '\\' && raw[1] == 'N' {
		return nil, true, nil
	}
	// Fast path: no backslashes → return as-is (share backing array; callers
	// that need to retain the slice across rows must copy).
	hasEscape := false
	for _, b := range raw {
		if b == '\\' {
			hasEscape = true
			break
		}
	}
	if !hasEscape {
		// Copy so the caller can safely retain the slice after the scanner
		// reuses its buffer on the next line.
		out := make([]byte, len(raw))
		copy(out, raw)
		return out, false, nil
	}

	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if b != '\\' {
			out = append(out, b)
			continue
		}
		// Backslash — need at least one more byte.
		if i+1 >= len(raw) {
			// Trailing backslash: treat as literal.
			out = append(out, '\\')
			continue
		}
		next := raw[i+1]
		switch next {
		case 'b':
			out = append(out, '\b')
			i++
		case 'f':
			out = append(out, '\f')
			i++
		case 'n':
			out = append(out, '\n')
			i++
		case 'r':
			out = append(out, '\r')
			i++
		case 't':
			out = append(out, '\t')
			i++
		case 'v':
			out = append(out, '\v')
			i++
		case '\\':
			out = append(out, '\\')
			i++
		case 'x', 'X':
			// \x<hex><hex>
			if i+3 >= len(raw) {
				return nil, false, fmt.Errorf("truncated hex escape")
			}
			v, err := hexByte(raw[i+2], raw[i+3])
			if err != nil {
				return nil, false, err
			}
			out = append(out, v)
			i += 3
		case '0', '1', '2', '3', '4', '5', '6', '7':
			// \<oct><oct><oct>
			if i+3 >= len(raw) {
				return nil, false, fmt.Errorf("truncated octal escape")
			}
			v, err := octByte(raw[i+1], raw[i+2], raw[i+3])
			if err != nil {
				return nil, false, err
			}
			out = append(out, v)
			i += 3
		default:
			// Unknown escape: emit the escaped byte literally.
			out = append(out, next)
			i++
		}
	}
	return out, false, nil
}

func hexByte(a, b byte) (byte, error) {
	h, err := hexDigit(a)
	if err != nil {
		return 0, err
	}
	l, err := hexDigit(b)
	if err != nil {
		return 0, err
	}
	return h<<4 | l, nil
}

func hexDigit(b byte) (byte, error) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', nil
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, nil
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, nil
	}
	return 0, fmt.Errorf("invalid hex digit %q", b)
}

func octByte(a, b, c byte) (byte, error) {
	for _, d := range [3]byte{a, b, c} {
		if d < '0' || d > '7' {
			return 0, fmt.Errorf("invalid octal digit %q", d)
		}
	}
	return ((a-'0')<<6 | (b-'0')<<3 | (c - '0')), nil
}
