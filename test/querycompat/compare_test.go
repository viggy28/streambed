//go:build integration

package querycompat

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestEqualFloatSpecialValues(t *testing.T) {
	if err := equalValue(701, []byte("NaN"), []byte("1"), 0); err == nil {
		t.Fatal("one-sided NaN must not compare equal")
	}
	if err := equalValue(701, []byte("Infinity"), []byte("-Infinity"), 0); err == nil {
		t.Fatal("opposite infinities must not compare equal")
	}
	if err := equalValue(701, []byte("NaN"), []byte("NaN"), 0); err != nil {
		t.Fatalf("matching NaN values should compare equal: %v", err)
	}
}

type resultColumn struct {
	Name string
	OID  uint32
}

type queryResult struct {
	Columns []resultColumn
	Rows    [][][]byte
}

func runQueryCase(t *testing.T, ctx context.Context, source, streambed *pgx.Conn, target targetFormat, tc queryCase) {
	t.Helper()
	behavior := tc.behavior(target)

	sourceResult, err := executeQuery(ctx, source, tc.Query)
	if err != nil {
		t.Fatalf("source Postgres rejected oracle query:\n%s\nerror: %v", tc.Query, err)
	}
	streambedResult, streambedErr := executeQuery(ctx, streambed, tc.Query)

	switch behavior.Expectation {
	case unsupported:
		if streambedErr == nil {
			t.Fatalf("query unexpectedly became supported; update its classification\nquery: %s", tc.Query)
		}
		if !regexp.MustCompile(behavior.ErrorMatch).MatchString(streambedErr.Error()) {
			t.Fatalf("Streambed error did not match %q\nquery: %s\nerror: %v", behavior.ErrorMatch, tc.Query, streambedErr)
		}
		return
	case supported, knownDifference:
		if streambedErr != nil {
			t.Fatalf("Streambed query failed\ntarget: %s\nquery: %s\nerror: %v", target, tc.Query, streambedErr)
		}
	default:
		t.Fatalf("invalid expectation %q", behavior.Expectation)
	}

	ignored := make(map[int]bool, len(behavior.IgnoreOIDs))
	for _, index := range behavior.IgnoreOIDs {
		ignored[index] = true
	}
	metadataDifference := compareColumns(t, tc.Query, sourceResult.Columns, streambedResult.Columns, ignored)
	if behavior.Expectation == knownDifference && !metadataDifference {
		t.Fatalf("known difference no longer differs; reclassify the case\ntarget: %s\nreason: %s\nquery: %s", target, behavior.Reason, tc.Query)
	}
	compareRows(t, tc, sourceResult, streambedResult)
}

func executeQuery(ctx context.Context, conn *pgx.Conn, query string) (queryResult, error) {
	rows, err := conn.Query(ctx, query, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return queryResult{}, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	result := queryResult{Columns: make([]resultColumn, len(fields))}
	for i, field := range fields {
		result.Columns[i] = resultColumn{Name: field.Name, OID: field.DataTypeOID}
	}
	for rows.Next() {
		raw := rows.RawValues()
		row := make([][]byte, len(raw))
		for i := range raw {
			if raw[i] != nil {
				row[i] = bytes.Clone(raw[i])
			}
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return queryResult{}, err
	}
	return result, nil
}

func compareColumns(t *testing.T, query string, source, streambed []resultColumn, ignored map[int]bool) bool {
	t.Helper()
	if len(source) != len(streambed) {
		t.Fatalf("column count mismatch\nquery: %s\nPostgres: %v\nStreambed: %v", query, source, streambed)
	}
	difference := false
	for i := range source {
		if source[i].Name != streambed[i].Name {
			t.Fatalf("column name mismatch at index %d\nquery: %s\nPostgres: %q\nStreambed: %q", i, query, source[i].Name, streambed[i].Name)
		}
		if source[i].OID != streambed[i].OID {
			if ignored[i] {
				difference = true
				continue
			}
			t.Fatalf("column OID mismatch for %q at index %d\nquery: %s\nPostgres: %d\nStreambed: %d", source[i].Name, i, query, source[i].OID, streambed[i].OID)
		}
	}
	for index := range ignored {
		if index < 0 || index >= len(source) {
			t.Fatalf("ignored OID index %d is outside %d result columns for query %s", index, len(source), query)
		}
	}
	return difference
}

func compareRows(t *testing.T, tc queryCase, source, streambed queryResult) {
	t.Helper()
	if len(source.Rows) != len(streambed.Rows) {
		t.Fatalf("row count mismatch\nquery: %s\nPostgres: %d\nStreambed: %d", tc.Query, len(source.Rows), len(streambed.Rows))
	}

	sourceRows := source.Rows
	streambedRows := streambed.Rows
	if !tc.Ordered {
		sourceRows = sortedRows(t, source.Columns, sourceRows)
		streambedRows = sortedRows(t, source.Columns, streambedRows)
	}
	for rowIndex := range sourceRows {
		if len(sourceRows[rowIndex]) != len(streambedRows[rowIndex]) {
			t.Fatalf("row width mismatch at row %d\nquery: %s", rowIndex, tc.Query)
		}
		for columnIndex := range sourceRows[rowIndex] {
			if err := equalValue(source.Columns[columnIndex].OID, sourceRows[rowIndex][columnIndex], streambedRows[rowIndex][columnIndex], tc.FloatEpsilon); err != nil {
				t.Fatalf("value mismatch at row %d, column %d (%s, OID %d)\nquery: %s\nPostgres: %q\nStreambed: %q\nreason: %v",
					rowIndex, columnIndex, source.Columns[columnIndex].Name, source.Columns[columnIndex].OID,
					tc.Query, sourceRows[rowIndex][columnIndex], streambedRows[rowIndex][columnIndex], err)
			}
		}
	}
}

func sortedRows(t *testing.T, columns []resultColumn, rows [][][]byte) [][][]byte {
	t.Helper()
	result := append([][][]byte(nil), rows...)
	sort.Slice(result, func(i, j int) bool {
		return canonicalRow(columns, result[i]) < canonicalRow(columns, result[j])
	})
	return result
}

func canonicalRow(columns []resultColumn, row [][]byte) string {
	parts := make([]string, len(row))
	for i, value := range row {
		canonical, err := canonicalValue(columns[i].OID, value)
		if err != nil {
			canonical = string(value)
		}
		parts[i] = canonical
	}
	return strings.Join(parts, "\x00")
}

func equalValue(oid uint32, source, streambed []byte, epsilon float64) error {
	if source == nil || streambed == nil {
		if source == nil && streambed == nil {
			return nil
		}
		return fmt.Errorf("NULL mismatch")
	}
	if oid == 700 || oid == 701 {
		a, err := strconv.ParseFloat(string(source), 64)
		if err != nil {
			return fmt.Errorf("parse Postgres float: %w", err)
		}
		b, err := strconv.ParseFloat(string(streambed), 64)
		if err != nil {
			return fmt.Errorf("parse Streambed float: %w", err)
		}
		if math.IsNaN(a) || math.IsNaN(b) {
			if math.IsNaN(a) && math.IsNaN(b) {
				return nil
			}
			return fmt.Errorf("NaN mismatch: %g != %g", a, b)
		}
		if math.IsInf(a, 0) || math.IsInf(b, 0) {
			if a == b {
				return nil
			}
			return fmt.Errorf("infinity mismatch: %g != %g", a, b)
		}
		if epsilon == 0 {
			if oid == 700 {
				epsilon = 1e-6
			} else {
				epsilon = 1e-12
			}
		}
		if math.Abs(a-b) > epsilon {
			return fmt.Errorf("float difference %g exceeds epsilon %g", math.Abs(a-b), epsilon)
		}
		return nil
	}
	a, err := canonicalValue(oid, source)
	if err != nil {
		return fmt.Errorf("normalize Postgres value: %w", err)
	}
	b, err := canonicalValue(oid, streambed)
	if err != nil {
		return fmt.Errorf("normalize Streambed value: %w", err)
	}
	if a != b {
		return fmt.Errorf("normalized values differ: %q != %q", a, b)
	}
	return nil
}

func canonicalValue(oid uint32, value []byte) (string, error) {
	if value == nil {
		return "<NULL>", nil
	}
	s := string(value)
	switch oid {
	case 16:
		switch strings.ToLower(s) {
		case "t", "true":
			return "true", nil
		case "f", "false":
			return "false", nil
		default:
			return "", fmt.Errorf("invalid boolean %q", s)
		}
	case 20, 21, 23:
		n := new(big.Int)
		if _, ok := n.SetString(s, 10); !ok {
			return "", fmt.Errorf("invalid integer %q", s)
		}
		return n.String(), nil
	case 1700:
		n := new(big.Rat)
		if _, ok := n.SetString(s); !ok {
			return "", fmt.Errorf("invalid numeric %q", s)
		}
		return n.RatString(), nil
	case 1082:
		parsed, err := scanTime(oid, value)
		if err != nil {
			return "", err
		}
		return parsed.Format("2006-01-02"), nil
	case 1114:
		parsed, err := scanTime(oid, value)
		if err != nil {
			return "", err
		}
		return parsed.Format("2006-01-02T15:04:05.999999999"), nil
	case 1184:
		parsed, err := scanTime(oid, value)
		if err != nil {
			return "", err
		}
		return parsed.UTC().Format(time.RFC3339Nano), nil
	case 2950:
		return strings.ToLower(s), nil
	case 17:
		decoded, err := decodeBytea(s)
		if err != nil {
			return "", err
		}
		return hex.EncodeToString(decoded), nil
	case 114, 3802:
		var decoded any
		if err := json.Unmarshal(value, &decoded); err != nil {
			return "", fmt.Errorf("invalid JSON: %w", err)
		}
		canonical, err := json.Marshal(decoded)
		if err != nil {
			return "", err
		}
		return string(canonical), nil
	default:
		return s, nil
	}
}

func scanTime(oid uint32, value []byte) (time.Time, error) {
	var result time.Time
	if err := pgtype.NewMap().Scan(oid, pgtype.TextFormatCode, value, &result); err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q for OID %d: %w", value, oid, err)
	}
	return result, nil
}

func decodeBytea(value string) ([]byte, error) {
	if strings.HasPrefix(value, `\x`) {
		return hex.DecodeString(value[2:])
	}
	var result []byte
	if err := pgtype.NewMap().Scan(17, pgtype.TextFormatCode, []byte(value), &result); err != nil {
		return nil, err
	}
	return result, nil
}
