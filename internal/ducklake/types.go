package ducklake

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/viggy28/streambed/internal/wal"
)

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func qname(catalog, schema, table string) string {
	return quoteIdent(catalog) + "." + quoteIdent(schema) + "." + quoteIdent(table)
}

func pgOIDToDuckDBType(oid uint32) string {
	switch oid {
	case 16:
		return "BOOLEAN"
	case 21:
		return "SMALLINT"
	case 23:
		return "INTEGER"
	case 20:
		return "BIGINT"
	case 700:
		return "REAL"
	case 701:
		return "DOUBLE"
	case 1082:
		return "DATE"
	case 1114:
		return "TIMESTAMP"
	case 1184:
		return "TIMESTAMPTZ"
	case 2950:
		return "UUID"
	case 17:
		return "BLOB"
	default:
		return "VARCHAR"
	}
}

func pgOIDToDuckDBAppenderType(oid uint32) duckdb.Type {
	switch oid {
	case 16:
		return duckdb.TYPE_BOOLEAN
	case 21:
		return duckdb.TYPE_SMALLINT
	case 23:
		return duckdb.TYPE_INTEGER
	case 20:
		return duckdb.TYPE_BIGINT
	case 700:
		return duckdb.TYPE_FLOAT
	case 701:
		return duckdb.TYPE_DOUBLE
	case 1082:
		return duckdb.TYPE_DATE
	case 1114:
		return duckdb.TYPE_TIMESTAMP
	case 1184:
		return duckdb.TYPE_TIMESTAMP_TZ
	case 2950:
		return duckdb.TYPE_UUID
	case 17:
		return duckdb.TYPE_BLOB
	default:
		return duckdb.TYPE_VARCHAR
	}
}

func typeInfos(columns []wal.Column) ([]duckdb.TypeInfo, error) {
	infos := make([]duckdb.TypeInfo, len(columns))
	for i, col := range columns {
		info, err := duckdb.NewTypeInfo(pgOIDToDuckDBAppenderType(col.OID))
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		infos[i] = info
	}
	return infos, nil
}

func columnNames(columns []wal.Column) []string {
	names := make([]string, len(columns))
	for i, c := range columns {
		names[i] = c.Name
	}
	return names
}

func parseValue(oid uint32, data []byte) (any, error) {
	s := string(data)
	switch oid {
	case 16:
		return s == "t" || s == "true" || s == "TRUE", nil
	case 21:
		v, err := strconv.ParseInt(s, 10, 16)
		return int16(v), err
	case 23:
		v, err := strconv.ParseInt(s, 10, 32)
		return int32(v), err
	case 20:
		return strconv.ParseInt(s, 10, 64)
	case 700:
		v, err := strconv.ParseFloat(s, 32)
		return float32(v), err
	case 701:
		return strconv.ParseFloat(s, 64)
	case 1082:
		return time.Parse("2006-01-02", s)
	case 1114:
		return parseTimestamp(s)
	case 1184:
		return parseTimestampTZ(s)
	case 2950:
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, err
		}
		var out duckdb.UUID
		copy(out[:], id[:])
		return out, nil
	case 17:
		return parseBytea(s)
	default:
		return s, nil
	}
}

func parseBytea(s string) ([]byte, error) {
	if strings.HasPrefix(s, `\x`) || strings.HasPrefix(s, `\X`) {
		return hex.DecodeString(s[2:])
	}
	return []byte(s), nil
}

func parseTimestamp(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp %q", s)
}

func parseTimestampTZ(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05.999999+00",
		"2006-01-02 15:04:05+00",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC(), nil
		}
	}
	t, err := parseTimestamp(strings.TrimRight(s, "Z"))
	if err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamptz %q", s)
}

func columnDDL(columns []wal.Column) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		parts[i] = fmt.Sprintf("%s %s", quoteIdent(c.Name), pgOIDToDuckDBType(c.OID))
	}
	return strings.Join(parts, ", ")
}

func columnList(columns []wal.Column) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		parts[i] = quoteIdent(c.Name)
	}
	return strings.Join(parts, ", ")
}

func keyColumns(columns []wal.Column, keyIdx []int) []wal.Column {
	out := make([]wal.Column, 0, len(keyIdx))
	for _, idx := range keyIdx {
		if idx >= 0 && idx < len(columns) {
			out = append(out, columns[idx])
		}
	}
	return out
}

func keyPredicate(tableAlias, keyAlias string, keys []wal.Column) string {
	parts := make([]string, len(keys))
	for i, c := range keys {
		col := quoteIdent(c.Name)
		parts[i] = fmt.Sprintf("%s.%s IS NOT DISTINCT FROM %s.%s", tableAlias, col, keyAlias, col)
	}
	return strings.Join(parts, " AND ")
}

func columnsFingerprint(columns []wal.Column) string {
	var b strings.Builder
	for _, c := range columns {
		b.WriteString(c.Name)
		b.WriteByte(':')
		b.WriteString(strconv.FormatUint(uint64(c.OID), 10))
		b.WriteByte(';')
	}
	return b.String()
}

func dedupRowsByKey(columns []wal.Column, keyIdx []int, rows [][]cell) [][]cell {
	if len(rows) <= 1 || len(keyIdx) == 0 {
		return rows
	}
	seen := make(map[string]int, len(rows))
	out := make([][]cell, 0, len(rows))
	for _, row := range rows {
		key := rowKey(columns, keyIdx, row)
		if idx, ok := seen[key]; ok {
			out[idx] = row
			continue
		}
		seen[key] = len(out)
		out = append(out, row)
	}
	return out
}

func dedupCellRows(rows [][]cell) [][]cell {
	if len(rows) <= 1 {
		return rows
	}
	seen := make(map[string]int, len(rows))
	out := make([][]cell, 0, len(rows))
	for _, row := range rows {
		key := cellsKey(row)
		if idx, ok := seen[key]; ok {
			out[idx] = row
			continue
		}
		seen[key] = len(out)
		out = append(out, row)
	}
	return out
}

func rowKey(columns []wal.Column, keyIdx []int, row []cell) string {
	parts := make([]cell, 0, len(keyIdx))
	for _, idx := range keyIdx {
		if idx >= 0 && idx < len(columns) && idx < len(row) {
			parts = append(parts, row[idx])
		}
	}
	return cellsKey(parts)
}

func cellsKey(row []cell) string {
	var b strings.Builder
	for _, c := range row {
		if c.IsNull {
			b.WriteString("n;")
			continue
		}
		b.WriteString(strconv.Itoa(len(c.Data)))
		b.WriteByte(':')
		b.WriteString(string(c.Data))
		b.WriteByte(';')
	}
	return b.String()
}
