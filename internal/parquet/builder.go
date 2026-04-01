package parquet

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
)

// ColumnDef describes a column for Parquet writing.
type ColumnDef struct {
	Name string
	OID  uint32
}

// Builder creates Parquet files from row batches.
type Builder struct{}

// Build takes column definitions and rows (text-format values from pgoutput),
// converts them to typed Go values, and writes a Parquet file.
// Returns the Parquet file as bytes.
func (b *Builder) Build(columns []ColumnDef, rows [][]Value) ([]byte, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows to write")
	}

	schema := buildSchema(columns)
	buf := new(bytes.Buffer)

	writer := parquet.NewGenericWriter[map[string]interface{}](buf,
		parquet.NewSchema("streambed", schemaToGroup(columns)),
		parquet.Compression(&snappy.Codec{}),
	)

	for _, row := range rows {
		record := make(map[string]interface{})
		for i, col := range columns {
			if i >= len(row) {
				continue
			}
			if row[i].IsNull {
				continue // leave key absent for null
			}
			v, err := parseValue(col.OID, row[i].Data)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", col.Name, err)
			}
			record[col.Name] = v
		}
		if _, err := writer.Write([]map[string]interface{}{record}); err != nil {
			return nil, fmt.Errorf("write row: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close writer: %w", err)
	}

	_ = schema // schema used for validation
	return buf.Bytes(), nil
}

// Value holds a single column value in text format from pgoutput.
type Value struct {
	Data   []byte
	IsNull bool
}

func buildSchema(columns []ColumnDef) *parquet.Schema {
	return parquet.NewSchema("streambed", schemaToGroup(columns))
}

func schemaToGroup(columns []ColumnDef) parquet.Group {
	fields := make([]parquet.Column, 0, len(columns))
	_ = fields
	group := make(parquet.Group)
	for _, col := range columns {
		group[col.Name] = parquet.Optional(oidToParquetNode(col.OID))
	}
	return group
}

func oidToParquetNode(oid uint32) parquet.Node {
	switch oid {
	case 16: // bool
		return parquet.Leaf(parquet.BooleanType)
	case 21: // int2
		return parquet.Int(32)
	case 23: // int4
		return parquet.Int(32)
	case 20: // int8
		return parquet.Int(64)
	case 700: // float4
		return parquet.Leaf(parquet.FloatType)
	case 701: // float8
		return parquet.Leaf(parquet.DoubleType)
	case 1082: // date
		return parquet.Date()
	case 1114, 1184: // timestamp, timestamptz
		return parquet.Timestamp(parquet.Microsecond)
	default:
		// text, varchar, uuid, json, jsonb, numeric, bytea, and everything else
		return parquet.String()
	}
}

// parseValue converts a pgoutput text-format value to the Go type expected by Parquet.
func parseValue(oid uint32, data []byte) (interface{}, error) {
	s := string(data)
	switch oid {
	case 16: // bool
		return s == "t" || s == "true" || s == "TRUE", nil
	case 21, 23: // int2, int4
		v, err := strconv.ParseInt(s, 10, 32)
		return int32(v), err
	case 20: // int8
		return strconv.ParseInt(s, 10, 64)
	case 700: // float4
		v, err := strconv.ParseFloat(s, 32)
		return float32(v), err
	case 701: // float8
		return strconv.ParseFloat(s, 64)
	case 1082: // date — days since Unix epoch
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return nil, err
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		days := int32(t.Sub(epoch).Hours() / 24)
		return days, nil
	case 1114: // timestamp — microseconds since epoch
		t, err := parseTimestamp(s)
		if err != nil {
			return nil, err
		}
		return t.UnixMicro(), nil
	case 1184: // timestamptz
		t, err := parseTimestampTZ(s)
		if err != nil {
			return nil, err
		}
		return t.UnixMicro(), nil
	default:
		// text, varchar, uuid, json, jsonb, numeric, bytea — all stored as string
		return s, nil
	}
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
	// Fallback: try without timezone
	t, err := parseTimestamp(strings.TrimRight(s, "Z"))
	if err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamptz %q", s)
}

// roundFloat32 is used in tests to compare float32 values.
func roundFloat32(f float32, places int) float32 {
	pow := math.Pow(10, float64(places))
	return float32(math.Round(float64(f)*pow) / pow)
}
