package parquet

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"
)

// ReadRows reads a Parquet file and returns rows as Value slices,
// converting typed Parquet values back to the text-format bytes that
// pgoutput would produce. This is the inverse of Build.
func ReadRows(data []byte, columns []ColumnDef) ([][]Value, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}

	// The generic reader needs an explicit schema when using map[string]any
	// because it cannot infer one from the type. Reuse the same schema
	// builder the writer uses so types match exactly.
	schema := parquet.NewSchema("streambed", schemaToGroup(columns))
	reader := parquet.NewGenericReader[map[string]interface{}](f, schema)
	defer reader.Close()

	count := int(reader.NumRows())
	if count == 0 {
		return nil, nil
	}

	// Read all rows in one batch. Each map must be pre-initialized
	// because parquet-go assigns into them via reflection.
	batch := make([]map[string]interface{}, count)
	for i := range batch {
		batch[i] = make(map[string]interface{})
	}
	n, err := reader.Read(batch)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read parquet rows: %w", err)
	}

	result := make([][]Value, n)
	for i := 0; i < n; i++ {
		row := make([]Value, len(columns))
		for j, col := range columns {
			v, ok := batch[i][col.Name]
			if !ok || v == nil {
				row[j] = Value{IsNull: true}
				continue
			}
			text, err := goValueToText(col.OID, v)
			if err != nil {
				return nil, fmt.Errorf("row %d col %s: %w", i, col.Name, err)
			}
			row[j] = Value{Data: text}
		}
		result[i] = row
	}
	return result, nil
}

// goValueToText converts a typed Go value (as returned by parquet-go)
// back to the text-format bytes that Postgres pgoutput would emit.
func goValueToText(oid uint32, v interface{}) ([]byte, error) {
	switch oid {
	case 16: // bool
		b, ok := v.(bool)
		if !ok {
			return []byte(fmt.Sprintf("%v", v)), nil
		}
		if b {
			return []byte("t"), nil
		}
		return []byte("f"), nil

	case 21, 23: // int2, int4 → stored as int32
		switch val := v.(type) {
		case int32:
			return []byte(strconv.FormatInt(int64(val), 10)), nil
		case int64:
			return []byte(strconv.FormatInt(val, 10)), nil
		default:
			return []byte(fmt.Sprintf("%v", v)), nil
		}

	case 20: // int8 → stored as int64
		switch val := v.(type) {
		case int64:
			return []byte(strconv.FormatInt(val, 10)), nil
		case int32:
			return []byte(strconv.FormatInt(int64(val), 10)), nil
		default:
			return []byte(fmt.Sprintf("%v", v)), nil
		}

	case 700: // float4
		switch val := v.(type) {
		case float32:
			return []byte(strconv.FormatFloat(float64(val), 'f', -1, 32)), nil
		case float64:
			return []byte(strconv.FormatFloat(val, 'f', -1, 32)), nil
		default:
			return []byte(fmt.Sprintf("%v", v)), nil
		}

	case 701: // float8
		switch val := v.(type) {
		case float64:
			return []byte(strconv.FormatFloat(val, 'f', -1, 64)), nil
		case float32:
			return []byte(strconv.FormatFloat(float64(val), 'f', -1, 64)), nil
		default:
			return []byte(fmt.Sprintf("%v", v)), nil
		}

	case 1082: // date — parquet stores as int32 (days since epoch)
		days, ok := v.(int32)
		if !ok {
			return []byte(fmt.Sprintf("%v", v)), nil
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		t := epoch.AddDate(0, 0, int(days))
		return []byte(t.Format("2006-01-02")), nil

	case 1114: // timestamp — parquet stores as int64 (microseconds since epoch)
		micros, ok := v.(int64)
		if !ok {
			return []byte(fmt.Sprintf("%v", v)), nil
		}
		t := time.UnixMicro(micros).UTC()
		return []byte(t.Format("2006-01-02 15:04:05.999999")), nil

	case 1184: // timestamptz — parquet stores as int64 (microseconds since epoch)
		micros, ok := v.(int64)
		if !ok {
			return []byte(fmt.Sprintf("%v", v)), nil
		}
		t := time.UnixMicro(micros).UTC()
		return []byte(t.Format("2006-01-02 15:04:05.999999+00")), nil

	default: // string, varchar, uuid, json, jsonb, numeric, bytea
		switch s := v.(type) {
		case string:
			return []byte(s), nil
		case []byte:
			return s, nil
		default:
			return []byte(fmt.Sprintf("%v", v)), nil
		}
	}
}
