// Package oracle compares Postgres rows to Iceberg rows and reports
// discrepancies. Postgres is the source of truth: any difference is a
// Streambed correctness issue.
//
// The oracle is intentionally simple: read all rows from both sides, key them
// by primary key, and diff. It is extracted from the integration oracle
// (test/integration/oracle_test.go) with the *testing.T dependency removed
// so it can be used by the continuous simtest harness.
package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// Discrepancy describes a single row-level mismatch between Postgres and Iceberg.
type Discrepancy struct {
	Key     string
	Kind    string // "missing_in_iceberg", "extra_in_iceberg", "value_mismatch"
	Details string
}

// DiffReport summarizes one oracle comparison.
type DiffReport struct {
	Schema        string
	Table         string
	PgRowCount    int
	IceRowCount   int
	Discrepancies []Discrepancy
}

// Counts returns per-kind discrepancy counts.
func (r DiffReport) Counts() (missing, extra, mismatch int) {
	for _, d := range r.Discrepancies {
		switch d.Kind {
		case "missing_in_iceberg":
			missing++
		case "extra_in_iceberg":
			extra++
		case "value_mismatch":
			mismatch++
		}
	}
	return
}

// QueryPgRows reads all rows from a Postgres table and returns them keyed by
// the composite primary key. Each row is a map from column name to stringified
// value. NULL becomes the literal "<NULL>" so key lookups stay simple.
//
// The caller owns the connection lifecycle.
func QueryPgRows(ctx context.Context, conn *pgconn.PgConn, schema, table string, keyColumns []string) (map[string]map[string]string, error) {
	orderBy := strings.Join(keyColumns, ", ")
	query := fmt.Sprintf("SELECT * FROM %s.%s ORDER BY %s", schema, table, orderBy)

	result := conn.Exec(ctx, query)
	results, err := result.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("oracle: query postgres %s.%s: %w", schema, table, err)
	}
	if len(results) == 0 {
		return make(map[string]map[string]string), nil
	}

	rr := results[0]
	colNames := make([]string, len(rr.FieldDescriptions))
	for i, fd := range rr.FieldDescriptions {
		colNames[i] = string(fd.Name)
	}

	keyIndices, err := columnIndices(colNames, keyColumns)
	if err != nil {
		return nil, fmt.Errorf("oracle: %w (table %s.%s)", err, schema, table)
	}

	rows := make(map[string]map[string]string, len(rr.Rows))
	for _, row := range rr.Rows {
		rowMap := make(map[string]string, len(colNames))
		for i, val := range row {
			if val == nil {
				rowMap[colNames[i]] = "<NULL>"
			} else {
				rowMap[colNames[i]] = string(val)
			}
		}
		keyParts := make([]string, len(keyColumns))
		for i, ki := range keyIndices {
			keyParts[i] = rowMap[colNames[ki]]
		}
		rows[strings.Join(keyParts, "|")] = rowMap
	}
	return rows, nil
}

// QueryIcebergRows reads all rows from an Iceberg table via DuckDB's iceberg_scan
// and returns them keyed the same way as QueryPgRows. The DuckDB connection
// must already have the iceberg and httpfs extensions loaded and S3
// credentials configured.
func QueryIcebergRows(ctx context.Context, duckDB *sql.DB, schema, table string, keyColumns []string, s3Bucket, s3Prefix string) (map[string]map[string]string, error) {
	icebergTable := fmt.Sprintf(
		"iceberg_scan('s3://%s/%s/%s/%s', allow_moved_paths = true)",
		s3Bucket, s3Prefix, schema, table)
	orderBy := strings.Join(keyColumns, ", ")
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s", icebergTable, orderBy)

	sqlRows, err := duckDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("oracle: query iceberg %s.%s: %w", schema, table, err)
	}
	defer sqlRows.Close()

	colNames, err := sqlRows.Columns()
	if err != nil {
		return nil, fmt.Errorf("oracle: get columns for %s.%s: %w", schema, table, err)
	}

	keyIndices, err := columnIndices(colNames, keyColumns)
	if err != nil {
		return nil, fmt.Errorf("oracle: %w (iceberg %s.%s)", err, schema, table)
	}

	rows := make(map[string]map[string]string)
	for sqlRows.Next() {
		vals := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := sqlRows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("oracle: scan iceberg row for %s.%s: %w", schema, table, err)
		}

		rowMap := make(map[string]string, len(colNames))
		for i, v := range vals {
			if v == nil {
				rowMap[colNames[i]] = "<NULL>"
			} else {
				rowMap[colNames[i]] = fmt.Sprintf("%v", v)
			}
		}
		keyParts := make([]string, len(keyColumns))
		for i, ki := range keyIndices {
			keyParts[i] = rowMap[colNames[ki]]
		}
		rows[strings.Join(keyParts, "|")] = rowMap
	}
	if err := sqlRows.Err(); err != nil {
		return nil, fmt.Errorf("oracle: iteration error for %s.%s: %w", schema, table, err)
	}
	return rows, nil
}

// DiffRowSets compares Postgres and Iceberg row sets and returns discrepancies.
// If compareColumns is nil, every column present in both rows is compared.
func DiffRowSets(pg, iceberg map[string]map[string]string, compareColumns []string) []Discrepancy {
	var discs []Discrepancy

	for key, pgRow := range pg {
		iceRow, ok := iceberg[key]
		if !ok {
			discs = append(discs, Discrepancy{
				Key:     key,
				Kind:    "missing_in_iceberg",
				Details: fmt.Sprintf("PG row: %v", pgRow),
			})
			continue
		}

		cols := compareColumns
		if cols == nil {
			cols = make([]string, 0, len(pgRow))
			for c := range pgRow {
				cols = append(cols, c)
			}
			sort.Strings(cols)
		}

		for _, col := range cols {
			pgVal, pgOK := pgRow[col]
			iceVal, iceOK := iceRow[col]
			if !pgOK || !iceOK {
				continue
			}
			if pgVal != iceVal {
				discs = append(discs, Discrepancy{
					Key:     key,
					Kind:    "value_mismatch",
					Details: fmt.Sprintf("column %q: PG=%q Iceberg=%q", col, pgVal, iceVal),
				})
			}
		}
	}

	for key := range iceberg {
		if _, ok := pg[key]; !ok {
			discs = append(discs, Discrepancy{
				Key:  key,
				Kind: "extra_in_iceberg",
			})
		}
	}

	return discs
}

func columnIndices(colNames, keyColumns []string) ([]int, error) {
	indices := make([]int, len(keyColumns))
	for i, kc := range keyColumns {
		found := false
		for j, cn := range colNames {
			if cn == kc {
				indices[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("key column %q not found (columns: %v)", kc, colNames)
		}
	}
	return indices, nil
}
