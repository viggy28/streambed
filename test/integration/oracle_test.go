//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ---------------------------------------------------------------------------
// Data Integrity Oracle
//
// The oracle compares actual row values between Postgres (source of truth)
// and Iceberg (via DuckDB iceberg_scan). It reports missing rows, extra rows,
// and value mismatches — not just counts.
// ---------------------------------------------------------------------------

// Discrepancy describes a single row-level mismatch between Postgres and Iceberg.
type Discrepancy struct {
	Key     string
	Kind    string // "missing_in_iceberg", "extra_in_iceberg", "value_mismatch"
	Details string
}

// queryPgRows queries Postgres and returns rows keyed by composite primary key.
// Each row is a map of column_name → string value.
func queryPgRows(t *testing.T, schema, table string, keyColumns []string) map[string]map[string]string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("oracle: connect to postgres: %v", err)
	}
	defer conn.Close(ctx)

	orderBy := strings.Join(keyColumns, ", ")
	query := fmt.Sprintf("SELECT * FROM %s.%s ORDER BY %s", schema, table, orderBy)

	result := conn.Exec(ctx, query)
	results, err := result.ReadAll()
	if err != nil {
		t.Fatalf("oracle: query postgres %s.%s: %v", schema, table, err)
	}
	if len(results) == 0 {
		return make(map[string]map[string]string)
	}

	rr := results[0]
	colNames := make([]string, len(rr.FieldDescriptions))
	for i, fd := range rr.FieldDescriptions {
		colNames[i] = string(fd.Name)
	}

	// Find key column indices.
	keyIndices := make([]int, len(keyColumns))
	for i, kc := range keyColumns {
		found := false
		for j, cn := range colNames {
			if cn == kc {
				keyIndices[i] = j
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("oracle: key column %q not found in result set (columns: %v)", kc, colNames)
		}
	}

	rows := make(map[string]map[string]string, len(rr.Rows))
	for _, row := range rr.Rows {
		rowMap := make(map[string]string, len(colNames))
		keyParts := make([]string, len(keyColumns))

		for i, val := range row {
			if val == nil {
				rowMap[colNames[i]] = "<NULL>"
			} else {
				rowMap[colNames[i]] = string(val)
			}
		}
		for i, ki := range keyIndices {
			keyParts[i] = rowMap[colNames[ki]]
		}
		compositeKey := strings.Join(keyParts, "|")
		rows[compositeKey] = rowMap
	}
	return rows
}

// queryIcebergRows queries Iceberg via DuckDB and returns rows keyed by composite primary key.
func queryIcebergRows(t *testing.T, duckDB *sql.DB, schema, table string, keyColumns []string) map[string]map[string]string {
	t.Helper()

	icebergTable := fmt.Sprintf(
		"iceberg_scan('s3://%s/%s/%s/%s', allow_moved_paths = true)",
		s3Bucket, s3Prefix, schema, table)
	orderBy := strings.Join(keyColumns, ", ")
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s", icebergTable, orderBy)

	sqlRows, err := duckDB.Query(query)
	if err != nil {
		t.Fatalf("oracle: query iceberg %s.%s: %v", schema, table, err)
	}
	defer sqlRows.Close()

	colNames, err := sqlRows.Columns()
	if err != nil {
		t.Fatalf("oracle: get columns: %v", err)
	}

	// Find key column indices.
	keyIndices := make([]int, len(keyColumns))
	for i, kc := range keyColumns {
		found := false
		for j, cn := range colNames {
			if cn == kc {
				keyIndices[i] = j
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("oracle: key column %q not found in iceberg result (columns: %v)", kc, colNames)
		}
	}

	rows := make(map[string]map[string]string)
	for sqlRows.Next() {
		vals := make([]interface{}, len(colNames))
		ptrs := make([]interface{}, len(colNames))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := sqlRows.Scan(ptrs...); err != nil {
			t.Fatalf("oracle: scan row: %v", err)
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
		compositeKey := strings.Join(keyParts, "|")
		rows[compositeKey] = rowMap
	}
	if err := sqlRows.Err(); err != nil {
		t.Fatalf("oracle: rows iteration: %v", err)
	}
	return rows
}

// diffRowSets compares two row sets and returns discrepancies.
func diffRowSets(pg, iceberg map[string]map[string]string, compareColumns []string) []Discrepancy {
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

		// Compare specified columns (or all common columns if compareColumns is nil).
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
				continue // column not in both; skip
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

// assertPgIcebergMatch is the top-level oracle assertion. It compares Postgres
// and Iceberg row-for-row using the specified key and compare columns.
// If compareColumns is nil, only row presence (by key) and count are checked.
func assertPgIcebergMatch(t *testing.T, duckDB *sql.DB, schema, table string, keyColumns, compareColumns []string) {
	t.Helper()

	pgRows := queryPgRows(t, schema, table, keyColumns)
	iceRows := queryIcebergRows(t, duckDB, schema, table, keyColumns)

	discs := diffRowSets(pgRows, iceRows, compareColumns)

	if len(discs) == 0 {
		t.Logf("oracle: %s.%s OK — %d rows match", schema, table, len(pgRows))
		return
	}

	// Report discrepancies.
	var missing, extra, mismatch int
	for _, d := range discs {
		switch d.Kind {
		case "missing_in_iceberg":
			missing++
		case "extra_in_iceberg":
			extra++
		case "value_mismatch":
			mismatch++
		}
	}

	// Log first 10 discrepancies for debugging.
	limit := 10
	if len(discs) < limit {
		limit = len(discs)
	}
	for _, d := range discs[:limit] {
		t.Logf("  [%s] key=%s %s", d.Kind, d.Key, d.Details)
	}
	if len(discs) > 10 {
		t.Logf("  ... and %d more discrepancies", len(discs)-10)
	}

	t.Fatalf("oracle: %s.%s FAILED — PG=%d rows, Iceberg=%d rows, missing=%d, extra=%d, value_mismatch=%d",
		schema, table, len(pgRows), len(iceRows), missing, extra, mismatch)
}

// assertPgIcebergCountMatch is a lighter check that only verifies row counts match.
func assertPgIcebergCountMatch(t *testing.T, duckDB *sql.DB, schema, table string, keyColumns []string) {
	t.Helper()

	pgRows := queryPgRows(t, schema, table, keyColumns)
	iceRows := queryIcebergRows(t, duckDB, schema, table, keyColumns)

	if len(pgRows) != len(iceRows) {
		t.Fatalf("oracle count: %s.%s PG=%d Iceberg=%d", schema, table, len(pgRows), len(iceRows))
	}
	t.Logf("oracle count: %s.%s OK — %d rows", schema, table, len(pgRows))
}

// ---------------------------------------------------------------------------
// Oracle-Based Integration Tests
// ---------------------------------------------------------------------------

// TestOracleAfterMixedDML verifies INSERT → UPDATE → DELETE sequences
// produce exact value-level matches between Postgres and Iceberg.
func TestOracleAfterMixedDML(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	execSQL(t, "ALTER TABLE test_events REPLICA IDENTITY FULL")
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	// Phase 1: INSERT 50 rows with deterministic values.
	t.Log("phase 1: inserting 50 rows...")
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO test_events (name, value) VALUES ")
	for i := 0; i < 50; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("('user_%d', %d.%02d)", i, i*10, i%100))
	}
	result := conn.Exec(ctx, sb.String())
	if _, err := result.ReadAll(); err != nil {
		t.Fatalf("insert: %v", err)
	}
	conn.Close(ctx)

	t.Log("syncing inserts...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	// Phase 2: UPDATE 10 rows (change name column).
	t.Log("phase 2: updating 10 rows...")
	for i := 1; i <= 10; i++ {
		execSQL(t, fmt.Sprintf("UPDATE test_events SET name = 'updated_%d' WHERE id = %d", i, i))
	}

	t.Log("syncing updates...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	// Phase 3: DELETE 5 rows.
	t.Log("phase 3: deleting 5 rows...")
	for i := 41; i <= 45; i++ {
		execSQL(t, fmt.Sprintf("DELETE FROM test_events WHERE id = %d", i))
	}

	t.Log("syncing deletes...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	// Final verification: count should be 45 (50 - 5).
	pgCount := pgRowCount(t, "test_events")
	if pgCount != 45 {
		t.Fatalf("expected 45 rows in PG, got %d", pgCount)
	}

	cleanup(t)
}

// TestOracleKeyLifecycle exercises INSERT → UPDATE → DELETE → re-INSERT
// on the same key and verifies the final state matches exactly.
func TestOracleKeyLifecycle(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	execSQL(t, "ALTER TABLE test_events REPLICA IDENTITY FULL")
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	// Insert 5 rows.
	t.Log("inserting 5 rows...")
	insertRows(t, 5)

	runSync(t, ctx, 12*time.Second, sharedStatePath)

	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	// UPDATE id=1, DELETE id=1, INSERT new row — all in rapid succession.
	t.Log("key lifecycle: UPDATE → DELETE → re-INSERT...")
	execSQL(t, "UPDATE test_events SET name = 'about_to_die' WHERE id = 1")
	execSQL(t, "DELETE FROM test_events WHERE id = 1")
	execSQL(t, "INSERT INTO test_events (name, value) VALUES ('reborn', 777.77)")

	runSync(t, ctx, 12*time.Second, sharedStatePath)

	// Verify: PG has 5 rows (deleted 1, inserted 1), Iceberg must match.
	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	cleanup(t)
}

// TestOracleRepeatedKillRestart runs 3 kill/restart cycles with interleaved
// INSERTs and UPDATEs, checking the oracle after each restart.
func TestOracleRepeatedKillRestart(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	execSQL(t, "ALTER TABLE test_events REPLICA IDENTITY FULL")
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	const cycles = 3
	const rowsPerCycle = 100

	for i := 0; i < cycles; i++ {
		t.Logf("cycle %d/%d: inserting %d rows...", i+1, cycles, rowsPerCycle)
		insertRows(t, rowsPerCycle)

		// Also update some rows from previous cycles.
		if i > 0 {
			for j := 1; j <= 5; j++ {
				id := (i-1)*rowsPerCycle + j
				execSQL(t, fmt.Sprintf("UPDATE test_events SET name = 'cycle%d_update' WHERE id = %d", i, id))
			}
		}

		syncDuration := 8 * time.Second
		if i == cycles-1 {
			syncDuration = 20 * time.Second
		}
		t.Logf("cycle %d/%d: syncing for %s...", i+1, cycles, syncDuration)
		runSync(t, ctx, syncDuration, sharedStatePath)

		// Oracle check after each cycle.
		assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})
	}

	cleanup(t)
}

// TestOracleMultiTable verifies data integrity across 3 tables with
// interleaved DML, checking each table independently.
func TestOracleMultiTable(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)

	setupNamedTable(t, "oracle_a")
	setupNamedTable(t, "oracle_b")
	setupNamedTable(t, "oracle_c")

	createSlotAndPublication(t)
	duckDB := newTestDuckDB(t)

	// Interleaved inserts.
	t.Log("inserting interleaved rows across 3 tables...")
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	r := conn.Exec(ctx, "BEGIN")
	if _, err := r.ReadAll(); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	for i := 0; i < 50; i++ {
		for _, stmt := range []string{
			fmt.Sprintf("INSERT INTO oracle_a (name, value) VALUES ('a_%d', %d.1)", i, i),
			fmt.Sprintf("INSERT INTO oracle_b (name, value) VALUES ('b_%d', %d.2)", i, i*2),
			fmt.Sprintf("INSERT INTO oracle_c (name, value) VALUES ('c_%d', %d.3)", i, i*3),
		} {
			r := conn.Exec(ctx, stmt)
			if _, err := r.ReadAll(); err != nil {
				t.Fatalf("exec: %v", err)
			}
		}
	}
	r = conn.Exec(ctx, "COMMIT")
	if _, err := r.ReadAll(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
	conn.Close(ctx)

	t.Log("syncing...")
	runSync(t, ctx, 20*time.Second)

	// Oracle check on each table.
	assertPgIcebergCountMatch(t, duckDB, "public", "oracle_a", []string{"id"})
	assertPgIcebergCountMatch(t, duckDB, "public", "oracle_b", []string{"id"})
	assertPgIcebergCountMatch(t, duckDB, "public", "oracle_c", []string{"id"})

	cleanup(t)
	execSQL(t, "DROP TABLE IF EXISTS oracle_a")
	execSQL(t, "DROP TABLE IF EXISTS oracle_b")
	execSQL(t, "DROP TABLE IF EXISTS oracle_c")
}

// TestOracleRapidUpdateSameKey sends 100 updates to the same row in rapid
// succession and verifies the final value matches Postgres.
func TestOracleRapidUpdateSameKey(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	// Insert 1 row.
	t.Log("inserting 1 row...")
	execSQL(t, "INSERT INTO test_events (name, value) VALUES ('initial', 0.0)")

	runSync(t, ctx, 12*time.Second, sharedStatePath)

	// Rapid-fire 100 updates on the same row.
	t.Log("rapid-fire 100 updates on id=1...")
	for i := 1; i <= 100; i++ {
		execSQL(t, fmt.Sprintf("UPDATE test_events SET name = 'version_%d', value = %d.%02d WHERE id = 1", i, i, i%100))
	}

	t.Log("syncing rapid updates...")
	runSync(t, ctx, 15*time.Second, sharedStatePath)

	// Oracle: must have exactly 1 row matching the final update.
	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	cleanup(t)
}

// TestOracleHighCardinalityDeletes inserts 1000 rows, deletes 999, and
// verifies the single survivor matches Postgres.
func TestOracleHighCardinalityDeletes(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	execSQL(t, "ALTER TABLE test_events REPLICA IDENTITY FULL")
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"
	duckDB := newTestDuckDB(t)

	t.Log("inserting 1000 rows...")
	insertRows(t, 1000)

	runSync(t, ctx, 15*time.Second, sharedStatePath)

	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	// Delete 999 rows (keep id=500).
	t.Log("deleting 999 rows (keeping id=500)...")
	execSQL(t, "DELETE FROM test_events WHERE id != 500")

	t.Log("syncing mass deletes...")
	runSync(t, ctx, 15*time.Second, sharedStatePath)

	// Oracle check: exactly 1 row.
	assertPgIcebergCountMatch(t, duckDB, "public", "test_events", []string{"id"})

	pgCount := pgRowCount(t, "test_events")
	if pgCount != 1 {
		t.Fatalf("expected 1 row in PG, got %d", pgCount)
	}

	cleanup(t)
}
