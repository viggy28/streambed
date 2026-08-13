//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/viggy28/streambed/internal/ducklake"
	"github.com/viggy28/streambed/internal/pipeline"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/wal"
)

func TestDuckLakeEndToEndInsertUpdateDelete(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	t.Cleanup(func() { cleanup(t) })
	setupNamedTable(t, "ducklake_ops")
	execSQL(t, "ALTER TABLE ducklake_ops REPLICA IDENTITY FULL")

	statePath := t.TempDir() + "/state.db"
	catalogPath := t.TempDir() + "/ducklake-catalog.sqlite"
	dataPath := fmt.Sprintf("s3://%s/%s/ducklake-%d/", s3Bucket, s3Prefix, time.Now().UnixNano())

	createSlotAndPublication(t)
	insertNamedRows(t, "ducklake_ops", 100)
	runDuckLakeSync(t, ctx, 12*time.Second, statePath, catalogPath, dataPath)

	execSQL(t, "UPDATE ducklake_ops SET name = 'ducklake_updated', value = 42.42 WHERE id BETWEEN 1 AND 30")
	execSQL(t, "DELETE FROM ducklake_ops WHERE id BETWEEN 31 AND 45")
	insertNamedRows(t, "ducklake_ops", 25)
	runDuckLakeSync(t, ctx, 12*time.Second, statePath, catalogPath, dataPath)

	duckDB := newDuckLakeReader(t, catalogPath, dataPath)
	defer duckDB.Close()
	assertPgDuckLakeMatch(t, duckDB, "public", "ducklake_ops", []string{"id"}, []string{"id", "name", "value"})
}

func TestDuckLakeSchemaEvolutionAddColumn(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	t.Cleanup(func() { cleanup(t) })
	setupNamedTable(t, "ducklake_schema")

	statePath := t.TempDir() + "/state.db"
	catalogPath := t.TempDir() + "/ducklake-catalog.sqlite"
	dataPath := fmt.Sprintf("s3://%s/%s/ducklake-schema-%d/", s3Bucket, s3Prefix, time.Now().UnixNano())

	createSlotAndPublication(t)
	insertNamedRows(t, "ducklake_schema", 10)
	runDuckLakeSync(t, ctx, 12*time.Second, statePath, catalogPath, dataPath)

	execSQL(t, "ALTER TABLE ducklake_schema ADD COLUMN extra_info TEXT")
	execSQL(t, "INSERT INTO ducklake_schema (name, value, extra_info) VALUES ('after_schema', 9.99, 'extra')")
	runDuckLakeSync(t, ctx, 12*time.Second, statePath, catalogPath, dataPath)

	duckDB := newDuckLakeReader(t, catalogPath, dataPath)
	defer duckDB.Close()
	var got string
	if err := duckDB.QueryRow(`SELECT extra_info FROM "streambed"."public"."ducklake_schema" WHERE name = 'after_schema'`).Scan(&got); err != nil {
		t.Fatalf("query evolved ducklake table: %v", err)
	}
	if got != "extra" {
		t.Fatalf("got extra_info=%q, want extra", got)
	}
}

func runDuckLakeSync(t *testing.T, ctx context.Context, duration time.Duration, statePath, catalogPath, dataPath string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	stateStore, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open state store: %v", err)
	}
	defer stateStore.Close()

	writer, err := ducklake.NewWriter(ctx, ducklake.Config{
		CatalogPath: catalogPath,
		DataPath:    dataPath,
		S3Endpoint:  minioEndpoint,
		S3Region:    s3Region,
	}, stateStore, flushRows, 5*time.Second, logger)
	if err != nil {
		t.Fatalf("create ducklake writer: %v", err)
	}
	defer writer.Close()

	pgConn, err := pgconn.Connect(ctx, pgReplConnStr())
	if err != nil {
		t.Fatalf("connect to postgres for replication: %v", err)
	}
	defer pgConn.Close(context.Background())

	if err := wal.CreatePublication(ctx, pgConn, slotName, nil, logger); err != nil {
		t.Fatalf("create publication: %v", err)
	}
	slotLSN, err := wal.CreateOrReuseSlot(ctx, pgConn, slotName, logger)
	if err != nil {
		t.Fatalf("setup replication slot: %v", err)
	}

	tableFlushLSN := make(map[string]pglogrepl.LSN)
	registeredTables, err := stateStore.GetRegisteredTables()
	if err != nil {
		t.Fatalf("get registered tables: %v", err)
	}
	for _, rt := range registeredTables {
		lsnStr, found, err := writer.GetTableFlushLSN(ctx, rt.Schema, rt.Table)
		if err != nil || !found {
			continue
		}
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			continue
		}
		tableFlushLSN[fmt.Sprintf("%s.%s", rt.Schema, rt.Table)] = lsn
	}

	startLSN := slotLSN
	for _, lsn := range tableFlushLSN {
		if lsn > startLSN {
			startLSN = lsn
		}
	}

	metaConn, err := pgx.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect metadata: %v", err)
	}
	defer metaConn.Close(context.Background())

	p := pipeline.New(pgConn, slotName, slotName, startLSN, nil,
		logger, stateStore, tableFlushLSN, writer, 5*time.Second, wal.NewMetadataQuerier(metaConn))

	syncCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	pipelineErr := p.Run(syncCtx)
	if pipelineErr != nil && syncCtx.Err() != nil {
		t.Logf("pipeline stopped: %v", pipelineErr)
	} else if pipelineErr != nil {
		t.Fatalf("unexpected pipeline error: %v", pipelineErr)
	}
}

func newDuckLakeReader(t *testing.T, catalogPath, dataPath string) *sql.DB {
	t.Helper()
	db, err := ducklake.Open(context.Background(), ducklake.Config{
		CatalogPath: catalogPath,
		DataPath:    dataPath,
		S3Endpoint:  minioEndpoint,
		S3Region:    s3Region,
	})
	if err != nil {
		t.Fatalf("open ducklake reader: %v", err)
	}
	return db
}

func assertPgDuckLakeMatch(t *testing.T, duckDB *sql.DB, schema, table string, keyColumns, compareColumns []string) {
	t.Helper()
	pgRows := queryPgRows(t, schema, table, keyColumns)
	dlRows := queryDuckLakeRows(t, duckDB, schema, table, keyColumns)
	discs := diffRowSets(pgRows, dlRows, compareColumns)
	if len(discs) == 0 {
		t.Logf("ducklake oracle: %s.%s OK — %d rows match", schema, table, len(pgRows))
		return
	}
	limit := len(discs)
	if limit > 10 {
		limit = 10
	}
	for _, d := range discs[:limit] {
		t.Logf("  [%s] key=%s %s", d.Kind, d.Key, d.Details)
	}
	t.Fatalf("ducklake oracle: %s.%s FAILED — discrepancies=%d", schema, table, len(discs))
}

func queryDuckLakeRows(t *testing.T, duckDB *sql.DB, schema, table string, keyColumns []string) map[string]map[string]string {
	t.Helper()
	orderBy := strings.Join(keyColumns, ", ")
	query := fmt.Sprintf(`SELECT * FROM "streambed".%s.%s ORDER BY %s`, quoteIdentLocal(schema), quoteIdentLocal(table), orderBy)
	sqlRows, err := duckDB.Query(query)
	if err != nil {
		t.Fatalf("oracle: query ducklake %s.%s: %v", schema, table, err)
	}
	defer sqlRows.Close()

	colNames, err := sqlRows.Columns()
	if err != nil {
		t.Fatalf("oracle: ducklake columns: %v", err)
	}
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
			t.Fatalf("oracle: key column %q not found in ducklake result (columns: %v)", kc, colNames)
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
			t.Fatalf("oracle: scan ducklake row: %v", err)
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
		if err := addUniqueOracleRow(rows, compositeKey, rowMap); err != nil {
			t.Fatalf("oracle: %v", err)
		}
	}
	if err := sqlRows.Err(); err != nil {
		t.Fatalf("oracle: ducklake rows iteration: %v", err)
	}
	return rows
}

func quoteIdentLocal(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
