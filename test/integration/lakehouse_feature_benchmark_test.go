//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/viggy28/streambed/internal/ducklake"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/pipeline"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/storage"
	"github.com/viggy28/streambed/internal/wal"
)

type lakehouseBenchTarget struct {
	Name            string `json:"name"`
	Format          string `json:"format"`
	DuckLakeCatalog string `json:"ducklake_catalog,omitempty"`
	IcebergMutation string `json:"iceberg_mutation,omitempty"`
}

type lakehouseBenchScenario struct {
	Name         string `json:"name"`
	BaselineRows int    `json:"baseline_rows"`
	InsertRows   int    `json:"insert_rows"`
	UpdateRows   int    `json:"update_rows"`
	DeleteRows   int    `json:"delete_rows"`
	FlushRows    int    `json:"flush_rows"`
}

type lakehouseBenchHandle struct {
	writer      pipeline.Writer
	closeWriter func()
	openQuery   func() (*sql.DB, string, func())
}

type lakehouseBenchQueryResult struct {
	Name     string `json:"name"`
	MedianMS int64  `json:"median_ms"`
	P95MS    int64  `json:"p95_ms"`
}

type lakehouseBenchResult struct {
	Target                 lakehouseBenchTarget        `json:"target"`
	Scenario               lakehouseBenchScenario      `json:"scenario"`
	BaselineLoadDurationMS int64                       `json:"baseline_load_duration_ms"`
	WriteDurationMS        int64                       `json:"write_duration_ms"`
	FlushCount             int                         `json:"flush_count"`
	FinalRows              int64                       `json:"final_rows"`
	DataObjects            int                         `json:"data_objects"`
	DeleteObjects          int                         `json:"delete_objects"`
	TotalObjects           int                         `json:"total_objects"`
	TotalObjectBytes       int64                       `json:"total_object_bytes"`
	Queries                []lakehouseBenchQueryResult `json:"queries"`
}

type lakehouseBenchReport struct {
	SchemaVersion int                    `json:"schema_version"`
	Metadata      benchmarkMetadata      `json:"metadata"`
	QueryRuns     int                    `json:"query_runs"`
	Results       []lakehouseBenchResult `json:"results"`
	Caveats       []string               `json:"caveats"`
}

type lakeBenchAggregate struct {
	Rows, DistinctIDs, SumIDs, SumVersion, SumAmount int64
}

// TestLakehouseFeatureBenchmark is a feature-validation benchmark for the full
// write + read loop. It writes deterministic CDC-like events directly through
// each lakehouse writer, then times representative DuckDB reads against the
// produced lake. Run with:
//
//	STREAMBED_RUN_LAKEHOUSE_FEATURE_BENCH=1 go test -tags integration -run TestLakehouseFeatureBenchmark -count=1 ./test/integration
//
// Optional knobs:
//
//	STREAMBED_LAKEHOUSE_BENCH_ROWS, STREAMBED_LAKEHOUSE_BENCH_QUERY_RUNS,
//	STREAMBED_LAKEHOUSE_BENCH_OUTPUT, STREAMBED_LAKEHOUSE_BENCH_INCLUDE_MOR=1
func TestLakehouseFeatureBenchmark(t *testing.T) {
	if os.Getenv("STREAMBED_RUN_LAKEHOUSE_FEATURE_BENCH") != "1" {
		t.Skip("set STREAMBED_RUN_LAKEHOUSE_FEATURE_BENCH=1 to run lakehouse feature benchmark")
	}
	skipIfMinIOUnavailable(t)

	baselineRows := envInt(t, "STREAMBED_LAKEHOUSE_BENCH_ROWS", 10_000, 100)
	queryRuns := envInt(t, "STREAMBED_LAKEHOUSE_BENCH_QUERY_RUNS", 7, 3)
	flushRowsList := envIntList(t, "STREAMBED_LAKEHOUSE_BENCH_FLUSH_ROWS", []int{100, 500, 1000, 10000}, 1)
	mutationRows := max(1, baselineRows/10)
	var scenarios []lakehouseBenchScenario
	for _, flushRows := range flushRowsList {
		scenarios = append(scenarios,
			lakehouseBenchScenario{Name: "append", BaselineRows: baselineRows, InsertRows: mutationRows, FlushRows: flushRows},
			lakehouseBenchScenario{Name: "update-10pct", BaselineRows: baselineRows, UpdateRows: mutationRows, FlushRows: flushRows},
			lakehouseBenchScenario{Name: "delete-10pct", BaselineRows: baselineRows, DeleteRows: mutationRows, FlushRows: flushRows},
			lakehouseBenchScenario{Name: "mixed-append-update-delete", BaselineRows: baselineRows, InsertRows: mutationRows, UpdateRows: mutationRows, DeleteRows: mutationRows, FlushRows: flushRows},
		)
	}
	targets := []lakehouseBenchTarget{
		{Name: "iceberg-cow", Format: "iceberg", IcebergMutation: string(iceberg.MutationModeCOW)},
		{Name: "ducklake-sqlite", Format: "ducklake", DuckLakeCatalog: "sqlite"},
		{Name: "ducklake-duckdb", Format: "ducklake", DuckLakeCatalog: "duckdb"},
	}
	if os.Getenv("STREAMBED_LAKEHOUSE_BENCH_INCLUDE_MOR") == "1" {
		targets = append(targets, lakehouseBenchTarget{Name: "iceberg-mor", Format: "iceberg", IcebergMutation: string(iceberg.MutationModeMOR)})
	}

	ctx := context.Background()
	var results []lakehouseBenchResult
	for _, scenario := range scenarios {
		for _, target := range targets {
			t.Run(fmt.Sprintf("%s/%s/flush=%d", target.Name, scenario.Name, scenario.FlushRows), func(t *testing.T) {
				result := runLakehouseFeatureBenchSample(t, ctx, target, scenario, queryRuns)
				results = append(results, result)
			})
		}
	}

	report := lakehouseBenchReport{SchemaVersion: 1, Metadata: collectBenchmarkMetadata(1, 0, queryRuns), QueryRuns: queryRuns, Results: results, Caveats: []string{
		"This benchmark bypasses Postgres logical replication and exercises writer + catalog commit + DuckDB reads only.",
		"Baseline load is performed as one bulk flush and reported separately; write_duration_ms measures only the scenario workload at the configured flush_rows.",
		"Iceberg and DuckLake write data to local MinIO; DuckLake catalog is local SQLite or DuckDB as configured.",
		"Read timings include DuckDB query execution over the produced lake but do not include psql-wire server overhead.",
	}}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("lakehouse feature benchmark JSON:\n%s", encoded)
	if out := os.Getenv("STREAMBED_LAKEHOUSE_BENCH_OUTPUT"); out != "" {
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(out, append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func runLakehouseFeatureBenchSample(t *testing.T, ctx context.Context, target lakehouseBenchTarget, scenario lakehouseBenchScenario, queryRuns int) lakehouseBenchResult {
	t.Helper()
	objectStore, err := storage.NewS3Client(ctx, s3Bucket, s3Region, minioEndpoint)
	if err != nil {
		t.Fatalf("create S3 client: %v", err)
	}
	samplePrefix := path.Join(s3Prefix, "lakehouse-feature-bench", fmt.Sprintf("%d", time.Now().UnixNano()), target.Name, scenario.Name)
	cleanupObjectPrefix(t, ctx, objectStore, samplePrefix)

	stateStore, err := state.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	defer stateStore.Close()

	logger := slog.New(slog.NewTextHandler(noopWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
	model := newLakeBenchModel()
	catalogPath := duckLakeBenchCatalogPath(t, target)

	baselineHandle := newLakehouseBenchTarget(t, ctx, target, objectStore, stateStore, samplePrefix, catalogPath, scenario.BaselineRows+1, logger)
	baselineStarted := time.Now()
	writeBaselineEvents(t, ctx, baselineHandle.writer, scenario.BaselineRows, scenario.BaselineRows+1, model)
	if err := baselineHandle.writer.FlushAll(ctx); err != nil {
		t.Fatalf("baseline flush: %v", err)
	}
	baselineDuration := time.Since(baselineStarted)
	baselineHandle.closeWriter()

	handle := newLakehouseBenchTarget(t, ctx, target, objectStore, stateStore, samplePrefix, catalogPath, scenario.FlushRows, logger)
	started := time.Now()
	if scenario.InsertRows > 0 || scenario.UpdateRows > 0 || scenario.DeleteRows > 0 {
		writeMutationEvents(t, ctx, handle.writer, scenario, model)
	}
	if err := handle.writer.FlushAll(ctx); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	writeDuration := time.Since(started)
	handle.closeWriter()

	queryDB, tableExpr, closeQuery := handle.openQuery()
	defer closeQuery()
	expected := model.aggregate()
	validateLakehouseBenchSnapshot(t, queryDB, tableExpr, expected)
	queries := runLakehouseBenchQueries(t, queryDB, tableExpr, expected, queryRuns)
	dataObjects, deleteObjects, totalObjects, totalBytes := lakehouseBenchObjectStats(t, ctx, objectStore, samplePrefix)

	return lakehouseBenchResult{Target: target, Scenario: scenario, BaselineLoadDurationMS: baselineDuration.Milliseconds(), WriteDurationMS: writeDuration.Milliseconds(), FlushCount: int(math.Ceil(float64(scenarioMutationUnits(scenario)) / float64(scenario.FlushRows))), FinalRows: expected.Rows, DataObjects: dataObjects, DeleteObjects: deleteObjects, TotalObjects: totalObjects, TotalObjectBytes: totalBytes, Queries: queries}
}

func scenarioMutationUnits(scenario lakehouseBenchScenario) int {
	return scenario.InsertRows + scenario.DeleteRows + 2*scenario.UpdateRows
}

func duckLakeBenchCatalogPath(t *testing.T, target lakehouseBenchTarget) string {
	t.Helper()
	if target.Format != "ducklake" {
		return ""
	}
	if target.DuckLakeCatalog == "duckdb" {
		return filepath.Join(t.TempDir(), "catalog.ducklake")
	}
	return filepath.Join(t.TempDir(), "catalog.sqlite")
}

func newLakehouseBenchTarget(t *testing.T, ctx context.Context, target lakehouseBenchTarget, s3Client storage.ObjectStorage, stateStore *state.Store, samplePrefix, catalogPath string, flushRows int, logger *slog.Logger) lakehouseBenchHandle {
	t.Helper()
	const schema, table = "public", "feature_bench"
	switch target.Format {
	case "iceberg":
		catalog := iceberg.NewCatalog(s3Client, s3Bucket, path.Join(samplePrefix, "iceberg"))
		writer := iceberg.NewWriter(catalog, s3Client, stateStore, slotName, flushRows, time.Hour, logger, iceberg.WithMutationMode(iceberg.MutationMode(target.IcebergMutation)))
		tableExpr := fmt.Sprintf("iceberg_scan('s3://%s/%s/%s/%s/%s', allow_moved_paths = true)", s3Bucket, samplePrefix, "iceberg", schema, table)
		return lakehouseBenchHandle{
			writer:      writer,
			closeWriter: func() {},
			openQuery: func() (*sql.DB, string, func()) {
				db := newTestDuckDB(t)
				return db, tableExpr, func() {}
			},
		}
	case "ducklake":
		cfg := ducklake.Config{CatalogPath: catalogPath, CatalogStore: target.DuckLakeCatalog, DataPath: fmt.Sprintf("s3://%s/%s/ducklake-data/", s3Bucket, samplePrefix), S3Endpoint: minioEndpoint, S3Region: s3Region}
		writer, err := ducklake.NewWriter(ctx, cfg, stateStore, flushRows, time.Hour, logger)
		if err != nil {
			t.Fatalf("ducklake writer: %v", err)
		}
		return lakehouseBenchHandle{
			writer:      writer,
			closeWriter: func() { _ = writer.Close() },
			openQuery: func() (*sql.DB, string, func()) {
				queryDB, err := ducklake.Open(ctx, cfg)
				if err != nil {
					t.Fatalf("ducklake query open: %v", err)
				}
				return queryDB, fmt.Sprintf("%s.%s.%s", quoteBenchIdent("streambed"), quoteBenchIdent(schema), quoteBenchIdent(table)), func() { _ = queryDB.Close() }
			},
		}
	default:
		t.Fatalf("unknown target format %q", target.Format)
		return lakehouseBenchHandle{}
	}
}

func writeBaselineEvents(t *testing.T, ctx context.Context, writer pipeline.Writer, rows, flushRows int, model *lakeBenchModel) {
	t.Helper()
	for id := 1; id <= rows; id++ {
		model.upsert(int64(id), 0, int64(id*10))
		if _, err := writer.HandleEvent(ctx, lakeInsertEvent(id, 0, int64(id*10), pglogrepl.LSN(id))); err != nil {
			t.Fatalf("baseline insert %d: %v", id, err)
		}
		if id%flushRows == 0 {
			if err := writer.FlushAll(ctx); err != nil {
				t.Fatalf("baseline flush: %v", err)
			}
		}
	}
}

func writeMutationEvents(t *testing.T, ctx context.Context, writer pipeline.Writer, scenario lakehouseBenchScenario, model *lakeBenchModel) {
	t.Helper()
	lsn := scenario.BaselineRows + 1
	for id := 1; id <= scenario.UpdateRows; id++ {
		amount := int64(id * 20)
		model.upsert(int64(id), 1, amount)
		if _, err := writer.HandleEvent(ctx, lakeUpdateEvent(id, 1, amount, pglogrepl.LSN(lsn))); err != nil {
			t.Fatalf("update %d: %v", id, err)
		}
		lsn++
	}
	deleteStart := scenario.UpdateRows + 1
	for id := deleteStart; id < deleteStart+scenario.DeleteRows; id++ {
		model.delete(int64(id))
		if _, err := writer.HandleEvent(ctx, lakeDeleteEvent(id, pglogrepl.LSN(lsn))); err != nil {
			t.Fatalf("delete %d: %v", id, err)
		}
		lsn++
	}
	insertStart := scenario.BaselineRows + 1
	for id := insertStart; id < insertStart+scenario.InsertRows; id++ {
		amount := int64(id * 10)
		model.upsert(int64(id), 0, amount)
		if _, err := writer.HandleEvent(ctx, lakeInsertEvent(id, 0, amount, pglogrepl.LSN(lsn))); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
		lsn++
	}
}

func lakeBenchColumns() []wal.Column {
	return []wal.Column{{Name: "id", OID: 20, IsKey: true}, {Name: "version", OID: 20}, {Name: "amount", OID: 20}, {Name: "payload", OID: 25}}
}

func lakeInsertEvent(id, version int, amount int64, lsn pglogrepl.LSN) wal.RowEvent {
	return wal.RowEvent{Schema: "public", Table: "feature_bench", Columns: lakeBenchColumns(), KeyColumns: []int{0}, Op: wal.OpInsert, Values: lakeBenchValues(id, version, amount), WALStartLSN: lsn}
}

func lakeUpdateEvent(id, version int, amount int64, lsn pglogrepl.LSN) wal.RowEvent {
	return wal.RowEvent{Schema: "public", Table: "feature_bench", Columns: lakeBenchColumns(), KeyColumns: []int{0}, Op: wal.OpUpdate, OldKey: []wal.ColumnValue{{Name: "id", OID: 20, Value: []byte(strconv.Itoa(id))}}, Values: lakeBenchValues(id, version, amount), WALStartLSN: lsn}
}

func lakeDeleteEvent(id int, lsn pglogrepl.LSN) wal.RowEvent {
	return wal.RowEvent{Schema: "public", Table: "feature_bench", Columns: lakeBenchColumns(), KeyColumns: []int{0}, Op: wal.OpDelete, OldKey: []wal.ColumnValue{{Name: "id", OID: 20, Value: []byte(strconv.Itoa(id))}}, WALStartLSN: lsn}
}

func lakeBenchValues(id, version int, amount int64) []wal.ColumnValue {
	return []wal.ColumnValue{{Name: "id", OID: 20, Value: []byte(strconv.Itoa(id))}, {Name: "version", OID: 20, Value: []byte(strconv.Itoa(version))}, {Name: "amount", OID: 20, Value: []byte(strconv.FormatInt(amount, 10))}, {Name: "payload", OID: 25, Value: []byte(fmt.Sprintf("payload_%08d", id))}}
}

func validateLakehouseBenchSnapshot(t *testing.T, db *sql.DB, tableExpr string, expected lakeBenchAggregate) {
	t.Helper()
	var got lakeBenchAggregate
	query := fmt.Sprintf(`SELECT count(*), count(DISTINCT id), COALESCE(sum(id), 0), COALESCE(sum(version), 0), COALESCE(sum(amount), 0) FROM %s`, tableExpr)
	if err := db.QueryRow(query).Scan(&got.Rows, &got.DistinctIDs, &got.SumIDs, &got.SumVersion, &got.SumAmount); err != nil {
		t.Fatalf("validate query: %v", err)
	}
	if got != expected || got.Rows != got.DistinctIDs {
		t.Fatalf("snapshot mismatch: got=%+v expected=%+v", got, expected)
	}
}

func runLakehouseBenchQueries(t *testing.T, db *sql.DB, tableExpr string, expected lakeBenchAggregate, runs int) []lakehouseBenchQueryResult {
	t.Helper()
	queries := []struct{ name, sql string }{
		{"full_aggregate", fmt.Sprintf(`SELECT count(*), count(DISTINCT id), COALESCE(sum(amount), 0) FROM %s`, tableExpr)},
		{"selective_range", fmt.Sprintf(`SELECT count(*), COALESCE(sum(amount), 0) FROM %s WHERE id BETWEEN %d AND %d`, tableExpr, max(1, int(expected.Rows)/4), max(1, int(expected.Rows)/4)+100)},
		{"point_lookup", fmt.Sprintf(`SELECT id, version, amount FROM %s WHERE id = %d`, tableExpr, max(1, int(expected.Rows)/2))},
		{"topn_ordered", fmt.Sprintf(`SELECT id, amount FROM %s ORDER BY amount DESC LIMIT 10`, tableExpr)},
	}
	out := make([]lakehouseBenchQueryResult, 0, len(queries))
	for _, q := range queries {
		durations := make([]time.Duration, 0, runs)
		for i := 0; i < runs; i++ {
			started := time.Now()
			rows, err := db.Query(q.sql)
			if err != nil {
				t.Fatalf("%s query: %v", q.name, err)
			}
			for rows.Next() {
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("%s rows: %v", q.name, err)
			}
			rows.Close()
			durations = append(durations, time.Since(started))
		}
		out = append(out, lakehouseBenchQueryResult{Name: q.name, MedianMS: durationPercentileMS(durations, 0.50), P95MS: durationPercentileMS(durations, 0.95)})
	}
	return out
}

type lakeBenchRow struct{ version, amount int64 }
type lakeBenchModel struct{ rows map[int64]lakeBenchRow }

func newLakeBenchModel() *lakeBenchModel { return &lakeBenchModel{rows: map[int64]lakeBenchRow{}} }
func (m *lakeBenchModel) upsert(id, version, amount int64) {
	m.rows[id] = lakeBenchRow{version: version, amount: amount}
}
func (m *lakeBenchModel) delete(id int64) { delete(m.rows, id) }
func (m *lakeBenchModel) aggregate() lakeBenchAggregate {
	var a lakeBenchAggregate
	for id, row := range m.rows {
		a.Rows++
		a.DistinctIDs++
		a.SumIDs += id
		a.SumVersion += row.version
		a.SumAmount += row.amount
	}
	return a
}

func lakehouseBenchObjectStats(t *testing.T, ctx context.Context, s3Client storage.ObjectStorage, prefix string) (dataObjects, deleteObjects, totalObjects int, totalBytes int64) {
	t.Helper()
	keys, err := s3Client.ListPrefix(ctx, prefix)
	if err != nil {
		t.Fatalf("list bench prefix: %v", err)
	}
	for _, key := range keys {
		totalObjects++
		obj, err := s3Client.GetObject(ctx, key)
		if err == nil {
			totalBytes += int64(len(obj))
		}
		if filepath.Ext(key) == ".parquet" {
			dataObjects++
		}
		if filepath.Ext(key) == ".posdel" || filepath.Ext(key) == ".eqdel" || filepath.Ext(key) == ".delete" || strings.Contains(key, "delete") {
			deleteObjects++
		}
	}
	return
}

func cleanupObjectPrefix(t *testing.T, ctx context.Context, s3Client storage.ObjectStorage, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		keys, err := s3Client.ListPrefix(ctx, prefix)
		if err == nil && len(keys) > 0 {
			_ = s3Client.DeleteObjects(ctx, keys)
		}
	})
}

func skipIfMinIOUnavailable(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "localhost:9002", 2*time.Second)
	if err != nil {
		t.Skipf("MinIO not available at localhost:9002: %v", err)
	}
	_ = conn.Close()
}

func envInt(t *testing.T, key string, def, minValue int) int {
	t.Helper()
	if raw := os.Getenv(key); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < minValue {
			t.Fatalf("%s must be >= %d, got %q", key, minValue, raw)
		}
		return v
	}
	return def
}

func envIntList(t *testing.T, key string, def []int, minValue int) []int {
	t.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.Atoi(part)
		if err != nil || v < minValue {
			t.Fatalf("%s entries must be >= %d, got %q", key, minValue, part)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		t.Fatalf("%s must contain at least one integer", key)
	}
	return out
}

func quoteBenchIdent(s string) string { return `"` + s + `"` }
