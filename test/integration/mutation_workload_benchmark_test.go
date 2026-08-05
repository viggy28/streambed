//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/viggy28/streambed/internal/iceberg"
)

type mutationWorkloadSpec struct {
	Name    string
	Kind    string
	Amount  int
	Batches int
}

type mutationWorkloadResult struct {
	SampleID                     string `json:"sample_id"`
	Repeat                       int    `json:"repeat"`
	Mode                         string `json:"mode"`
	Scenario                     string `json:"scenario"`
	BaselineRows                 int    `json:"baseline_rows"`
	MutationRows                 int    `json:"mutation_rows"`
	InsertOperations             int    `json:"insert_operations"`
	UpdateOperations             int    `json:"update_operations"`
	DeleteOperations             int    `json:"delete_operations"`
	Batches                      int    `json:"batches"`
	SourceSQLDurationMS          int64  `json:"source_sql_duration_ms"`
	CDCStartupToCommitDurationMS int64  `json:"cdc_startup_to_commit_duration_ms"`
	WriterFlushDurationMS        int64  `json:"writer_flush_duration_ms"`
	WriterFlushes                int    `json:"writer_flushes"`
	QueryMedianDurationMS        int64  `json:"query_median_duration_ms"`
	QueryP95DurationMS           int64  `json:"query_p95_duration_ms"`
	LogicalStorageGetCalls       int64  `json:"logical_storage_get_calls"`
	LogicalStorageGetBytes       int64  `json:"logical_storage_get_bytes"`
	LogicalStoragePutCalls       int64  `json:"logical_storage_put_calls"`
	LogicalStoragePutBytes       int64  `json:"logical_storage_put_bytes"`
	RetainedDataObjectCount      int    `json:"retained_data_object_count"`
	RetainedDeleteObjectCount    int    `json:"retained_delete_object_count"`
	RetainedDataObjectBytes      int64  `json:"retained_data_object_bytes"`
	RetainedDeleteObjectBytes    int64  `json:"retained_delete_object_bytes"`
}

type mutationWorkloadReport struct {
	SchemaVersion int                      `json:"schema_version"`
	Metadata      benchmarkMetadata        `json:"metadata"`
	BaselineRows  int                      `json:"baseline_rows"`
	Samples       []mutationWorkloadResult `json:"samples"`
	Caveats       []string                 `json:"caveats"`
}

type workloadAggregate struct {
	Rows, DistinctIDs, SumIDs, SumVersions, PayloadBytes int64
}

// TestMutationModeWorkloads benchmarks inserts, update/delete densities, a
// mixed transaction, and repeated mutation batches against a 100 MiB logical
// baseline by default. It is deliberately separate from the table-size
// benchmark so workload shape and table size are not conflated.
func TestMutationModeWorkloads(t *testing.T) {
	if os.Getenv("STREAMBED_RUN_MUTATION_WORKLOAD_BENCH") != "1" {
		t.Skip("set STREAMBED_RUN_MUTATION_WORKLOAD_BENCH=1 to run workload benchmark")
	}
	skipIfNotAvailable(t)

	baselineRows := 100 * 1024 // 1 KiB logical payload per row ~= 100 MiB
	if raw := os.Getenv("STREAMBED_MUTATION_WORKLOAD_ROWS"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 10_000 {
			t.Fatalf("STREAMBED_MUTATION_WORKLOAD_ROWS must be >= 10000, got %q", raw)
		}
		baselineRows = v
	}
	repetitions := 3
	if raw := os.Getenv("STREAMBED_MUTATION_WORKLOAD_REPETITIONS"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 3 {
			t.Fatalf("STREAMBED_MUTATION_WORKLOAD_REPETITIONS must be >= 3, got %q", raw)
		}
		repetitions = v
	}
	queryRuns := 5

	pct := func(n int, fraction float64) int { return max(1, int(math.Round(float64(n)*fraction))) }
	specs := []mutationWorkloadSpec{
		{Name: "insert-100", Kind: "insert", Amount: 100, Batches: 1},
		{Name: "update-100", Kind: "update", Amount: min(100, baselineRows), Batches: 1},
		{Name: "update-1pct", Kind: "update", Amount: pct(baselineRows, .01), Batches: 1},
		{Name: "update-10pct", Kind: "update", Amount: pct(baselineRows, .10), Batches: 1},
		{Name: "update-100pct", Kind: "update", Amount: baselineRows, Batches: 1},
		{Name: "delete-100", Kind: "delete", Amount: min(100, baselineRows), Batches: 1},
		{Name: "delete-1pct", Kind: "delete", Amount: pct(baselineRows, .01), Batches: 1},
		{Name: "delete-10pct", Kind: "delete", Amount: pct(baselineRows, .10), Batches: 1},
		{Name: "delete-100pct", Kind: "delete", Amount: baselineRows, Batches: 1},
		{Name: "mixed-100", Kind: "mixed", Amount: 100, Batches: 1},
		{Name: "repeated-10", Kind: "repeated", Amount: 100, Batches: 10},
		{Name: "repeated-100", Kind: "repeated", Amount: 100, Batches: 100},
	}
	if os.Getenv("STREAMBED_MUTATION_WORKLOAD_INCLUDE_1000") == "1" {
		if baselineRows < 80_000 {
			t.Fatal("repeated-1000 requires STREAMBED_MUTATION_WORKLOAD_ROWS >= 80000")
		}
		specs = append(specs, mutationWorkloadSpec{Name: "repeated-1000", Kind: "repeated", Amount: 100, Batches: 1000})
	}

	var results []mutationWorkloadResult
	for specIndex, spec := range specs {
		for repeat := 1; repeat <= repetitions; repeat++ {
			modes := []iceberg.MutationMode{iceberg.MutationModeCOW, iceberg.MutationModeMOR}
			if (specIndex+repeat)%2 == 0 {
				modes[0], modes[1] = modes[1], modes[0]
			}
			for _, mode := range modes {
				t.Run(fmt.Sprintf("%s/repeat-%d/%s", spec.Name, repeat, mode), func(t *testing.T) {
					ctx := context.Background()
					cleanup(t)
					clearS3Prefix(t)
					execSQL(t, "DROP TABLE IF EXISTS mutation_size_bench")
					t.Cleanup(func() { execSQL(t, "DROP TABLE IF EXISTS mutation_size_bench"); cleanup(t) })
					execSQL(t, "CREATE TABLE mutation_size_bench (id BIGINT PRIMARY KEY, version BIGINT NOT NULL DEFAULT 0, payload TEXT NOT NULL)")
					createSlotAndPublication(t)
					execSQL(t, fmt.Sprintf(`INSERT INTO mutation_size_bench
						SELECT i, 0, repeat(md5(i::text), 32) FROM generate_series(1, %d) AS g(i)`, baselineRows))

					statePath := filepath.Join(t.TempDir(), "state.db")
					if _, err := runSyncBenchWithMode(t, ctx, baselineRows, 50_000, time.Second, 10*time.Minute, mode, nil, true, 0, statePath); err != nil {
						t.Fatalf("baseline sync: %v", err)
					}

					sourceStarted := time.Now()
					applyMutationWorkload(t, spec, baselineRows)
					sourceDuration := time.Since(sourceStarted)
					expected := postgresWorkloadAggregate(t)
					expectedDigest := postgresWorkloadDigest(t)

					targetRows, targetDeletes, flushUnits := workloadTargets(spec)
					metrics := &benchStorageMetrics{}
					tracker, err := runSyncBenchWithMode(t, ctx, targetRows, flushUnits, 5*time.Minute, 30*time.Minute, mode, metrics, false, targetDeletes, statePath)
					if err != nil {
						t.Fatalf("mutation sync: %v", err)
					}
					rows, flushes, cdcDuration, avgFlushMS, _, _ := tracker.snapshot()
					tracker.mu.Lock()
					deletes := tracker.cumDeletes
					tracker.mu.Unlock()
					expectedFlushes := 1
					if spec.Kind == "repeated" {
						expectedFlushes = spec.Batches
					}
					if rows != int64(targetRows) || deletes != int64(targetDeletes) || flushes != expectedFlushes {
						t.Fatalf("flush shape: rows=%d/%d deletes=%d/%d flushes=%d/%d", rows, targetRows, deletes, targetDeletes, flushes, expectedFlushes)
					}
					gets, getBytes, puts, putBytes := metrics.snapshot()

					queryDurations := validateWorkloadSnapshot(t, expected, expectedDigest, queryRuns)
					dataCount, deleteCount, dataBytes, deleteBytes := mutationTableFiles(t)
					inserts, updates, deleteOps := workloadOperationCounts(spec)
					results = append(results, mutationWorkloadResult{
						SampleID: fmt.Sprintf("%s-r%d-%s", spec.Name, repeat, mode), Repeat: repeat, Mode: string(mode),
						Scenario: spec.Name, BaselineRows: baselineRows, MutationRows: inserts + updates + deleteOps,
						InsertOperations: inserts, UpdateOperations: updates, DeleteOperations: deleteOps, Batches: spec.Batches,
						SourceSQLDurationMS: sourceDuration.Milliseconds(), CDCStartupToCommitDurationMS: cdcDuration.Milliseconds(),
						WriterFlushDurationMS: int64(math.Round(avgFlushMS * float64(flushes))), WriterFlushes: flushes,
						QueryMedianDurationMS: durationPercentileMS(queryDurations, .50), QueryP95DurationMS: durationPercentileMS(queryDurations, .95),
						LogicalStorageGetCalls: gets, LogicalStorageGetBytes: getBytes, LogicalStoragePutCalls: puts, LogicalStoragePutBytes: putBytes,
						RetainedDataObjectCount: dataCount, RetainedDeleteObjectCount: deleteCount,
						RetainedDataObjectBytes: dataBytes, RetainedDeleteObjectBytes: deleteBytes,
					})
				})
			}
		}
	}

	report := mutationWorkloadReport{SchemaVersion: 1, Metadata: collectBenchmarkMetadata(repetitions, 0, queryRuns), BaselineRows: baselineRows, Samples: results, Caveats: []string{
		"All scenarios use a fixed logical baseline (100 MiB by default); this benchmark measures workload shape, not table-size scaling.",
		"Repeated cases queue committed source transactions before CDC starts and force one Iceberg flush per batch; query latency is measured after the final batch.",
		"Retained object metrics include files reachable only from historical snapshots; they are useful for storage growth but are not active scan-file counts.",
		"Runs use local MinIO and deterministic compressible payloads; cloud latency, CPU, and RSS are not represented.",
	}}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("mutation workload benchmark JSON:\n%s", encoded)
	if out := os.Getenv("STREAMBED_MUTATION_WORKLOAD_BENCH_OUTPUT"); out != "" {
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(out, append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func workloadTargets(spec mutationWorkloadSpec) (targetRows, targetDeletes, flushUnits int) {
	switch spec.Kind {
	case "insert":
		return spec.Amount, 0, spec.Amount
	case "update":
		return spec.Amount, spec.Amount, 2 * spec.Amount // replacement row + equality-delete key
	case "delete":
		return 0, spec.Amount, spec.Amount
	case "mixed":
		return 80, 60, 140 // 40 inserts + 40 updates; updates also carry 40 delete keys, plus 20 deletes
	case "repeated":
		return 80 * spec.Batches, 60 * spec.Batches, 140
	default:
		panic("unknown workload: " + spec.Kind)
	}
}

func workloadOperationCounts(spec mutationWorkloadSpec) (inserts, updates, deletes int) {
	switch spec.Kind {
	case "insert":
		return spec.Amount, 0, 0
	case "update":
		return 0, spec.Amount, 0
	case "delete":
		return 0, 0, spec.Amount
	case "mixed":
		return 40, 40, 20
	case "repeated":
		return 40 * spec.Batches, 40 * spec.Batches, 20 * spec.Batches
	default:
		panic("unknown workload: " + spec.Kind)
	}
}

func applyMutationWorkload(t *testing.T, spec mutationWorkloadSpec, baselineRows int) {
	t.Helper()
	switch spec.Kind {
	case "insert":
		execSQL(t, fmt.Sprintf(`INSERT INTO mutation_size_bench
			SELECT %d+i, 1, repeat('i', 1024) FROM generate_series(1, %d) AS g(i)`, baselineRows, spec.Amount))
	case "update":
		execSQL(t, fmt.Sprintf("UPDATE mutation_size_bench SET version=1, payload=repeat('u', 1024) WHERE id <= %d", spec.Amount))
	case "delete":
		execSQL(t, fmt.Sprintf("DELETE FROM mutation_size_bench WHERE id <= %d", spec.Amount))
	case "mixed":
		execSQL(t, fmt.Sprintf(`BEGIN;
			INSERT INTO mutation_size_bench SELECT %d+i, 1, repeat('i', 1024) FROM generate_series(1, 40) AS g(i);
			UPDATE mutation_size_bench SET version=1, payload=repeat('m', 1024) WHERE id BETWEEN 1 AND 39;
			UPDATE mutation_size_bench SET id=%d, version=2, payload=repeat('k', 1024) WHERE id=40;
			DELETE FROM mutation_size_bench WHERE id BETWEEN 41 AND 60;
			COMMIT;`, baselineRows, baselineRows+41))
	case "repeated":
		for batch := 0; batch < spec.Batches; batch++ {
			insertStart := baselineRows + batch*40
			updateStart := batch*40 + 1
			deleteStart := baselineRows/2 + batch*20 + 1
			execSQL(t, fmt.Sprintf(`BEGIN;
				INSERT INTO mutation_size_bench SELECT %d+i, %d, repeat('i', 1024) FROM generate_series(1, 40) AS g(i);
				UPDATE mutation_size_bench SET version=version+1, payload=repeat('r', 1024) WHERE id BETWEEN %d AND %d;
				DELETE FROM mutation_size_bench WHERE id BETWEEN %d AND %d;
				COMMIT;`, insertStart, batch+1, updateStart, updateStart+39, deleteStart, deleteStart+19))
		}
	default:
		t.Fatalf("unknown workload kind %q", spec.Kind)
	}
}

func postgresWorkloadAggregate(t *testing.T) workloadAggregate {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var a workloadAggregate
	err = conn.QueryRow(ctx, `SELECT count(*), count(DISTINCT id),
		COALESCE(sum(id), 0), COALESCE(sum(version), 0), COALESCE(sum(length(payload)), 0)
		FROM mutation_size_bench`).Scan(&a.Rows, &a.DistinctIDs, &a.SumIDs, &a.SumVersions, &a.PayloadBytes)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func postgresWorkloadDigest(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, "SELECT id, version, md5(payload) FROM mutation_size_bench ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	h := sha256.New()
	for rows.Next() {
		var id, version int64
		var payloadMD5 string
		if err := rows.Scan(&id, &version, &payloadMD5); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(h, "%d:%d:%s\n", id, version, payloadMD5)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func validateWorkloadSnapshot(t *testing.T, expected workloadAggregate, expectedDigest string, runs int) []time.Duration {
	t.Helper()
	db := newTestDuckDB(t)
	scan := fmt.Sprintf("iceberg_scan('s3://%s/%s/public/mutation_size_bench', allow_moved_paths = true)", s3Bucket, s3Prefix)
	query := fmt.Sprintf(`SELECT count(*), count(DISTINCT id), COALESCE(sum(id), 0),
		COALESCE(sum(version), 0), COALESCE(sum(length(payload)), 0) FROM %s`, scan)
	digestRows, err := db.Query(fmt.Sprintf("SELECT id, version, md5(payload) FROM %s ORDER BY id", scan))
	if err != nil {
		t.Fatalf("DuckDB digest query: %v", err)
	}
	h := sha256.New()
	for digestRows.Next() {
		var id, version int64
		var payloadMD5 string
		if err := digestRows.Scan(&id, &version, &payloadMD5); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(h, "%d:%d:%s\n", id, version, payloadMD5)
	}
	if err := digestRows.Err(); err != nil {
		t.Fatal(err)
	}
	digestRows.Close()
	if digest := hex.EncodeToString(h.Sum(nil)); digest != expectedDigest {
		t.Fatalf("exact row digest mismatch: got=%s expected=%s", digest, expectedDigest)
	}

	durations := make([]time.Duration, 0, runs)
	for i := 0; i < runs; i++ {
		started := time.Now()
		var got workloadAggregate
		if err := db.QueryRow(query).Scan(&got.Rows, &got.DistinctIDs, &got.SumIDs, &got.SumVersions, &got.PayloadBytes); err != nil {
			t.Fatalf("DuckDB workload query: %v", err)
		}
		durations = append(durations, time.Since(started))
		if got != expected || got.Rows != got.DistinctIDs {
			t.Fatalf("snapshot mismatch: got=%+v expected=%+v", got, expected)
		}
	}
	return durations
}
