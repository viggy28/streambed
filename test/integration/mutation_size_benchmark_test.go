//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/storage"
)

type benchStorageMetrics struct {
	inner              storage.ObjectStorage
	mu                 sync.Mutex
	getCount, putCount int64
	getBytes, putBytes int64
}

func (m *benchStorageMetrics) PutObject(ctx context.Context, key string, data []byte, contentType string) error {
	err := m.inner.PutObject(ctx, key, data, contentType)
	if err == nil {
		m.mu.Lock()
		m.putCount++
		m.putBytes += int64(len(data))
		m.mu.Unlock()
	}
	return err
}
func (m *benchStorageMetrics) GetObject(ctx context.Context, key string) ([]byte, error) {
	data, err := m.inner.GetObject(ctx, key)
	if err == nil {
		m.mu.Lock()
		m.getCount++
		m.getBytes += int64(len(data))
		m.mu.Unlock()
	}
	return data, err
}
func (m *benchStorageMetrics) HeadObject(ctx context.Context, key string) (bool, error) {
	return m.inner.HeadObject(ctx, key)
}
func (m *benchStorageMetrics) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	return m.inner.ListPrefix(ctx, prefix)
}
func (m *benchStorageMetrics) DeleteObjects(ctx context.Context, keys []string) error {
	return m.inner.DeleteObjects(ctx, keys)
}
func (m *benchStorageMetrics) Bucket() string { return m.inner.Bucket() }
func (m *benchStorageMetrics) snapshot() (int64, int64, int64, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getCount, m.getBytes, m.putCount, m.putBytes
}

type mutationSizeResult struct {
	SampleID                     string `json:"sample_id"`
	Repeat                       int    `json:"repeat"`
	Mode                         string `json:"mode"`
	TargetMiB                    int    `json:"target_mib"`
	LogicalBytes                 int64  `json:"logical_bytes"`
	Rows                         int    `json:"rows"`
	UpdatedRows                  int    `json:"updated_rows"`
	CDCStartupToCommitDurationMS int64  `json:"cdc_startup_to_commit_duration_ms"`
	WriterFlushDurationMS        int64  `json:"writer_flush_duration_ms"`
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

type linearProjection struct {
	Mode                 string  `json:"mode"`
	Metric               string  `json:"metric"`
	TargetMiB            int     `json:"target_mib"`
	ProjectedValue       float64 `json:"projected_value"`
	SlopePerMiB          float64 `json:"slope_per_mib"`
	Intercept            float64 `json:"intercept"`
	RSquared             float64 `json:"r_squared"`
	HoldoutRelativeError float64 `json:"holdout_relative_error"`
	Measured             bool    `json:"measured"`
}

type benchmarkMetadata struct {
	TimestampUTC string `json:"timestamp_utc"`
	GitCommit    string `json:"git_commit"`
	GitDirty     bool   `json:"git_dirty"`
	GoVersion    string `json:"go_version"`
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	Repetitions  int    `json:"repetitions"`
	UpdatedRows  int    `json:"updated_rows,omitempty"`
	QueryRuns    int    `json:"query_runs"`
}

type mutationBenchmarkReport struct {
	SchemaVersion      int                  `json:"schema_version"`
	Metadata           benchmarkMetadata    `json:"metadata"`
	Samples            []mutationSizeResult `json:"samples"`
	Extrapolated100GiB []linearProjection   `json:"extrapolated_100_gib"`
	Caveats            []string             `json:"caveats"`
}

// TestMutationModeByTableSize is an opt-in, resource-intensive integration
// benchmark. Targets are logical payload sizes; actual Parquet bytes are
// reported separately. Set STREAMBED_RUN_MUTATION_SIZE_BENCH=1 to enable it.
// The 1 GiB case additionally requires STREAMBED_MUTATION_BENCH_MAX_MIB=1024.
func TestMutationModeByTableSize(t *testing.T) {
	if os.Getenv("STREAMBED_RUN_MUTATION_SIZE_BENCH") != "1" {
		t.Skip("set STREAMBED_RUN_MUTATION_SIZE_BENCH=1 to run mutation size benchmark")
	}
	skipIfNotAvailable(t)

	maxMiB := 100
	if raw := os.Getenv("STREAMBED_MUTATION_BENCH_MAX_MIB"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			t.Fatalf("invalid STREAMBED_MUTATION_BENCH_MAX_MIB=%q", raw)
		}
		maxMiB = v
	}
	const payloadBytes = 1024
	const updatedRows = 100
	const queryRuns = 5
	repetitions := 3
	if raw := os.Getenv("STREAMBED_MUTATION_BENCH_REPETITIONS"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 3 {
			t.Fatalf("STREAMBED_MUTATION_BENCH_REPETITIONS must be >= 3, got %q", raw)
		}
		repetitions = v
	}
	targets := []int{10, 100, 1024}
	var results []mutationSizeResult

	for targetIndex, targetMiB := range targets {
		if targetMiB > maxMiB {
			t.Logf("resource guard: skipping %d MiB target (max=%d MiB)", targetMiB, maxMiB)
			continue
		}
		for repeat := 1; repeat <= repetitions; repeat++ {
			modes := []iceberg.MutationMode{iceberg.MutationModeCOW, iceberg.MutationModeMOR}
			if (targetIndex+repeat)%2 == 0 {
				modes[0], modes[1] = modes[1], modes[0]
			}
			for _, mode := range modes {
				name := fmt.Sprintf("%dMiB/repeat-%d/%s", targetMiB, repeat, mode)
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					cleanup(t)
					clearS3Prefix(t)
					execSQL(t, "DROP TABLE IF EXISTS mutation_size_bench")
					t.Cleanup(func() { execSQL(t, "DROP TABLE IF EXISTS mutation_size_bench"); cleanup(t) })
					execSQL(t, "CREATE TABLE mutation_size_bench (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)")
					createSlotAndPublication(t)

					logicalBytes := int64(targetMiB) * 1024 * 1024
					rows := int(logicalBytes / payloadBytes)
					execSQL(t, fmt.Sprintf(`INSERT INTO mutation_size_bench
						SELECT i, repeat(md5(i::text), 32) FROM generate_series(1, %d) AS g(i)`, rows))
					statePath := t.TempDir() + "/state.db"
					baselineTimeout := 5 * time.Minute
					if targetMiB >= 1024 {
						baselineTimeout = 20 * time.Minute
					}
					if _, err := runSyncBenchWithMode(t, ctx, rows, 50_000, time.Second, baselineTimeout, mode, nil, true, 0, statePath); err != nil {
						t.Fatalf("baseline sync: %v", err)
					}

					execSQL(t, fmt.Sprintf("UPDATE mutation_size_bench SET payload = repeat('f', 1024) WHERE id <= %d", updatedRows))
					metrics := &benchStorageMetrics{}
					mutationTimeout := 5 * time.Minute
					if targetMiB >= 1024 {
						mutationTimeout = 30 * time.Minute
					}
					tracker, err := runSyncBenchWithMode(t, ctx, updatedRows, 2*updatedRows, time.Second, mutationTimeout, mode, metrics, false, updatedRows, statePath)
					if err != nil {
						t.Fatalf("mutation sync: %v", err)
					}
					flushedRows, flushes, cdcDuration, avgFlushMS, _, _ := tracker.snapshot()
					tracker.mu.Lock()
					flushedDeletes := tracker.cumDeletes
					tracker.mu.Unlock()
					if flushedRows != updatedRows || flushedDeletes != updatedRows || flushes != 1 {
						t.Fatalf("flush shape: rows=%d/%d deletes=%d/%d flushes=%d/1", flushedRows, updatedRows, flushedDeletes, updatedRows, flushes)
					}
					gets, getBytes, puts, putBytes := metrics.snapshot()

					duckDB := newTestDuckDB(t)
					scan := fmt.Sprintf("iceberg_scan('s3://%s/%s/public/mutation_size_bench', allow_moved_paths = true)", s3Bucket, s3Prefix)
					query := fmt.Sprintf("SELECT count(*), count(*) FILTER (WHERE id <= %d AND payload = repeat('f', 1024)) FROM %s", updatedRows, scan)
					queryDurations := make([]time.Duration, 0, queryRuns)
					for i := 0; i < queryRuns; i++ {
						started := time.Now()
						var total, changed int64
						if err := duckDB.QueryRow(query).Scan(&total, &changed); err != nil {
							t.Fatalf("DuckDB correctness query: %v", err)
						}
						queryDurations = append(queryDurations, time.Since(started))
						if total != int64(rows) || changed != updatedRows {
							t.Fatalf("wrong rows: total=%d/%d changed=%d/%d", total, rows, changed, updatedRows)
						}
					}

					dataCount, deleteCount, dataBytes, deleteBytes := mutationTableFiles(t)
					results = append(results, mutationSizeResult{
						SampleID: fmt.Sprintf("%dMiB-r%d-%s", targetMiB, repeat, mode), Repeat: repeat,
						Mode: string(mode), TargetMiB: targetMiB, LogicalBytes: logicalBytes, Rows: rows, UpdatedRows: updatedRows,
						CDCStartupToCommitDurationMS: cdcDuration.Milliseconds(), WriterFlushDurationMS: int64(math.Round(avgFlushMS * float64(flushes))),
						QueryMedianDurationMS: durationPercentileMS(queryDurations, 0.50), QueryP95DurationMS: durationPercentileMS(queryDurations, 0.95),
						LogicalStorageGetCalls: gets, LogicalStorageGetBytes: getBytes, LogicalStoragePutCalls: puts, LogicalStoragePutBytes: putBytes,
						RetainedDataObjectCount: dataCount, RetainedDeleteObjectCount: deleteCount,
						RetainedDataObjectBytes: dataBytes, RetainedDeleteObjectBytes: deleteBytes,
					})
				})
			}
		}
	}

	report := mutationBenchmarkReport{SchemaVersion: 2, Metadata: collectBenchmarkMetadata(repetitions, updatedRows, queryRuns), Samples: results, Caveats: []string{
		"100 GiB values are linear extrapolations, not measurements; projections failing the 1 GiB holdout threshold are suppressed.",
		"Targets are logical payload sizes; retained physical Parquet object bytes include historical files and depend on compression.",
		"Logical storage counters include successful ObjectStorage calls/bytes during startup reconciliation and mutation commit; they are not physical HTTP request counts.",
		"Runs use local MinIO and one 100-row mutation batch; cloud S3 latency and sustained delete accumulation are not represented.",
		"CPU and RSS are not captured in-process; use an external profiler for resource measurements.",
	}}
	report.Extrapolated100GiB = extrapolate100GiB(results)
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("mutation benchmark JSON:\n%s", encoded)
	if out := os.Getenv("STREAMBED_MUTATION_BENCH_OUTPUT"); out != "" {
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(out, append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// mutationTableFiles inventories all retained physical Parquet objects under
// the table prefix, including files retained only by historical snapshots.
func mutationTableFiles(t *testing.T) (dataCount, deleteCount int, dataBytes, deleteBytes int64) {
	t.Helper()
	client := newTestS3Client(t)
	ctx := context.Background()
	var token *string
	prefix := s3Prefix + "/public/mutation_size_bench/data/"
	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s3Bucket), Prefix: aws.String(prefix), ContinuationToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, obj := range out.Contents {
			if strings.HasSuffix(aws.ToString(obj.Key), "-delete.parquet") {
				deleteCount++
				deleteBytes += aws.ToInt64(obj.Size)
			} else if strings.HasSuffix(aws.ToString(obj.Key), ".parquet") {
				dataCount++
				dataBytes += aws.ToInt64(obj.Size)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}
	return
}

func extrapolate100GiB(results []mutationSizeResult) []linearProjection {
	const target = 100 * 1024
	const maxHoldoutError = 0.20
	metrics := []struct {
		name  string
		value func(mutationSizeResult) float64
	}{
		{"writer_flush_duration_ms", func(r mutationSizeResult) float64 { return float64(r.WriterFlushDurationMS) }},
		{"logical_storage_get_bytes", func(r mutationSizeResult) float64 { return float64(r.LogicalStorageGetBytes) }},
		{"logical_storage_put_bytes", func(r mutationSizeResult) float64 { return float64(r.LogicalStoragePutBytes) }},
	}
	var projections []linearProjection
	for _, mode := range []string{"cow", "mor"} {
		for _, metric := range metrics {
			var x, y []float64
			for _, size := range []int{10, 100, 1024} {
				var samples []float64
				for _, result := range results {
					if result.Mode == mode && result.TargetMiB == size {
						samples = append(samples, metric.value(result))
					}
				}
				if len(samples) == 0 {
					continue
				}
				x = append(x, float64(size))
				y = append(y, medianFloat64(samples))
			}
			if len(x) != 3 || y[2] <= 0 {
				continue
			}
			holdoutSlope, holdoutIntercept, _ := linearFit(x[:2], y[:2])
			holdoutPrediction := holdoutIntercept + holdoutSlope*x[2]
			holdoutError := math.Abs(holdoutPrediction-y[2]) / y[2]
			if math.IsNaN(holdoutError) || math.IsInf(holdoutError, 0) || holdoutError > maxHoldoutError {
				continue
			}
			slope, intercept, r2 := linearFit(x, y)
			predicted := intercept + slope*target
			if predicted < 0 || math.IsNaN(predicted) || math.IsInf(predicted, 0) {
				continue
			}
			projections = append(projections, linearProjection{
				Mode: mode, Metric: metric.name, TargetMiB: target, ProjectedValue: predicted,
				SlopePerMiB: slope, Intercept: intercept, RSquared: r2,
				HoldoutRelativeError: holdoutError, Measured: false,
			})
		}
	}
	return projections
}

func TestMutationBenchmarkExtrapolationAcceptsValidatedLinearData(t *testing.T) {
	results := []mutationSizeResult{
		{Mode: "cow", TargetMiB: 10, WriterFlushDurationMS: 20, LogicalStorageGetBytes: 10, LogicalStoragePutBytes: 20},
		{Mode: "cow", TargetMiB: 100, WriterFlushDurationMS: 200, LogicalStorageGetBytes: 100, LogicalStoragePutBytes: 200},
		{Mode: "cow", TargetMiB: 1024, WriterFlushDurationMS: 2048, LogicalStorageGetBytes: 1024, LogicalStoragePutBytes: 2048},
	}
	projections := extrapolate100GiB(results)
	if len(projections) != 3 {
		t.Fatalf("projections=%d, want 3", len(projections))
	}
	for _, p := range projections {
		if p.Measured || p.TargetMiB != 100*1024 || p.RSquared < 0.999 || p.HoldoutRelativeError > 0.20 {
			t.Fatalf("unexpected projection: %+v", p)
		}
	}
}

func TestMutationBenchmarkExtrapolationRejectsFailedHoldout(t *testing.T) {
	results := []mutationSizeResult{
		{Mode: "cow", TargetMiB: 10, WriterFlushDurationMS: 10, LogicalStorageGetBytes: 10, LogicalStoragePutBytes: 10},
		{Mode: "cow", TargetMiB: 100, WriterFlushDurationMS: 100, LogicalStorageGetBytes: 100, LogicalStoragePutBytes: 100},
		{Mode: "cow", TargetMiB: 1024, WriterFlushDurationMS: 100000, LogicalStorageGetBytes: 100000, LogicalStoragePutBytes: 100000},
	}
	if projections := extrapolate100GiB(results); len(projections) != 0 {
		t.Fatalf("failed holdout must suppress projections: %+v", projections)
	}
}

func durationPercentileMS(values []time.Duration, percentile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index].Milliseconds()
}

func medianFloat64(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[middle-1] + sorted[middle]) / 2
	}
	return sorted[middle]
}

func collectBenchmarkMetadata(repetitions, updatedRows, queryRuns int) benchmarkMetadata {
	metadata := benchmarkMetadata{
		TimestampUTC: time.Now().UTC().Format(time.RFC3339), GoVersion: runtime.Version(),
		OS: runtime.GOOS, Arch: runtime.GOARCH, Repetitions: repetitions, UpdatedRows: updatedRows, QueryRuns: queryRuns,
	}
	if output, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
		metadata.GitCommit = strings.TrimSpace(string(output))
	}
	if output, err := exec.Command("git", "status", "--porcelain").Output(); err == nil {
		metadata.GitDirty = len(output) > 0
	}
	return metadata
}

func linearFit(x, y []float64) (slope, intercept, r2 float64) {
	var sx, sy float64
	for i := range x {
		sx += x[i]
		sy += y[i]
	}
	mx, my := sx/float64(len(x)), sy/float64(len(y))
	var cov, varx, sst, sse float64
	for i := range x {
		cov += (x[i] - mx) * (y[i] - my)
		varx += (x[i] - mx) * (x[i] - mx)
		sst += (y[i] - my) * (y[i] - my)
	}
	if varx == 0 {
		return 0, my, 0
	}
	slope = cov / varx
	intercept = my - slope*mx
	for i := range x {
		d := y[i] - (intercept + slope*x[i])
		sse += d * d
	}
	if sst == 0 {
		return slope, intercept, 1
	}
	r2 = 1 - sse/sst
	if math.IsNaN(r2) {
		r2 = 0
	}
	return
}
