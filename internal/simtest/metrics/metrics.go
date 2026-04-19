// Package metrics collects simtest observability data — workload throughput,
// oracle results, replication lag, supervisor restarts — and emits them to
// both stderr (human-readable) and a JSONL file (machine-readable).
//
// All counters are additive; snapshots are taken on a periodic tick. A single
// Collector is shared across all simtest components; concurrent-safe.
package metrics

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"runtime"
	"sync"
	"time"
)

// Collector is the central metrics registry. Zero value is not usable — use New.
type Collector struct {
	mu sync.Mutex

	// Cumulative counters (monotonic).
	rowsByWorkload    map[string]int64
	streambedRestarts int64
	oracleChecks      int64
	oracleFailures    int64

	// Latest sampled values.
	lastOracleReport     oracleSample
	lastReplicationLag   int64
	lastReplicationCheck time.Time

	// Output sinks.
	jsonl  *jsonlWriter
	stderr *slog.Logger
	start  time.Time
}

type oracleSample struct {
	tickID         int64
	durationMs     int64
	discrepancies  int
	catchUpOK      bool
	invariantFails int
}

// New builds a Collector that writes JSONL events to the given path. The file
// is created in append mode; timestamped filenames are the caller's job.
func New(jsonlPath string, logger *slog.Logger) (*Collector, error) {
	jw, err := newJSONL(jsonlPath)
	if err != nil {
		return nil, err
	}
	return &Collector{
		rowsByWorkload: make(map[string]int64),
		jsonl:          jw,
		stderr:         logger,
		start:          time.Now(),
	}, nil
}

// Close flushes any buffered JSONL output.
func (c *Collector) Close() error {
	return c.jsonl.Close()
}

// Snapshot is a consistent point-in-time view of the key counters. Returned
// by-value so callers are safe from later mutations.
type Snapshot struct {
	Uptime            time.Duration
	RowsByWorkload    map[string]int64
	StreambedRestarts int64
	OracleChecks      int64
	OracleFailures    int64
	ReplicationLag    int64
}

// Snapshot returns the current counter values atomically.
func (c *Collector) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Snapshot{
		Uptime:            time.Since(c.start),
		RowsByWorkload:    copyInt64Map(c.rowsByWorkload),
		StreambedRestarts: c.streambedRestarts,
		OracleChecks:      c.oracleChecks,
		OracleFailures:    c.oracleFailures,
		ReplicationLag:    c.lastReplicationLag,
	}
}

// AddRows records n rows written by the named workload.
func (c *Collector) AddRows(workload string, n int) {
	c.mu.Lock()
	c.rowsByWorkload[workload] += int64(n)
	c.mu.Unlock()
}

// RecordStreambedRestart bumps the supervisor restart counter.
func (c *Collector) RecordStreambedRestart(reason string) {
	c.mu.Lock()
	c.streambedRestarts++
	c.mu.Unlock()
	c.jsonl.write(map[string]any{
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
		"event": "streambed_restart",
		"reason": reason,
	})
}

// RecordOracle records one oracle tick's outcome.
func (c *Collector) RecordOracle(tickID int64, durationMs int64, totalDiscrepancies int, catchUpOK bool, invariantFails int) {
	c.mu.Lock()
	c.oracleChecks++
	ok := catchUpOK && totalDiscrepancies == 0 && invariantFails == 0
	if !ok {
		c.oracleFailures++
	}
	c.lastOracleReport = oracleSample{
		tickID:         tickID,
		durationMs:     durationMs,
		discrepancies:  totalDiscrepancies,
		catchUpOK:      catchUpOK,
		invariantFails: invariantFails,
	}
	c.mu.Unlock()

	c.jsonl.write(map[string]any{
		"ts":              time.Now().UTC().Format(time.RFC3339Nano),
		"event":           "oracle_tick",
		"tick_id":         tickID,
		"duration_ms":     durationMs,
		"discrepancies":   totalDiscrepancies,
		"catch_up_ok":     catchUpOK,
		"invariant_fails": invariantFails,
	})
}

// RecordReplicationLag records the current WAL lag in bytes.
func (c *Collector) RecordReplicationLag(bytes int64) {
	c.mu.Lock()
	c.lastReplicationLag = bytes
	c.lastReplicationCheck = time.Now()
	c.mu.Unlock()
}

// Heartbeat emits a periodic snapshot of the current metrics to both stderr
// and JSONL. Meant to be called by a ticker from the runner.
func (c *Collector) Heartbeat() {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	c.mu.Lock()
	uptime := time.Since(c.start).Truncate(time.Second)
	snapshot := map[string]any{
		"ts":                 time.Now().UTC().Format(time.RFC3339Nano),
		"event":              "heartbeat",
		"uptime_s":           int64(uptime.Seconds()),
		"rows_by_workload":   copyInt64Map(c.rowsByWorkload),
		"streambed_restarts": c.streambedRestarts,
		"oracle_checks":      c.oracleChecks,
		"oracle_failures":    c.oracleFailures,
		"last_oracle": map[string]any{
			"tick_id":         c.lastOracleReport.tickID,
			"duration_ms":     c.lastOracleReport.durationMs,
			"discrepancies":   c.lastOracleReport.discrepancies,
			"catch_up_ok":     c.lastOracleReport.catchUpOK,
			"invariant_fails": c.lastOracleReport.invariantFails,
		},
		"replication_lag_bytes": c.lastReplicationLag,
		"memory_alloc_mb":       mem.Alloc / (1024 * 1024),
		"goroutines":            runtime.NumGoroutine(),
	}
	c.mu.Unlock()

	c.jsonl.write(snapshot)

	// Human summary for stderr.
	c.stderr.Info("simtest heartbeat",
		"uptime", uptime,
		"rows", snapshot["rows_by_workload"],
		"streambed_restarts", snapshot["streambed_restarts"],
		"oracle_checks", snapshot["oracle_checks"],
		"oracle_failures", snapshot["oracle_failures"],
		"lag_bytes", snapshot["replication_lag_bytes"],
		"mem_mb", snapshot["memory_alloc_mb"],
	)
}

// StartHeartbeat launches a goroutine that calls Heartbeat every interval
// until ctx is done.
func (c *Collector) StartHeartbeat(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.Heartbeat()
			}
		}
	}()
}

func copyInt64Map(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	maps.Copy(out, m)
	return out
}

// jsonlWriter serializes metrics events to a line-per-event file. Buffered
// writes are flushed on Close; each event is self-contained so a crash mid-run
// loses at most the last buffer's worth of data.
type jsonlWriter struct {
	mu  sync.Mutex
	f   *os.File
	w   *bufio.Writer
	enc *json.Encoder
}

func newJSONL(path string) (*jsonlWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open jsonl %s: %w", path, err)
	}
	w := bufio.NewWriterSize(f, 64*1024)
	enc := json.NewEncoder(w)
	return &jsonlWriter{f: f, w: w, enc: enc}, nil
}

func (j *jsonlWriter) write(event map[string]any) {
	j.mu.Lock()
	defer j.mu.Unlock()
	_ = j.enc.Encode(event)
}

func (j *jsonlWriter) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.w.Flush(); err != nil {
		j.f.Close()
		return err
	}
	return j.f.Close()
}
