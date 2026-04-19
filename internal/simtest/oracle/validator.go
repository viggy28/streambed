package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// TableSpec describes one workload table the validator should verify.
type TableSpec struct {
	Schema      string
	Table       string
	KeyColumns  []string // composite primary key
	SentinelSQL string   // INSERT that places a sentinel row with `filler = $1`
	SentinelCol string   // column that the sentinel value is written into
}

// Invariant is a self-consistency check evaluated against Iceberg alone.
// Return non-nil error to signal a violation. Invariants are only valid after
// the per-table sentinel catch-up gate has succeeded — transient violations
// during normal pipeline operation are expected because Streambed does not
// preserve Postgres transaction boundaries.
type Invariant interface {
	Name() string
	Check(ctx context.Context, duckDB *sql.DB) error
}

// Result is one oracle tick's outcome.
type Result struct {
	TickID       int64
	StartedAt    time.Time
	FinishedAt   time.Time
	Reports      []DiffReport
	InvariantErr map[string]string // invariant name -> error text (if violated)
	CatchUpErr   string            // non-empty if sentinel never appeared
}

// OK reports whether the tick was clean — no discrepancies, no invariant
// violations, no catch-up failure.
func (r Result) OK() bool {
	if r.CatchUpErr != "" {
		return false
	}
	if len(r.InvariantErr) > 0 {
		return false
	}
	for _, rep := range r.Reports {
		if len(rep.Discrepancies) > 0 {
			return false
		}
	}
	return true
}

// Validator runs the oracle on a loop: plant per-table sentinels, wait for
// them to appear in Iceberg, diff row sets, evaluate invariants.
type Validator struct {
	PgConnStr       string // non-replication connection string
	DuckDB          *sql.DB
	S3Bucket        string
	S3Prefix        string
	Tables          []TableSpec
	Invariants      []Invariant
	SentinelTimeout time.Duration // how long to wait for each sentinel
	Logger          *slog.Logger
}

// RunOnce performs a single oracle tick: sentinel -> wait -> diff -> invariants.
func (v *Validator) RunOnce(ctx context.Context, tickID int64) Result {
	res := Result{
		TickID:       tickID,
		StartedAt:    time.Now(),
		InvariantErr: make(map[string]string),
	}
	defer func() { res.FinishedAt = time.Now() }()

	pg, err := pgconn.Connect(ctx, v.PgConnStr)
	if err != nil {
		res.CatchUpErr = fmt.Sprintf("connect postgres: %v", err)
		return res
	}
	defer pg.Close(ctx)

	sentinel := fmt.Sprintf("sim-oracle-tick-%d-%d", tickID, time.Now().UnixNano())

	// 1. Plant per-table sentinels.
	for _, ts := range v.Tables {
		if err := plantSentinel(ctx, pg, ts, sentinel); err != nil {
			res.CatchUpErr = fmt.Sprintf("plant sentinel in %s.%s: %v", ts.Schema, ts.Table, err)
			return res
		}
	}

	// 2. Wait for every sentinel to appear in Iceberg.
	for _, ts := range v.Tables {
		if err := v.waitForSentinel(ctx, ts, sentinel); err != nil {
			res.CatchUpErr = fmt.Sprintf("wait sentinel %s.%s: %v", ts.Schema, ts.Table, err)
			return res
		}
	}

	// 3. Diff each table (excluding the sentinel row from both sides via a
	// post-filter — see filteredDiff).
	for _, ts := range v.Tables {
		report, err := v.diffTable(ctx, pg, ts, sentinel)
		if err != nil {
			res.CatchUpErr = fmt.Sprintf("diff %s.%s: %v", ts.Schema, ts.Table, err)
			return res
		}
		res.Reports = append(res.Reports, report)
	}

	// 4. Invariants.
	for _, inv := range v.Invariants {
		if err := inv.Check(ctx, v.DuckDB); err != nil {
			res.InvariantErr[inv.Name()] = err.Error()
		}
	}

	// 5. Clean up sentinels (best effort — a failure here doesn't invalidate
	// the check we just performed).
	for _, ts := range v.Tables {
		_ = removeSentinel(ctx, pg, ts, sentinel)
	}

	return res
}

func plantSentinel(ctx context.Context, pg *pgconn.PgConn, ts TableSpec, sentinel string) error {
	if ts.SentinelSQL == "" {
		return fmt.Errorf("TableSpec %s.%s has no SentinelSQL", ts.Schema, ts.Table)
	}
	sql := strings.ReplaceAll(ts.SentinelSQL, "$SENTINEL", sqlQuote(sentinel))
	r := pg.Exec(ctx, sql)
	_, err := r.ReadAll()
	return err
}

func removeSentinel(ctx context.Context, pg *pgconn.PgConn, ts TableSpec, sentinel string) error {
	sql := fmt.Sprintf("DELETE FROM %s.%s WHERE %s = %s",
		ts.Schema, ts.Table, ts.SentinelCol, sqlQuote(sentinel))
	r := pg.Exec(ctx, sql)
	_, err := r.ReadAll()
	return err
}

// waitForSentinel polls Iceberg until the sentinel row for this table appears
// or the sentinel timeout elapses.
func (v *Validator) waitForSentinel(ctx context.Context, ts TableSpec, sentinel string) error {
	deadline := time.Now().Add(v.SentinelTimeout)
	query := fmt.Sprintf(
		"SELECT 1 FROM iceberg_scan('s3://%s/%s/%s/%s', allow_moved_paths = true) WHERE %s = %s LIMIT 1",
		v.S3Bucket, v.S3Prefix, ts.Schema, ts.Table, ts.SentinelCol, sqlQuote(sentinel))

	pollInterval := 500 * time.Millisecond
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("sentinel not visible after %s", v.SentinelTimeout)
		}
		var one int
		err := v.DuckDB.QueryRowContext(ctx, query).Scan(&one)
		if err == nil {
			return nil
		}
		// Iceberg table may not exist yet on the very first tick, or DuckDB
		// may race with a snapshot swap. Sleep and retry.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// diffTable runs a full oracle comparison, filtering out the current tick's
// sentinel row from both sides so the diff only reports real workload rows.
func (v *Validator) diffTable(ctx context.Context, pg *pgconn.PgConn, ts TableSpec, sentinel string) (DiffReport, error) {
	pgRows, err := QueryPgRows(ctx, pg, ts.Schema, ts.Table, ts.KeyColumns)
	if err != nil {
		return DiffReport{}, err
	}
	iceRows, err := QueryIcebergRows(ctx, v.DuckDB, ts.Schema, ts.Table, ts.KeyColumns, v.S3Bucket, v.S3Prefix)
	if err != nil {
		return DiffReport{}, err
	}

	dropSentinel(pgRows, ts.SentinelCol, sentinel)
	dropSentinel(iceRows, ts.SentinelCol, sentinel)

	return DiffReport{
		Schema:        ts.Schema,
		Table:         ts.Table,
		PgRowCount:    len(pgRows),
		IceRowCount:   len(iceRows),
		Discrepancies: DiffRowSets(pgRows, iceRows, nil),
	}, nil
}

func dropSentinel(rows map[string]map[string]string, col, sentinel string) {
	for k, r := range rows {
		if r[col] == sentinel {
			delete(rows, k)
		}
	}
}

// sqlQuote wraps s in single quotes and escapes any single quotes inside.
// Good enough for sentinel strings we generate ourselves.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
