//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestPgbenchQueryLatency sets up the pgbench schema (accounts, branches,
// tellers, history), populates it, syncs everything to Iceberg, and then
// runs a suite of analytical queries against both Postgres and DuckDB/Iceberg.
// NOTE: This test is slow (~2min) and is intended for manual runs, not CI.
// For the full query suite, use: scripts/bench-pgbench.sql
func TestPgbenchQueryLatency(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)

	// ---------------------------------------------------------------
	// 1. Create pgbench-like schema
	// ---------------------------------------------------------------
	for _, ddl := range []string{
		"DROP TABLE IF EXISTS pgbench_history",
		"DROP TABLE IF EXISTS pgbench_tellers",
		"DROP TABLE IF EXISTS pgbench_accounts",
		"DROP TABLE IF EXISTS pgbench_branches",
		`CREATE TABLE pgbench_branches (
			bid    INTEGER PRIMARY KEY,
			bbalance INTEGER NOT NULL DEFAULT 0,
			filler   TEXT
		)`,
		`CREATE TABLE pgbench_accounts (
			aid      INTEGER PRIMARY KEY,
			bid      INTEGER NOT NULL,
			abalance INTEGER NOT NULL DEFAULT 0,
			filler   TEXT
		)`,
		`CREATE TABLE pgbench_tellers (
			tid      INTEGER PRIMARY KEY,
			bid      INTEGER NOT NULL,
			tbalance INTEGER NOT NULL DEFAULT 0,
			filler   TEXT
		)`,
		`CREATE TABLE pgbench_history (
			tid    INTEGER NOT NULL,
			bid    INTEGER NOT NULL,
			aid    INTEGER NOT NULL,
			delta  INTEGER NOT NULL,
			mtime  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			filler TEXT
		)`,
	} {
		execSQL(t, ddl)
	}

	createSlotAndPublication(t)

	// ---------------------------------------------------------------
	// 2. Populate — scale factor ~10 (100k accounts)
	// ---------------------------------------------------------------
	const (
		numBranches = 10
		numTellers  = 100
		numAccounts = 100000
		numHistory  = 200000
	)

	t.Logf("populating pgbench schema: %d branches, %d tellers, %d accounts, %d history rows...",
		numBranches, numTellers, numAccounts, numHistory)

	// Branches
	{
		var sb strings.Builder
		sb.WriteString("INSERT INTO pgbench_branches (bid, bbalance, filler) VALUES ")
		for i := 1; i <= numBranches; i++ {
			if i > 1 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("(%d, %d, '%s')", i, i*100000, "branch_filler_padding"))
		}
		execSQL(t, sb.String())
	}

	// Tellers
	{
		var sb strings.Builder
		sb.WriteString("INSERT INTO pgbench_tellers (tid, bid, tbalance, filler) VALUES ")
		for i := 1; i <= numTellers; i++ {
			if i > 1 {
				sb.WriteString(", ")
			}
			bid := (i-1)%numBranches + 1
			sb.WriteString(fmt.Sprintf("(%d, %d, %d, '%s')", i, bid, i*1000, "teller_filler"))
		}
		execSQL(t, sb.String())
	}

	// Accounts (batch insert)
	{
		conn, err := pgconn.Connect(ctx, pgConnStr())
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		r := conn.Exec(ctx, "BEGIN")
		r.ReadAll()
		for i := 0; i < numAccounts; i += 5000 {
			batchSize := 5000
			if i+batchSize > numAccounts {
				batchSize = numAccounts - i
			}
			var sb strings.Builder
			sb.WriteString("INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES ")
			for j := 0; j < batchSize; j++ {
				aid := i + j + 1
				bid := (aid-1)%numBranches + 1
				if j > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(fmt.Sprintf("(%d, %d, %d, '%s')",
					aid, bid, (aid*37)%200000-100000, "account_filler_for_padding"))
			}
			r := conn.Exec(ctx, sb.String())
			if _, err := r.ReadAll(); err != nil {
				t.Fatalf("insert accounts at %d: %v", i, err)
			}
		}
		r = conn.Exec(ctx, "COMMIT")
		r.ReadAll()
		conn.Close(ctx)
	}

	// History (batch insert)
	{
		conn, err := pgconn.Connect(ctx, pgConnStr())
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		r := conn.Exec(ctx, "BEGIN")
		r.ReadAll()
		for i := 0; i < numHistory; i += 5000 {
			batchSize := 5000
			if i+batchSize > numHistory {
				batchSize = numHistory - i
			}
			var sb strings.Builder
			sb.WriteString("INSERT INTO pgbench_history (tid, bid, aid, delta, filler) VALUES ")
			for j := 0; j < batchSize; j++ {
				row := i + j
				aid := row%numAccounts + 1
				bid := (aid-1)%numBranches + 1
				tid := row%numTellers + 1
				delta := (row*73)%20001 - 10000 // range [-10000, 10000]
				if j > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(fmt.Sprintf("(%d, %d, %d, %d, '%s')",
					tid, bid, aid, delta, "history_filler"))
			}
			r := conn.Exec(ctx, sb.String())
			if _, err := r.ReadAll(); err != nil {
				t.Fatalf("insert history at %d: %v", i, err)
			}
		}
		r = conn.Exec(ctx, "COMMIT")
		r.ReadAll()
		conn.Close(ctx)
	}

	// ---------------------------------------------------------------
	// 3. Sync to Iceberg
	// ---------------------------------------------------------------
	t.Log("syncing pgbench tables to Iceberg...")
	runSync(t, ctx, 120*time.Second)

	// Verify row counts.
	for _, tc := range []struct {
		table    string
		expected int64
	}{
		{"pgbench_branches", numBranches},
		{"pgbench_tellers", numTellers},
		{"pgbench_accounts", numAccounts},
		{"pgbench_history", numHistory},
	} {
		got := countNamedTableSnapshotRows(t, "public", tc.table)
		t.Logf("  %s: %d rows (expected %d)", tc.table, got, tc.expected)
		if got != tc.expected {
			t.Fatalf("%s: expected %d, got %d", tc.table, tc.expected, got)
		}
	}

	// ---------------------------------------------------------------
	// 4. Set up DuckDB for Iceberg queries
	// ---------------------------------------------------------------
	duckDB := newTestDuckDB(t)

	scan := func(table string) string {
		return fmt.Sprintf("iceberg_scan('s3://%s/%s/public/%s', allow_moved_paths = true)", s3Bucket, s3Prefix, table)
	}

	// Create DuckDB views so queries read naturally.
	for _, table := range []string{"pgbench_accounts", "pgbench_branches", "pgbench_tellers", "pgbench_history"} {
		stmt := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s", table, scan(table))
		if _, err := duckDB.Exec(stmt); err != nil {
			t.Fatalf("create view %s: %v", table, err)
		}
	}

	// ---------------------------------------------------------------
	// 5. Queries — simple to monster
	// ---------------------------------------------------------------
	queries := []struct {
		name string
		sql  string
	}{
		// --- Single-table scans ---
		{
			name: "Q1: COUNT accounts",
			sql:  "SELECT count(*) FROM pgbench_accounts",
		},
		{
			name: "Q2: SUM all balances",
			sql:  "SELECT sum(abalance) FROM pgbench_accounts",
		},
		{
			name: "Q3: Balance histogram",
			sql: `SELECT
				CASE
					WHEN abalance < -50000 THEN 'very_negative'
					WHEN abalance < 0      THEN 'negative'
					WHEN abalance < 50000  THEN 'positive'
					ELSE 'very_positive'
				END AS bucket,
				count(*) AS cnt,
				avg(abalance) AS avg_bal
			FROM pgbench_accounts
			GROUP BY 1 ORDER BY 1`,
		},
		// --- Aggregation on history ---
		{
			name: "Q4: History delta stats",
			sql: `SELECT
				count(*) AS txns,
				sum(delta) AS total_delta,
				avg(delta) AS avg_delta,
				min(delta) AS min_delta,
				max(delta) AS max_delta
			FROM pgbench_history`,
		},
		{
			name: "Q5: Top 10 accounts by txn count",
			sql: `SELECT aid, count(*) AS txn_count, sum(delta) AS net_delta
			FROM pgbench_history
			GROUP BY aid
			ORDER BY txn_count DESC
			LIMIT 10`,
		},
		// --- Two-table joins ---
		{
			name: "Q6: Branch totals (join accounts)",
			sql: `SELECT b.bid, b.bbalance,
				count(a.aid) AS num_accounts,
				sum(a.abalance) AS total_abalance,
				avg(a.abalance) AS avg_abalance
			FROM pgbench_branches b
			JOIN pgbench_accounts a ON a.bid = b.bid
			GROUP BY b.bid, b.bbalance
			ORDER BY b.bid`,
		},
		{
			name: "Q7: History per branch",
			sql: `SELECT h.bid,
				count(*) AS txn_count,
				sum(h.delta) AS total_delta,
				avg(h.delta) AS avg_delta
			FROM pgbench_history h
			GROUP BY h.bid
			ORDER BY total_delta DESC`,
		},
		// --- Three-table joins ---
		{
			name: "Q8: Teller activity with branch",
			sql: `SELECT t.tid, b.bid, b.bbalance,
				count(h.aid) AS txn_count,
				sum(h.delta) AS total_delta
			FROM pgbench_tellers t
			JOIN pgbench_branches b ON b.bid = t.bid
			JOIN pgbench_history h ON h.tid = t.tid
			GROUP BY t.tid, b.bid, b.bbalance
			ORDER BY txn_count DESC
			LIMIT 20`,
		},
		// --- Four-table join (the monster) ---
		{
			name: "Q9: Full 4-table join — account activity report",
			sql: `SELECT
				b.bid AS branch_id,
				b.bbalance AS branch_balance,
				count(DISTINCT a.aid) AS active_accounts,
				count(h.delta) AS total_txns,
				sum(h.delta) AS net_flow,
				avg(h.delta) AS avg_txn_size,
				sum(CASE WHEN h.delta > 0 THEN h.delta ELSE 0 END) AS credits,
				sum(CASE WHEN h.delta < 0 THEN h.delta ELSE 0 END) AS debits,
				count(DISTINCT h.tid) AS active_tellers
			FROM pgbench_branches b
			JOIN pgbench_accounts a ON a.bid = b.bid
			JOIN pgbench_history h ON h.aid = a.aid
			JOIN pgbench_tellers t ON t.tid = h.tid AND t.bid = b.bid
			GROUP BY b.bid, b.bbalance
			ORDER BY total_txns DESC`,
		},
		// --- Subquery + window function style ---
		{
			name: "Q10: Accounts with above-avg txn volume",
			sql: `SELECT aid, txn_count, net_delta
			FROM (
				SELECT aid,
					count(*) AS txn_count,
					sum(delta) AS net_delta
				FROM pgbench_history
				GROUP BY aid
			) sub
			WHERE txn_count > (
				SELECT avg(c) FROM (
					SELECT count(*) AS c FROM pgbench_history GROUP BY aid
				) avg_sub
			)
			ORDER BY txn_count DESC
			LIMIT 20`,
		},
		// --- Heavy aggregation ---
		{
			name: "Q11: Branch P50/P95 account balance",
			sql: `SELECT bid,
				count(*) AS num_accounts,
				percentile_cont(0.5) WITHIN GROUP (ORDER BY abalance) AS p50_balance,
				percentile_cont(0.95) WITHIN GROUP (ORDER BY abalance) AS p95_balance,
				min(abalance) AS min_balance,
				max(abalance) AS max_balance
			FROM pgbench_accounts
			GROUP BY bid
			ORDER BY bid`,
		},
	}

	// PG connection for queries.
	pgConn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect for queries: %v", err)
	}
	defer pgConn.Close(ctx)

	const runs = 3

	t.Logf("\n%-50s %15s %15s %10s", "Query", "Postgres", "DuckDB", "Speedup")
	t.Logf("%-50s %15s %15s %10s",
		strings.Repeat("-", 50), strings.Repeat("-", 15), strings.Repeat("-", 15), strings.Repeat("-", 10))

	for _, q := range queries {
		// Warm up.
		pgConn.Exec(ctx, q.sql).ReadAll()
		func() {
			rows, err := duckDB.QueryContext(ctx, q.sql)
			if err != nil {
				t.Logf("SKIP %s (DuckDB error: %v)", q.name, err)
				return
			}
			for rows.Next() {
			}
			rows.Close()
		}()

		// Benchmark Postgres.
		var pgTotal time.Duration
		for i := 0; i < runs; i++ {
			start := time.Now()
			result := pgConn.Exec(ctx, q.sql)
			result.ReadAll()
			pgTotal += time.Since(start)
		}
		pgAvg := pgTotal / runs

		// Benchmark DuckDB.
		var duckTotal time.Duration
		var duckErr error
		for i := 0; i < runs; i++ {
			start := time.Now()
			rows, err := duckDB.QueryContext(ctx, q.sql)
			if err != nil {
				duckErr = err
				break
			}
			for rows.Next() {
			}
			rows.Close()
			duckTotal += time.Since(start)
		}
		if duckErr != nil {
			t.Logf("%-50s %15s %15s %10s", q.name, pgAvg, "ERROR", "N/A")
			continue
		}
		duckAvg := duckTotal / runs

		// Speedup: >1 means DuckDB is faster.
		speedup := float64(pgAvg) / float64(duckAvg)
		t.Logf("%-50s %15s %15s %9.2fx", q.name, pgAvg, duckAvg, speedup)
	}

	cleanup(t)
	execSQL(t, "DROP TABLE IF EXISTS pgbench_history")
	execSQL(t, "DROP TABLE IF EXISTS pgbench_tellers")
	execSQL(t, "DROP TABLE IF EXISTS pgbench_accounts")
	execSQL(t, "DROP TABLE IF EXISTS pgbench_branches")
}
