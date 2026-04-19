package oracle

import (
	"context"
	"database/sql"
	"fmt"
)

// PgbenchBalanceInvariant asserts an intrinsic property of pgbench data:
//   SUM(pgbench_tellers.tbalance) = SUM(pgbench_branches.bbalance)
//
// Every pgbench transaction adjusts one teller and its owning branch by the
// same delta, so the two sums track each other exactly. The invariant is
// evaluated on Iceberg alone — no Postgres round-trip — which makes it a
// cheap self-consistency check to pair with the row-level diff.
//
// IMPORTANT: this check is only valid AFTER the validator has confirmed per-
// table catch-up via sentinels. Streambed does not preserve Postgres
// transaction boundaries, so a mid-flight flush can legitimately show an
// imbalance. Run this inside Validator.Invariants, which the validator
// evaluates only after the catch-up gate succeeds.
type PgbenchBalanceInvariant struct {
	S3Bucket string
	S3Prefix string
}

func (PgbenchBalanceInvariant) Name() string { return "pgbench_balance" }

func (p PgbenchBalanceInvariant) Check(ctx context.Context, duckDB *sql.DB) error {
	tellers := fmt.Sprintf("iceberg_scan('s3://%s/%s/public/pgbench_tellers', allow_moved_paths = true)",
		p.S3Bucket, p.S3Prefix)
	branches := fmt.Sprintf("iceberg_scan('s3://%s/%s/public/pgbench_branches', allow_moved_paths = true)",
		p.S3Bucket, p.S3Prefix)
	// The pgbench_accounts row we plant as the sentinel has aid=-1 and never
	// contributes to tellers or branches, so no exclusion is needed here.
	query := fmt.Sprintf(`
		SELECT
		  (SELECT COALESCE(SUM(tbalance), 0) FROM %s)
		- (SELECT COALESCE(SUM(bbalance), 0) FROM %s)
		AS delta`, tellers, branches)

	var delta int64
	if err := duckDB.QueryRowContext(ctx, query).Scan(&delta); err != nil {
		return fmt.Errorf("query: %w", err)
	}
	if delta != 0 {
		return fmt.Errorf("SUM(tbalance) - SUM(bbalance) = %d (expected 0)", delta)
	}
	return nil
}
