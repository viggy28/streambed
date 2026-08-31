//go:build integration

package querycompat

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

const fixtureValueCount = 5

func cleanupSource(t *testing.T, conn *pgx.Conn, slot string) {
	t.Helper()
	ctx := context.Background()
	if slot != "" {
		_, _ = conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name)
			FROM pg_replication_slots WHERE slot_name = $1 AND active = false`, slot)
		_, _ = conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{slot}.Sanitize())
	}
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS oracle_values")
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS oracle_groups")
}

func createFixtureSchema(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	statements := []string{
		`CREATE TABLE oracle_groups (
			id   INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			tier INTEGER
		)`,
		`CREATE TABLE oracle_values (
			id                  INTEGER PRIMARY KEY,
			group_id            INTEGER,
			bool_value          BOOLEAN,
			int2_value          SMALLINT,
			int4_value          INTEGER,
			int8_value          BIGINT,
			numeric_value       NUMERIC(18,4),
			unconstrained_value NUMERIC,
			real_value          REAL,
			double_value        DOUBLE PRECISION,
			text_value          TEXT,
			varchar_value       VARCHAR(32),
			date_value          DATE,
			timestamp_value     TIMESTAMP,
			timestamptz_value   TIMESTAMPTZ,
			uuid_value          UUID,
			bytea_value         BYTEA,
			jsonb_value         JSONB
		)`,
	}
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("create query compatibility fixture: %v\n%s", err, statement)
		}
	}
}

func insertFixture(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin fixture insert: %v", err)
	}
	defer tx.Rollback(ctx)

	statements := []string{
		`INSERT INTO oracle_groups (id, name, tier) VALUES
			(1, 'alpha', 1),
			(2, 'βeta', 1),
			(3, 'empty', NULL)`,
		`INSERT INTO oracle_values VALUES
			(1, 1, true,  -12, 123456,  9007199254740991,
			 12.5000, 12345678901234567890.12345, 1.25,  0.125,
			 'hello', 'apple', DATE '2024-02-29',
			 TIMESTAMP '2024-01-02 03:04:05.123456',
			 TIMESTAMPTZ '2024-01-02 03:04:05.123456+00',
			 '550e8400-e29b-41d4-a716-446655440000', decode('00DEADFF', 'hex'),
			 '{"a":1,"tags":["x","y"]}'),
			(2, 1, false, 0, -1, -9007199254740991,
			 -0.0100, -0.00000000000000000001, -1.25, -0.125,
			 '', '', DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00',
			 TIMESTAMPTZ '2024-01-01 23:00:00-05',
			 '00000000-0000-0000-0000-000000000000', decode('', 'hex'),
			 '{"a":null}'),
			(3, 2, NULL, NULL, NULL, NULL,
			 NULL, NULL, NULL, NULL,
			 'Unicode: café 🚀', NULL, NULL, NULL, NULL,
			 NULL, NULL, NULL),
			(4, NULL, true, 12, 123456, 42,
			 12.5000, 100, 1.25, 0.125,
			 'hello', 'banana', DATE '2024-02-29',
			 TIMESTAMP '2024-01-02 03:04:05.123456',
			 TIMESTAMPTZ '2024-01-02 03:04:05.123456+00',
			 '550e8400-e29b-41d4-a716-446655440001', decode('00DEADFF', 'hex'),
			 '{"tags":["y","x"],"a":1}'),
			(5, 2, false, 7, 0, 0,
			 0.0000, 0, 0, 0,
			 'zebra', 'zebra', DATE '2000-01-01',
			 TIMESTAMP '2000-01-01 12:30:00',
			 TIMESTAMPTZ '2000-01-01 12:30:00+00',
			 '550e8400-e29b-41d4-a716-446655440002', decode('FF', 'hex'),
			 '[]')`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatalf("insert query compatibility fixture: %v\n%s", err, statement)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit fixture insert: %v", err)
	}

	var commitLSN string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&commitLSN); err != nil {
		t.Fatalf("read fixture commit LSN: %v", err)
	}
	return commitLSN
}
