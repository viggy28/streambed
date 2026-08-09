package state

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}

	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	if err := createTables(db); err != nil {
		db.Close()
		return nil, err
	}

	return &Store{db: db}, nil
}

func createTables(db *sql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS replication_state (
			slot_name    TEXT PRIMARY KEY,
			flushed_lsn  TEXT NOT NULL,
			updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS synced_tables (
			schema_name    TEXT NOT NULL,
			table_name     TEXT NOT NULL,
			column_count   INTEGER NOT NULL,
			first_seen     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			last_flush     TIMESTAMP,
			last_flush_lsn TEXT,
			PRIMARY KEY (schema_name, table_name)
		);
		CREATE TABLE IF NOT EXISTS table_commit_locks (
			schema_name TEXT NOT NULL,
			table_name  TEXT NOT NULL,
			owner       TEXT NOT NULL,
			expires_at  INTEGER NOT NULL,
			updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (schema_name, table_name)
		);
	`); err != nil {
		return err
	}

	// Phase 3: backfill_lsn records the LSN at which a resync COPY snapshot
	// was taken. The sync consumer uses it to suppress duplicate events from
	// the main replication slot whose WAL position falls at or before this
	// value. SQLite has no "IF NOT EXISTS" on ADD COLUMN so we tolerate the
	// duplicate-column error on repeat runs.
	if _, err := db.Exec(`ALTER TABLE synced_tables ADD COLUMN backfill_lsn TEXT`); err != nil {
		if !isDuplicateColumnErr(err) {
			return fmt.Errorf("add backfill_lsn column: %w", err)
		}
	}
	return nil
}

// isDuplicateColumnErr detects the SQLite error raised when ALTER TABLE ADD
// COLUMN is called for a column that already exists.
func isDuplicateColumnErr(err error) bool {
	if err == nil {
		return false
	}
	return containsCI(err.Error(), "duplicate column")
}

func containsCI(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		ok := true
		for j := 0; j < len(sub); j++ {
			a, b := s[i+j], sub[j]
			if a >= 'A' && a <= 'Z' {
				a += 'a' - 'A'
			}
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func (s *Store) RegisterTable(schema, table string, columnCount int) error {
	_, err := s.db.Exec(`
		INSERT INTO synced_tables (schema_name, table_name, column_count)
		VALUES (?, ?, ?)
		ON CONFLICT(schema_name, table_name) DO UPDATE SET column_count = ?
	`, schema, table, columnCount, columnCount)
	return err
}

// RegisteredTable holds a registered table's schema and name.
type RegisteredTable struct {
	Schema string
	Table  string
}

// GetRegisteredTables returns every table in synced_tables. Used at startup
// to enumerate tables whose flush LSN should be read from Iceberg.
func (s *Store) GetRegisteredTables() ([]RegisteredTable, error) {
	rows, err := s.db.Query(
		"SELECT schema_name, table_name FROM synced_tables",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RegisteredTable
	for rows.Next() {
		var t RegisteredTable
		if err := rows.Scan(&t.Schema, &t.Table); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// DeleteTable removes the synced_tables row for the given schema.table.
// Returns the number of rows deleted (0 if the table wasn't registered).
func (s *Store) DeleteTable(schema, table string) (int64, error) {
	res, err := s.db.Exec(
		"DELETE FROM synced_tables WHERE schema_name = ? AND table_name = ?",
		schema, table,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// SetBackfillLSN records the LSN at which a resync COPY snapshot was taken.
// The consumer uses this to filter out replay events whose WAL position is at
// or below this value (those rows are already in the COPY-sourced snapshot).
func (s *Store) SetBackfillLSN(schema, table string, lsn pglogrepl.LSN) error {
	_, err := s.db.Exec(
		`UPDATE synced_tables SET backfill_lsn = ? WHERE schema_name = ? AND table_name = ?`,
		lsn.String(), schema, table,
	)
	return err
}

// GetBackfillLSNs returns the current backfill_lsn filter for every registered
// table that has one. Consumers load this once at startup into an in-memory
// filter map; entries are cleared as events pass the filter LSN.
func (s *Store) GetBackfillLSNs() (map[string]pglogrepl.LSN, error) {
	rows, err := s.db.Query(
		`SELECT schema_name, table_name, backfill_lsn FROM synced_tables WHERE backfill_lsn IS NOT NULL`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]pglogrepl.LSN)
	for rows.Next() {
		var schema, table, lsnStr string
		if err := rows.Scan(&schema, &table, &lsnStr); err != nil {
			return nil, err
		}
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			return nil, fmt.Errorf("parse backfill_lsn %q for %s.%s: %w", lsnStr, schema, table, err)
		}
		out[schema+"."+table] = lsn
	}
	return out, rows.Err()
}

// ClearBackfillLSN removes the backfill filter for a table once the main-slot
// stream has advanced past it. Safe to call when no filter is set.
func (s *Store) ClearBackfillLSN(schema, table string) error {
	_, err := s.db.Exec(
		`UPDATE synced_tables SET backfill_lsn = NULL WHERE schema_name = ? AND table_name = ?`,
		schema, table,
	)
	return err
}

// TableCommitLock represents ownership of a short critical section that
// publishes Iceberg metadata for one table. The lock is local to the shared
// SQLite state database; all writers/maintenance processes for a table must
// use the same state DB for this to coordinate them.
type TableCommitLock struct {
	Schema string
	Table  string
	Owner  string
}

// AcquireTableCommitLock waits up to wait for a table-level commit lock. The
// lock expires after ttl so a crashed process does not block maintenance
// forever. The caller must release the returned lock.
func (s *Store) AcquireTableCommitLock(ctx context.Context, schema, table, ownerName string, ttl, wait time.Duration) (*TableCommitLock, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("commit lock ttl must be positive")
	}
	if wait < 0 {
		return nil, fmt.Errorf("commit lock wait must be non-negative")
	}
	if ownerName == "" {
		ownerName = "unknown"
	}
	owner := fmt.Sprintf("%s:%d:%s", ownerName, os.Getpid(), uuid.NewString())
	deadline := time.Now().Add(wait)
	for {
		now := time.Now()
		expires := now.Add(ttl).UnixMilli()
		res, err := s.db.ExecContext(ctx, `
			INSERT INTO table_commit_locks (schema_name, table_name, owner, expires_at, updated_at)
			VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
			ON CONFLICT(schema_name, table_name) DO UPDATE SET
				owner = excluded.owner,
				expires_at = excluded.expires_at,
				updated_at = CURRENT_TIMESTAMP
			WHERE table_commit_locks.expires_at < ?
		`, schema, table, owner, expires, now.UnixMilli())
		if err != nil {
			return nil, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			return &TableCommitLock{Schema: schema, Table: table, Owner: owner}, nil
		}
		if wait == 0 || time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out acquiring commit lock for %s.%s", schema, table)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// RefreshTableCommitLock verifies that the caller still owns an unexpired lock
// and extends its expiry. It returns false if the lock was stolen or expired.
func (s *Store) RefreshTableCommitLock(ctx context.Context, lock *TableCommitLock, ttl time.Duration) (bool, error) {
	if lock == nil {
		return false, nil
	}
	if ttl <= 0 {
		return false, fmt.Errorf("commit lock ttl must be positive")
	}
	now := time.Now()
	res, err := s.db.ExecContext(ctx, `
		UPDATE table_commit_locks
		SET expires_at = ?, updated_at = CURRENT_TIMESTAMP
		WHERE schema_name = ? AND table_name = ? AND owner = ? AND expires_at > ?
	`, now.Add(ttl).UnixMilli(), lock.Schema, lock.Table, lock.Owner, now.UnixMilli())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ReleaseTableCommitLock releases a lock if the caller still owns it.
func (s *Store) ReleaseTableCommitLock(ctx context.Context, lock *TableCommitLock) error {
	if lock == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM table_commit_locks
		WHERE schema_name = ? AND table_name = ? AND owner = ?
	`, lock.Schema, lock.Table, lock.Owner)
	return err
}

func (s *Store) Close() error {
	return s.db.Close()
}
