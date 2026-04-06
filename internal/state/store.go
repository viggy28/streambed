package state

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

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

func (s *Store) GetFlushedLSN() (map[string]string, error) {
	rows, err := s.db.Query(
		"SELECT table_name, last_flush_lsn FROM synced_tables where last_flush_lsn is NOT NULL",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tableFlushLSN := make(map[string]string)
	var tableName, last_flush_lsn string

	for rows.Next() {
		if err := rows.Scan(&tableName, &last_flush_lsn); err != nil {
			return nil, err
		}
		tableFlushLSN[tableName] = last_flush_lsn
	}

	return tableFlushLSN, nil
}

func (s *Store) GetSafestFlushedLSN() (pglogrepl.LSN, error) {
	var lsnStr string
	// ideally we should find the minimum of last_flush_lsn_position
	// since it's a string and need to be decoded to a position, I am using the last_flush timestamp
	err := s.db.QueryRow(
		"SELECT last_flush_lsn FROM synced_tables WHERE last_flush IS NOT NULL ORDER BY last_flush ASC LIMIT 1",
	).Scan(&lsnStr)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return 0, fmt.Errorf("parse LSN %q: %w", lsnStr, err)
	}
	return lsn, nil
}

func (s *Store) SetFlushedLSN(slotName string, lsn pglogrepl.LSN) error {
	_, err := s.db.Exec(`
		INSERT INTO replication_state (slot_name, flushed_lsn, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(slot_name) DO UPDATE SET flushed_lsn = ?, updated_at = ?
	`, slotName, lsn.String(), time.Now().UTC(), lsn.String(), time.Now().UTC())
	return err
}

func (s *Store) RegisterTable(schema, table string, columnCount int, lsn pglogrepl.LSN) error {
	_, err := s.db.Exec(`
		INSERT INTO synced_tables (schema_name, table_name, column_count, last_flush_lsn)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(schema_name, table_name) DO UPDATE SET column_count = ?
	`, schema, table, columnCount, lsn.String(), columnCount)
	return err
}

func (s *Store) UpdateLastFlush(lsn pglogrepl.LSN, schema, table string) error {
	_, err := s.db.Exec(`
		UPDATE synced_tables SET last_flush = ?, last_flush_lsn = ? WHERE schema_name = ? AND table_name = ?
	`, time.Now().UTC(), lsn.String(), schema, table)
	return err
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

func (s *Store) Close() error {
	return s.db.Close()
}
