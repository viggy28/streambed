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
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS replication_state (
			slot_name    TEXT PRIMARY KEY,
			flushed_lsn  TEXT NOT NULL,
			updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS synced_tables (
			schema_name  TEXT NOT NULL,
			table_name   TEXT NOT NULL,
			column_count INTEGER NOT NULL,
			first_seen   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			last_flush   TIMESTAMP,
			last_flush_lsn TEXT,
			PRIMARY KEY (schema_name, table_name)
		);
	`)
	return err
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
	`, schema, table, columnCount, columnCount, lsn.String())
	return err
}

func (s *Store) UpdateLastFlush(lsn pglogrepl.LSN, schema, table string) error {
	_, err := s.db.Exec(`
		UPDATE synced_tables SET last_flush = ?, last_flush_lsn = ? WHERE schema_name = ? AND table_name = ?
	`, time.Now().UTC(), lsn.String(), schema, table)
	return err
}

func (s *Store) Close() error {
	return s.db.Close()
}
