package ducklake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pglogrepl"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/wal"
)

const defaultCatalogName = "streambed"

type Config struct {
	CatalogPath  string
	CatalogStore string // DuckLake catalog store: "sqlite" (default) or "duckdb"
	DataPath     string
	S3Endpoint   string
	S3Region     string
	CatalogName  string
	ReadOnly     bool
}

type Writer struct {
	db            *sql.DB
	state         *state.Store
	flushRows     int
	flushInterval time.Duration
	logger        *slog.Logger
	cfg           Config
	catalogName   string
	buffers       map[string]*tableBuffer
	ensuredTables map[string]string
}

type tableBuffer struct {
	Schema     string
	Table      string
	Columns    []wal.Column
	KeyColumns []int
	Rows       [][]cell
	Deletes    [][]cell
	LastLSN    pglogrepl.LSN
	FirstLSN   pglogrepl.LSN
}

type cell struct {
	Data   []byte
	IsNull bool
}

func NewWriter(ctx context.Context, cfg Config, store *state.Store, flushRows int, flushInterval time.Duration, logger *slog.Logger) (*Writer, error) {
	if cfg.CatalogName == "" {
		cfg.CatalogName = defaultCatalogName
	}
	if err := os.MkdirAll(filepath.Dir(cfg.CatalogPath), 0o755); err != nil {
		return nil, fmt.Errorf("create ducklake catalog directory: %w", err)
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		db:            db,
		state:         store,
		flushRows:     flushRows,
		flushInterval: flushInterval,
		logger:        logger,
		cfg:           cfg,
		catalogName:   cfg.CatalogName,
		buffers:       make(map[string]*tableBuffer),
		ensuredTables: make(map[string]string),
	}
	return w, nil
}

func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := Configure(ctx, db, cfg); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func Configure(ctx context.Context, db *sql.DB, cfg Config) error {
	catalogStore := normalizedCatalogStore(cfg.CatalogStore)
	stmts := []string{
		"INSTALL ducklake",
		"LOAD ducklake",
	}
	if catalogStore == "sqlite" {
		stmts = append(stmts,
			"INSTALL sqlite",
			"LOAD sqlite",
		)
	}
	stmts = append(stmts,
		"INSTALL httpfs",
		"LOAD httpfs",
		"INSTALL icu",
		"LOAD icu",
	)
	if cfg.S3Region != "" {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_region = '%s'", strings.ReplaceAll(cfg.S3Region, "'", "''")))
	}
	if cfg.S3Endpoint != "" {
		endpoint := strings.TrimPrefix(strings.TrimPrefix(cfg.S3Endpoint, "http://"), "https://")
		stmts = append(stmts,
			fmt.Sprintf("SET GLOBAL s3_endpoint = '%s'", strings.ReplaceAll(endpoint, "'", "''")),
			"SET GLOBAL s3_url_style = 'path'",
			"SET GLOBAL s3_use_ssl = false",
		)
	}
	key := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if key == "" && cfg.S3Endpoint != "" {
		key = "minioadmin"
	}
	if secret == "" && cfg.S3Endpoint != "" {
		secret = "minioadmin"
	}
	if key != "" {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_access_key_id = '%s'", strings.ReplaceAll(key, "'", "''")))
	}
	if secret != "" {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_secret_access_key = '%s'", strings.ReplaceAll(secret, "'", "''")))
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}
	attachOptions := []string{
		fmt.Sprintf("DATA_PATH '%s'", strings.ReplaceAll(cfg.DataPath, "'", "''")),
	}
	if cfg.ReadOnly {
		attachOptions = append(attachOptions, "READ_ONLY")
	}
	attach := fmt.Sprintf("ATTACH '%s' AS %s (%s)",
		duckLakeAttachPath(cfg.CatalogPath, catalogStore),
		quoteIdent(catalogName(cfg)),
		strings.Join(attachOptions, ", "),
	)
	if _, err := db.ExecContext(ctx, attach); err != nil {
		return fmt.Errorf("attach ducklake catalog: %w", err)
	}
	return nil
}

func normalizedCatalogStore(store string) string {
	if strings.EqualFold(store, "duckdb") {
		return "duckdb"
	}
	return "sqlite"
}

func duckLakeAttachPath(catalogPath, catalogStore string) string {
	escapedPath := strings.ReplaceAll(catalogPath, "'", "''")
	if catalogStore == "duckdb" {
		return "ducklake:" + escapedPath
	}
	return "ducklake:sqlite:" + escapedPath
}

func catalogName(cfg Config) string {
	if cfg.CatalogName == "" {
		return defaultCatalogName
	}
	return cfg.CatalogName
}

func (w *Writer) Close() error {
	return w.db.Close()
}

func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	if _, err := w.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", qname(w.catalogName, schema, table))); err != nil {
		return err
	}
	delete(w.ensuredTables, schema+"."+table)
	return nil
}

func (w *Writer) HandleEvent(ctx context.Context, event wal.RowEvent) (bool, error) {
	if event.Op == wal.OpTruncate {
		return false, w.Truncate(ctx, event)
	}
	transitioned := w.buffer(event)
	key := event.Schema + "." + event.Table
	buf := w.buffers[key]
	if len(buf.Rows)+len(buf.Deletes) >= w.flushRows {
		if err := w.flush(ctx, key); err != nil {
			return false, err
		}
	}
	return transitioned, nil
}

func (w *Writer) HandleSchemaChange(ctx context.Context, rel *wal.RelationMessage, defaults map[string]string) error {
	key := rel.Namespace + "." + rel.Name
	if buf, exists := w.buffers[key]; exists && (len(buf.Rows) > 0 || len(buf.Deletes) > 0) {
		if err := w.flush(ctx, key); err != nil {
			return fmt.Errorf("pre-evolution flush for %s: %w", key, err)
		}
	}
	buf, exists := w.buffers[key]
	if !exists {
		buf = &tableBuffer{Schema: rel.Namespace, Table: rel.Name}
		w.buffers[key] = buf
	}
	tableExists, err := w.tableExists(ctx, rel.Namespace, rel.Name)
	if err != nil {
		return err
	}
	if tableExists && len(rel.Changes) > 0 {
		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := w.applySchemaChanges(ctx, tx, rel, defaults); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := w.setCommitMessage(ctx, tx, rel.Namespace, rel.Name, "schema", ""); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit schema evolution for %s: %w", key, err)
		}
		delete(w.ensuredTables, key)
	}
	buf.Columns = rel.Columns
	buf.KeyColumns = rel.KeyColumnIndexes
	if w.state != nil {
		if err := w.state.RegisterTable(rel.Namespace, rel.Name, len(rel.Columns)); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) buffer(event wal.RowEvent) bool {
	key := event.Schema + "." + event.Table
	buf, exists := w.buffers[key]
	if !exists {
		buf = &tableBuffer{
			Schema:     event.Schema,
			Table:      event.Table,
			Columns:    event.Columns,
			KeyColumns: event.KeyColumns,
			LastLSN:    event.WALStartLSN,
		}
		w.buffers[key] = buf
		if w.state != nil {
			if err := w.state.RegisterTable(event.Schema, event.Table, len(event.Columns)); err != nil {
				w.logger.Warn("register table failed", "table", key, "error", err)
			}
		}
		w.logger.Info("new ducklake table discovered", "schema", event.Schema, "table", event.Table)
	}
	if len(event.Columns) > 0 {
		buf.Columns = event.Columns
	}
	if len(event.KeyColumns) > 0 && len(buf.KeyColumns) == 0 {
		buf.KeyColumns = event.KeyColumns
	}
	wasEmpty := len(buf.Rows) == 0 && len(buf.Deletes) == 0
	if event.Op == wal.OpInsert || event.Op == wal.OpUpdate {
		row := make([]cell, len(event.Values))
		for i, v := range event.Values {
			row[i] = cell{Data: append([]byte(nil), v.Value...), IsNull: v.IsNull || v.IsUnchangedTOAST}
		}
		buf.Rows = append(buf.Rows, row)
	}
	if event.Op == wal.OpUpdate || event.Op == wal.OpDelete {
		keyRow := make([]cell, len(event.OldKey))
		for i, v := range event.OldKey {
			keyRow[i] = cell{Data: append([]byte(nil), v.Value...), IsNull: v.IsNull}
		}
		buf.Deletes = append(buf.Deletes, keyRow)
	}
	if event.WALStartLSN > buf.LastLSN {
		buf.LastLSN = event.WALStartLSN
	}
	transitioned := wasEmpty && (len(buf.Rows) > 0 || len(buf.Deletes) > 0)
	if transitioned {
		buf.FirstLSN = event.WALStartLSN
	}
	return transitioned
}

func (w *Writer) FlushAll(ctx context.Context) error {
	keys := make([]string, 0, len(w.buffers))
	for key := range w.buffers {
		if buf := w.buffers[key]; buf != nil && (len(buf.Rows) > 0 || len(buf.Deletes) > 0) {
			keys = append(keys, key)
		}
	}
	return w.flushKeys(ctx, keys)
}

func (w *Writer) ComputePendingMinLSN() pglogrepl.LSN {
	var min pglogrepl.LSN
	for _, buf := range w.buffers {
		if buf.FirstLSN == 0 {
			continue
		}
		if min == 0 || buf.FirstLSN < min {
			min = buf.FirstLSN
		}
	}
	return min
}

func (w *Writer) flush(ctx context.Context, key string) error {
	return w.flushKeys(ctx, []string{key})
}

func (w *Writer) flushKeys(ctx context.Context, keys []string) error {
	type flushedTable struct {
		key     string
		buf     *tableBuffer
		rows    int
		deletes int
	}
	if len(keys) == 0 {
		return nil
	}
	start := time.Now()
	pending := make([]*tableBuffer, 0, len(keys))
	for _, key := range keys {
		buf := w.buffers[key]
		if buf == nil || (len(buf.Rows) == 0 && len(buf.Deletes) == 0) {
			continue
		}
		if len(buf.Deletes) > 0 && len(buf.KeyColumns) == 0 {
			w.logger.Warn("dropping ducklake deletes for table without key columns", "table", key, "deletes", len(buf.Deletes))
			buf.Deletes = nil
		}
		if len(buf.Rows) == 0 && len(buf.Deletes) == 0 {
			buf.FirstLSN = 0
			continue
		}
		buf.Rows = dedupRowsByKey(buf.Columns, buf.KeyColumns, buf.Rows)
		buf.Deletes = dedupCellRows(buf.Deletes)
		pending = append(pending, buf)
	}
	if len(pending) == 0 {
		return nil
	}
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	flushed := make([]flushedTable, 0, len(pending))
	tableLSNs := make(map[string]string, len(pending))
	for _, buf := range pending {
		key := buf.Schema + "." + buf.Table
		rows := len(buf.Rows)
		deletes := len(buf.Deletes)
		if err := w.ensureTableForFlush(ctx, conn, key, buf); err != nil {
			return err
		}
		if deletes > 0 {
			if err := w.stageDeletesAndApply(ctx, conn, buf); err != nil {
				return err
			}
		}
		if rows > 0 {
			if err := w.stageRowsAndInsert(ctx, conn, buf); err != nil {
				return err
			}
		}
		tableLSNs[key] = buf.LastLSN.String()
		flushed = append(flushed, flushedTable{key: key, buf: buf, rows: rows, deletes: deletes})
	}
	if len(flushed) == 1 {
		buf := flushed[0].buf
		if err := w.setCommitMessage(ctx, conn, buf.Schema, buf.Table, "flush", buf.LastLSN.String()); err != nil {
			return err
		}
	} else if err := w.setBatchCommitMessage(ctx, conn, tableLSNs); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit ducklake flush: %w", err)
	}
	committed = true
	duration := time.Since(start)
	for _, ft := range flushed {
		w.logger.Info("ducklake flush completed",
			"schema", ft.buf.Schema,
			"table", ft.buf.Table,
			"rows", ft.rows,
			"deletes", ft.deletes,
			"mutation_mode", "delete_insert",
			"duration_ms", duration.Milliseconds(),
		)
		ft.buf.Rows = nil
		ft.buf.Deletes = nil
		ft.buf.FirstLSN = 0
	}
	return nil
}

type ducklakeExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type ducklakeQueryExecer interface {
	ducklakeExecer
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (w *Writer) ensureTableForFlush(ctx context.Context, conn *sql.Conn, key string, buf *tableBuffer) error {
	fp := columnsFingerprint(buf.Columns)
	if w.ensuredTables[key] == fp {
		return nil
	}
	if err := w.ensureTable(ctx, conn, buf.Schema, buf.Table, buf.Columns); err != nil {
		return err
	}
	if err := w.reconcileTableSchema(ctx, conn, buf.Schema, buf.Table, buf.Columns); err != nil {
		return err
	}
	w.ensuredTables[key] = fp
	return nil
}

func (w *Writer) ensureTable(ctx context.Context, tx ducklakeExecer, schema, table string, columns []wal.Column) error {
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", quoteIdent(w.catalogName), quoteIdent(schema))); err != nil {
		return fmt.Errorf("create ducklake schema: %w", err)
	}
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", qname(w.catalogName, schema, table), columnDDL(columns))
	if _, err := tx.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create ducklake table: %w", err)
	}
	return nil
}

func (w *Writer) stageRowsAndInsert(ctx context.Context, conn *sql.Conn, buf *tableBuffer) error {
	cols := columnList(buf.Columns)
	stmt := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM sb_rows", qname(w.catalogName, buf.Schema, buf.Table), cols, cols)
	return appendQuery(ctx, conn, stmt, "sb_rows", buf.Columns, buf.Rows)
}

func (w *Writer) stageDeletesAndApply(ctx context.Context, conn *sql.Conn, buf *tableBuffer) error {
	keys := keyColumns(buf.Columns, buf.KeyColumns)
	if len(keys) == 0 {
		return nil
	}
	stmt := fmt.Sprintf("DELETE FROM %s AS t USING sb_keys AS k WHERE %s", qname(w.catalogName, buf.Schema, buf.Table), keyPredicate("t", "k", keys))
	return appendQuery(ctx, conn, stmt, "sb_keys", keys, buf.Deletes)
}

func appendQuery(ctx context.Context, conn *sql.Conn, query, table string, columns []wal.Column, rows [][]cell) error {
	types, err := typeInfos(columns)
	if err != nil {
		return err
	}
	names := columnNames(columns)
	return conn.Raw(func(raw any) error {
		driverConn, ok := raw.(driver.Conn)
		if !ok {
			return fmt.Errorf("duckdb raw connection has unexpected type %T", raw)
		}
		appender, err := duckdb.NewQueryAppender(driverConn, query, table, types, names)
		if err != nil {
			return err
		}
		closed := false
		defer func() {
			if !closed {
				_ = appender.Clear()
				_ = appender.Close()
			}
		}()
		for _, row := range rows {
			values, err := rowValues(columns, row)
			if err != nil {
				return err
			}
			if err := appender.AppendRow(values...); err != nil {
				return err
			}
		}
		if err := appender.CloseWithCancel(ctx); err != nil {
			return err
		}
		closed = true
		return nil
	})
}

func rowValues(columns []wal.Column, row []cell) ([]driver.Value, error) {
	values := make([]driver.Value, len(columns))
	for i, col := range columns {
		if i >= len(row) || row[i].IsNull {
			values[i] = nil
			continue
		}
		v, err := parseValue(col.OID, row[i].Data)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		values[i] = v
	}
	return values, nil
}

func (w *Writer) Truncate(ctx context.Context, event wal.RowEvent) error {
	exists, err := w.tableExists(ctx, event.Schema, event.Table)
	if err != nil {
		return err
	}
	if !exists {
		w.logger.Info("ducklake truncate on not-yet-created table, no-op", "table", event.Schema+"."+event.Table)
		return nil
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", qname(w.catalogName, event.Schema, event.Table))); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := w.setCommitMessage(ctx, tx, event.Schema, event.Table, "truncate", event.WALStartLSN.String()); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if buf := w.buffers[event.Schema+"."+event.Table]; buf != nil {
		buf.Rows = nil
		buf.Deletes = nil
		buf.FirstLSN = 0
		buf.LastLSN = event.WALStartLSN
	}
	delete(w.ensuredTables, event.Schema+"."+event.Table)
	return nil
}

func (w *Writer) applySchemaChanges(ctx context.Context, tx ducklakeExecer, rel *wal.RelationMessage, defaults map[string]string) error {
	table := qname(w.catalogName, rel.Namespace, rel.Name)
	for _, ch := range rel.Changes {
		switch ch.Type {
		case wal.SchemaChangeAdd:
			stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, quoteIdent(ch.Column), pgOIDToDuckDBType(ch.NewOID))
			if def := defaults[ch.Column]; def != "" && isSimpleDefault(def) {
				stmt += " DEFAULT " + def
			}
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("add ducklake column %s: %w", ch.Column, err)
			}
		case wal.SchemaChangeDrop:
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, quoteIdent(ch.Column))); err != nil {
				return fmt.Errorf("drop ducklake column %s: %w", ch.Column, err)
			}
		case wal.SchemaChangeTypeChange:
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", table, quoteIdent(ch.Column), pgOIDToDuckDBType(ch.NewOID))); err != nil {
				return fmt.Errorf("alter ducklake column %s: %w", ch.Column, err)
			}
		}
	}
	return nil
}

func (w *Writer) reconcileTableSchema(ctx context.Context, tx ducklakeQueryExecer, schema, table string, columns []wal.Column) error {
	rows, err := tx.QueryContext(ctx,
		"SELECT column_name FROM information_schema.columns WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
		w.catalogName, schema, table,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	existing := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		existing[name] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	tableName := qname(w.catalogName, schema, table)
	for _, col := range columns {
		if existing[col.Name] {
			continue
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, quoteIdent(col.Name), pgOIDToDuckDBType(col.OID))
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("reconcile add ducklake column %s: %w", col.Name, err)
		}
	}
	return nil
}

func isSimpleDefault(s string) bool {
	if s == "" {
		return false
	}
	upper := strings.ToUpper(s)
	return !strings.Contains(upper, "NEXTVAL(") && !strings.ContainsAny(s, ";")
}

func (w *Writer) setCommitMessage(ctx context.Context, tx ducklakeExecer, schema, table, op, lsn string) error {
	extra := map[string]any{
		"streambed.table":   schema + "." + table,
		"streambed.op":      op,
		"streambed.version": "1",
	}
	if lsn != "" {
		extra["streambed.last_flush_lsn"] = lsn
	}
	data, err := json.Marshal(extra)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf("CALL %s.set_commit_message(?, ?, extra_info => ?)", quoteIdent(w.catalogName)),
		"streambed", op+" "+schema+"."+table, string(data))
	return err
}

func (w *Writer) setBatchCommitMessage(ctx context.Context, tx ducklakeExecer, tableLSNs map[string]string) error {
	extra := map[string]any{
		"streambed.op":      "flush",
		"streambed.version": "1",
		"streambed.tables":  tableLSNs,
	}
	data, err := json.Marshal(extra)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf("CALL %s.set_commit_message(?, ?, extra_info => ?)", quoteIdent(w.catalogName)),
		"streambed", fmt.Sprintf("flush %d tables", len(tableLSNs)), string(data))
	return err
}

func (w *Writer) tableExists(ctx context.Context, schema, table string) (bool, error) {
	var n int
	err := w.db.QueryRowContext(ctx,
		"SELECT count(*) FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
		w.catalogName, schema, table,
	).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (w *Writer) GetTableFlushLSN(ctx context.Context, schema, table string) (string, bool, error) {
	return GetTableFlushLSN(ctx, w.db, w.catalogName, schema, table)
}

func GetTableFlushLSN(ctx context.Context, db *sql.DB, catalogName, schema, table string) (string, bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT commit_extra_info FROM %s.snapshots() WHERE commit_extra_info IS NOT NULL ORDER BY snapshot_id DESC", quoteIdent(catalogName)))
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	target := schema + "." + table
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return "", false, err
		}
		var extra map[string]any
		if err := json.Unmarshal([]byte(raw), &extra); err != nil {
			continue
		}
		if extra["streambed.table"] == target {
			if lsn, ok := extra["streambed.last_flush_lsn"].(string); ok && lsn != "" {
				return lsn, true, nil
			}
		}
		if tables, ok := extra["streambed.tables"].(map[string]any); ok {
			if lsn, ok := tables[target].(string); ok && lsn != "" {
				return lsn, true, nil
			}
		}
	}
	return "", false, rows.Err()
}
