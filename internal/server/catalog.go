package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/viggy28/streambed/internal/storage"
)

// TableInfo holds metadata about a discovered Iceberg table.
type TableInfo struct {
	Schema string
	Table  string
	S3Path string // full s3:// path to table root, e.g., "s3://bucket/prefix/public/orders"
}

// TableCatalog discovers Iceberg tables on S3 and registers them as DuckDB views.
// It scans for version-hint.text files to find tables, then creates
// CREATE VIEW statements so queries can reference tables by name.
type TableCatalog struct {
	s3Bucket string
	s3Prefix string
	s3Client *storage.S3Client
	logger   *slog.Logger

	mu          sync.RWMutex
	tables      map[string]TableInfo // key: "schema.table"
	emptyTables map[string]bool      // tables with no snapshots (all rows deleted)
}

// NewTableCatalog creates a new catalog that discovers tables under the given S3 prefix.
func NewTableCatalog(s3Client *storage.S3Client, bucket, prefix string, logger *slog.Logger) *TableCatalog {
	return &TableCatalog{
		s3Bucket: bucket,
		s3Prefix: prefix,
		s3Client: s3Client,
		logger:   logger,
		tables:      make(map[string]TableInfo),
		emptyTables: make(map[string]bool),
	}
}

// Refresh scans S3 for Iceberg tables by looking for version-hint.text files.
// Each file at <prefix>/<schema>/<table>/metadata/version-hint.text indicates
// an Iceberg table exists at that path.
func (c *TableCatalog) Refresh(ctx context.Context) error {
	keys, err := c.s3Client.ListPrefix(ctx, c.s3Prefix)
	if err != nil {
		return fmt.Errorf("list S3 prefix %q: %w", c.s3Prefix, err)
	}

	discovered := make(map[string]TableInfo)
	suffix := "/metadata/version-hint.text"

	for _, key := range keys {
		if !strings.HasSuffix(key, suffix) {
			continue
		}

		// Extract schema and table from the key path.
		// Key format: <prefix>/<schema>/<table>/metadata/version-hint.text
		tablePath := strings.TrimSuffix(key, suffix)
		relPath := strings.TrimPrefix(tablePath, c.s3Prefix)
		relPath = strings.TrimPrefix(relPath, "/")

		parts := strings.SplitN(relPath, "/", 2)
		if len(parts) != 2 {
			c.logger.Warn("unexpected table path structure", "key", key)
			continue
		}

		schema := parts[0]
		table := parts[1]
		qualifiedName := schema + "." + table

		discovered[qualifiedName] = TableInfo{
			Schema: schema,
			Table:  table,
			S3Path: fmt.Sprintf("s3://%s/%s", c.s3Bucket, tablePath),
		}
	}

	c.mu.Lock()
	c.tables = discovered
	c.mu.Unlock()

	c.logger.Info("catalog refreshed", "tables", len(discovered))
	return nil
}

// Tables returns a snapshot of all discovered tables.
func (c *TableCatalog) Tables() map[string]TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]TableInfo, len(c.tables))
	for k, v := range c.tables {
		result[k] = v
	}
	return result
}

// Resolve returns the iceberg_scan() SQL expression for a table name.
// Accepts both qualified ("public.orders") and unqualified ("orders") names.
func (c *TableCatalog) Resolve(tableName string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Try qualified name first
	if info, ok := c.tables[tableName]; ok {
		if c.emptyTables[tableName] {
			return "", fmt.Errorf("table %q has no data (all rows deleted)", tableName)
		}
		return fmt.Sprintf("iceberg_scan('%s', allow_moved_paths = true)", info.S3Path), nil
	}

	// Try unqualified: search all schemas for a matching table name
	var matches []TableInfo
	for qualName, info := range c.tables {
		if info.Table == tableName && !c.emptyTables[qualName] {
			matches = append(matches, info)
		}
	}

	switch len(matches) {
	case 0:
		return "", fmt.Errorf("table %q not found", tableName)
	case 1:
		return fmt.Sprintf("iceberg_scan('%s', allow_moved_paths = true)", matches[0].S3Path), nil
	default:
		return "", fmt.Errorf("ambiguous table name %q: found in multiple schemas", tableName)
	}
}

// RegisterViews creates or replaces DuckDB views for all discovered tables.
// Each table gets two views:
//   - Unqualified name (e.g., "orders") if unambiguous across schemas
//   - Qualified name with underscore (e.g., "public_orders") always
//
// Returns ErrDuckDBFatal if a FATAL error invalidated the DuckDB engine.
// The caller should re-open DuckDB and retry.
func (c *TableCatalog) RegisterViews(db *sql.DB) error {
	c.mu.RLock()
	tables := make(map[string]TableInfo, len(c.tables))
	for k, v := range c.tables {
		tables[k] = v
	}
	c.mu.RUnlock()

	// Count how many schemas each table name appears in for ambiguity detection
	tableNameCount := make(map[string]int)
	for _, info := range tables {
		tableNameCount[info.Table]++
	}

	// Reset empty tables tracking for this refresh cycle.
	c.mu.Lock()
	c.emptyTables = make(map[string]bool)
	c.mu.Unlock()

	var registered int
	for _, info := range tables {
		scanExpr := fmt.Sprintf("iceberg_scan('%s', allow_moved_paths = true)", info.S3Path)

		// Always register qualified view: public_orders
		qualifiedView := info.Schema + "_" + info.Table
		stmt := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s", qualifiedView, scanExpr)
		if _, err := db.Exec(stmt); err != nil {
			if isFatalDuckDBError(err) {
				c.logger.Error("DuckDB FATAL error during view registration, engine invalidated",
					"view", qualifiedView, "error", err)
				return ErrDuckDBFatal
			}
			// Empty table (all rows deleted) — iceberg_scan can't handle
			// current-snapshot-id=-1 with no snapshots. Drop stale views
			// so queries get "table not found" instead of a confusing IO error.
			if isEmptyTableError(err) {
				qualifiedName := info.Schema + "." + info.Table
				c.logger.Info("table has no snapshots, dropping stale views",
					"view", qualifiedView, "table", qualifiedName)
				db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", qualifiedView))
				db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", info.Table))
				c.mu.Lock()
				c.emptyTables[qualifiedName] = true
				c.mu.Unlock()
				continue
			}
			c.logger.Warn("failed to register view", "view", qualifiedView, "error", err)
			continue
		}

		// Register unqualified view if unambiguous
		if tableNameCount[info.Table] == 1 {
			stmt = fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s", info.Table, scanExpr)
			if _, err := db.Exec(stmt); err != nil {
				c.logger.Warn("failed to register unqualified view", "view", info.Table, "error", err)
			}
		}

		registered++
		c.logger.Debug("registered view",
			"schema", info.Schema,
			"table", info.Table,
			"qualified_view", qualifiedView,
		)
	}

	c.logger.Info("views registered", "count", registered, "total", len(tables))
	return nil
}

// ErrDuckDBFatal indicates that a FATAL error invalidated the DuckDB engine.
var ErrDuckDBFatal = fmt.Errorf("duckdb engine invalidated by fatal error")

// isEmptyTableError checks if iceberg_scan failed because the table has no
// snapshots (e.g., all rows were deleted via commitEmptyTable).
func isEmptyTableError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "No snapshots found")
}

// isFatalDuckDBError checks if an error is a DuckDB FATAL error that
// invalidates the entire engine (e.g., empty manifest list crash).
func isFatalDuckDBError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "FATAL Error") ||
		strings.Contains(err.Error(), "database has been invalidated")
}
