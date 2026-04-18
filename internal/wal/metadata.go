package wal

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// MetadataQuerier provides read-only access to Postgres catalog metadata
// (column defaults, type info) over a regular (non-replication) connection.
type MetadataQuerier struct {
	conn *pgx.Conn
}

// NewMetadataQuerier wraps an existing pgx connection for metadata queries.
func NewMetadataQuerier(conn *pgx.Conn) *MetadataQuerier {
	return &MetadataQuerier{conn: conn}
}

// GetColumnDefaults returns the default expressions for the specified columns
// of a table. The result maps column name → default expression as text
// (e.g. "42", "'hello'::text"). Columns without defaults are omitted.
func (m *MetadataQuerier) GetColumnDefaults(ctx context.Context, schema, table string, columns []string) (map[string]string, error) {
	if len(columns) == 0 {
		return nil, nil
	}

	// Query pg_attrdef joined with pg_attribute and pg_class to get
	// column default expressions for the specified columns.
	query := `
		SELECT a.attname, pg_get_expr(d.adbin, d.adrelid) AS default_expr
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE n.nspname = $1
		  AND c.relname = $2
		  AND a.attname = ANY($3)
		  AND NOT a.attisdropped
	`

	rows, err := m.conn.Query(ctx, query, schema, table, columns)
	if err != nil {
		return nil, fmt.Errorf("query column defaults: %w", err)
	}
	defer rows.Close()

	defaults := make(map[string]string)
	for rows.Next() {
		var colName, defaultExpr string
		if err := rows.Scan(&colName, &defaultExpr); err != nil {
			return nil, fmt.Errorf("scan column default: %w", err)
		}
		defaults[colName] = defaultExpr
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate column defaults: %w", err)
	}
	return defaults, nil
}

// Close closes the underlying connection.
func (m *MetadataQuerier) Close(ctx context.Context) error {
	return m.conn.Close(ctx)
}
