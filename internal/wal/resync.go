package wal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
)

// BackfillColumn describes one column of a table being backfilled via COPY.
// The OID matches what pgoutput would emit on the logical replication stream,
// so downstream row handlers can treat BackfillRow values identically to
// RowEvent.Values from the main slot.
type BackfillColumn struct {
	Name string
	OID  uint32
}

// BackfillRow is a single row emitted from a COPY TO STDOUT read. Values are
// in pgoutput's text format (strings). NULLs appear with IsNull=true and
// Value=nil.
type BackfillRow struct {
	Values []ColumnValue
}

// FetchTableColumns returns the column names and type OIDs for schema.table
// in attribute-order. The returned OIDs match the values that pgoutput would
// put in a RelationMessage, so the backfill row stream can be processed by
// the same type handlers the main WAL stream uses.
//
// Only live (non-dropped) user columns are returned.
func FetchTableColumns(ctx context.Context, conn *pgconn.PgConn, schema, table string) ([]BackfillColumn, error) {
	// regclass lookup handles quoting, casing, and search_path correctly.
	qualified := quoteIdent(schema) + "." + quoteIdent(table)
	sql := fmt.Sprintf(`
		SELECT attname, atttypid
		FROM pg_catalog.pg_attribute
		WHERE attrelid = '%s'::regclass
		  AND attnum > 0
		  AND NOT attisdropped
		ORDER BY attnum`, escapeLiteral(qualified))

	mr := conn.Exec(ctx, sql)
	results, err := mr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("query pg_attribute for %s: %w", qualified, err)
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("unexpected result count from pg_attribute query: %d", len(results))
	}

	var cols []BackfillColumn
	for _, row := range results[0].Rows {
		if len(row) != 2 {
			return nil, fmt.Errorf("unexpected column count in pg_attribute row: %d", len(row))
		}
		name := string(row[0])
		var oid uint32
		if _, err := fmt.Sscanf(string(row[1]), "%d", &oid); err != nil {
			return nil, fmt.Errorf("parse atttypid %q: %w", row[1], err)
		}
		cols = append(cols, BackfillColumn{Name: name, OID: oid})
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("table %s has no columns (or does not exist)", qualified)
	}
	return cols, nil
}

// CopyTableUnderSnapshot runs COPY schema.table TO STDOUT against conn inside
// a REPEATABLE READ transaction using the supplied snapshotName. The snapshot
// must have been exported by another connection (e.g. CreateTempSlotWithSnapshot)
// that is still open.
//
// Each decoded row is passed to rowFn. rowFn should NOT retain slices from
// its argument across calls — the underlying byte buffers are reused.
//
// The transaction is committed on success, rolled back on error.
func CopyTableUnderSnapshot(
	ctx context.Context,
	conn *pgconn.PgConn,
	snapshotName string,
	schema, table string,
	columns []BackfillColumn,
	rowFn func(BackfillRow) error,
	logger *slog.Logger,
) (rowCount int64, err error) {
	qualified := quoteIdent(schema) + "." + quoteIdent(table)

	// Step 1: open a REPEATABLE READ txn and bind the exported snapshot.
	// SET TRANSACTION SNAPSHOT must be the first statement after BEGIN —
	// postponing it to after any other read would fail.
	if err := execAll(ctx, conn, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return 0, fmt.Errorf("begin txn: %w", err)
	}

	// Best-effort rollback if we bail early. After a successful COMMIT this
	// becomes a no-op (the transaction is already finished).
	defer func() {
		if err != nil {
			_ = execAll(context.Background(), conn, "ROLLBACK")
		}
	}()

	setSnap := fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", escapeLiteral(snapshotName))
	if err := execAll(ctx, conn, setSnap); err != nil {
		return 0, fmt.Errorf("set transaction snapshot: %w", err)
	}

	// Step 2: stream COPY output. We pipe CopyTo's writer through an
	// in-memory pipe so the parser consumes rows as they arrive, avoiding
	// buffering the entire table in memory.
	copySQL := fmt.Sprintf("COPY %s TO STDOUT", qualified)
	logger.Info("running COPY under snapshot", "sql", copySQL, "snapshot", snapshotName)

	pr, pw := io.Pipe()

	// Producer: CopyTo writes raw bytes to pw. When it returns, close pw so
	// the reader sees EOF.
	errCh := make(chan error, 1)
	go func() {
		_, copyErr := conn.CopyTo(ctx, pw, copySQL)
		// Close with or without error so the consumer side unblocks.
		pw.CloseWithError(copyErr)
		errCh <- copyErr
	}()

	// Consumer: parse lines → rows.
	expectedCols := len(columns)
	parseErr := parseCopyTextStream(pr, func(fields []copyTextField) error {
		if len(fields) != expectedCols {
			return fmt.Errorf("row has %d fields, expected %d for %s",
				len(fields), expectedCols, qualified)
		}
		values := make([]ColumnValue, expectedCols)
		for i, f := range fields {
			values[i] = ColumnValue{
				Name:   columns[i].Name,
				OID:    columns[i].OID,
				Value:  f.Data,
				IsNull: f.IsNull,
			}
		}
		rowCount++
		return rowFn(BackfillRow{Values: values})
	})

	// Wait for the producer and surface whichever error came first.
	producerErr := <-errCh
	if parseErr != nil {
		return rowCount, fmt.Errorf("parse COPY output: %w", parseErr)
	}
	if producerErr != nil {
		return rowCount, fmt.Errorf("COPY %s: %w", qualified, producerErr)
	}

	// Step 3: commit the (read-only) snapshot transaction.
	if err := execAll(ctx, conn, "COMMIT"); err != nil {
		return rowCount, fmt.Errorf("commit snapshot txn: %w", err)
	}

	logger.Info("COPY complete", "table", qualified, "rows", rowCount)
	return rowCount, nil
}

// execAll runs a simple SQL statement that returns no result rows and
// swallows the (empty) results. Used for BEGIN/COMMIT/ROLLBACK/SET.
func execAll(ctx context.Context, conn *pgconn.PgConn, sql string) error {
	mr := conn.Exec(ctx, sql)
	_, err := mr.ReadAll()
	return err
}

// quoteIdent wraps an identifier in double quotes, doubling any embedded
// double quotes per Postgres syntax. Sufficient for schema and table names
// the operator typed at the CLI; this is not a general-purpose SQL quoter.
func quoteIdent(s string) string {
	var b bytes.Buffer
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			b.WriteByte('"')
		}
		b.WriteByte(s[i])
	}
	b.WriteByte('"')
	return b.String()
}

// escapeLiteral doubles single quotes so a string can be embedded in a
// single-quoted SQL literal.
func escapeLiteral(s string) string {
	var b bytes.Buffer
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			b.WriteByte('\'')
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
