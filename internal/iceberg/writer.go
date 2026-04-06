package iceberg

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	pqbuilder "github.com/viggy28/streambed/internal/parquet"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/storage"
	"github.com/viggy28/streambed/internal/wal"
)

// Writer receives RowEvents, buffers them per table, and flushes to S3 + Iceberg.
type Writer struct {
	catalog       *Catalog
	parquet       *pqbuilder.Builder
	storage       *storage.S3Client
	state         *state.Store
	slotName      string
	flushRows     int
	flushInterval time.Duration
	logger        *slog.Logger

	buffers map[string]*tableBuffer // key: "schema.table"
}

type tableBuffer struct {
	Schema     string
	Table      string
	Columns    []wal.Column
	KeyColumns []int // positions into Columns; stamped once from first event
	Rows       [][]pqbuilder.Value
	// Deletes holds equality-delete keys, one per row. Each inner slice
	// is aligned with KeyColumns: Deletes[i][j] is the value of the
	// j-th key column for the i-th pending delete.
	Deletes [][]pqbuilder.Value
	LastLSN pglogrepl.LSN
}

func NewWriter(
	catalog *Catalog,
	s3Client *storage.S3Client,
	stateStore *state.Store,
	slotName string,
	flushRows int,
	flushInterval time.Duration,
	logger *slog.Logger,
) *Writer {
	return &Writer{
		catalog:       catalog,
		parquet:       &pqbuilder.Builder{},
		storage:       s3Client,
		state:         stateStore,
		slotName:      slotName,
		flushRows:     flushRows,
		flushInterval: flushInterval,
		logger:        logger,
		buffers:       make(map[string]*tableBuffer),
	}
}

// Start reads from events channel, buffers rows, and flushes on threshold.
// Sends flushed LSN on ackCh for the consumer's standby status.
func (w *Writer) Start(ctx context.Context, events <-chan wal.RowEvent, ackCh chan<- pglogrepl.LSN) error {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case event, ok := <-events:
			if !ok {
				w.logger.Info("flushing all...")
				return w.flushAll(context.Background(), ackCh)
			}
			w.buffer(event)

			key := fmt.Sprintf("%s.%s", event.Schema, event.Table)
			b := w.buffers[key]
			if len(b.Rows)+len(b.Deletes) >= w.flushRows {
				if err := w.flush(ctx, key, ackCh); err != nil {
					w.logger.Error("flush error", "table", key, "error", err)
					return err
				}
			}

		case <-ticker.C:
			if err := w.flushAll(ctx, ackCh); err != nil {
				return err
			}

		case <-ctx.Done():
			// Final flush on shutdown
			w.logger.Info("shutdown: flushing remaining buffers")
			return w.flushAll(context.Background(), ackCh)
		}
	}
}

func (w *Writer) buffer(event wal.RowEvent) {
	key := fmt.Sprintf("%s.%s", event.Schema, event.Table)
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

		// Register table in state store
		w.state.RegisterTable(event.Schema, event.Table, len(event.Columns), event.WALStartLSN)
		w.logger.Info("new table discovered",
			"schema", event.Schema,
			"table", event.Table,
			"columns", len(event.Columns),
			"key_columns", len(event.KeyColumns),
		)
	}

	// Update schema/key info when a newer RelationMessage provides it
	// (e.g., ALTER TABLE ... REPLICA IDENTITY after initial discovery).
	if len(event.Columns) > 0 {
		buf.Columns = event.Columns
	}
	if len(event.KeyColumns) > 0 && len(buf.KeyColumns) == 0 {
		buf.KeyColumns = event.KeyColumns
		w.logger.Info("key columns updated",
			"table", key,
			"key_columns", len(event.KeyColumns),
		)
	}

	// Append a full data row (for INSERT and UPDATE).
	if event.Op == wal.OpInsert || event.Op == wal.OpUpdate {
		row := make([]pqbuilder.Value, len(event.Values))
		for i, v := range event.Values {
			row[i] = pqbuilder.Value{Data: v.Value, IsNull: v.IsNull}
		}
		buf.Rows = append(buf.Rows, row)
	}
	// Append an equality-delete key (for UPDATE and DELETE).
	if event.Op == wal.OpUpdate || event.Op == wal.OpDelete {
		keyRow := make([]pqbuilder.Value, len(event.OldKey))
		for i, v := range event.OldKey {
			keyRow[i] = pqbuilder.Value{Data: v.Value, IsNull: v.IsNull}
		}
		buf.Deletes = append(buf.Deletes, keyRow)
	}

	if event.WALStartLSN > buf.LastLSN {
		buf.LastLSN = event.WALStartLSN
	}
}

func (w *Writer) flush(ctx context.Context, key string, ackCh chan<- pglogrepl.LSN) error {
	buf, exists := w.buffers[key]
	if !exists || (len(buf.Rows) == 0 && len(buf.Deletes) == 0) {
		return nil
	}

	start := time.Now()
	rowCount := len(buf.Rows)
	delCount := len(buf.Deletes)

	// Convert full-table columns for parquet builder.
	cols := make([]pqbuilder.ColumnDef, len(buf.Columns))
	for i, c := range buf.Columns {
		cols[i] = pqbuilder.ColumnDef{Name: c.Name, OID: c.OID}
	}

	// Ensure Iceberg table exists before writing anything.
	tableExists, err := w.catalog.TableExists(ctx, buf.Schema, buf.Table)
	if err != nil {
		return fmt.Errorf("check table %s: %w", key, err)
	}
	if !tableExists && rowCount == 0 {
		w.logger.Warn("skipping delete-only flush for non-existent table",
			"table", key, "deletes", delCount)
		buf.Deletes = nil
		return nil
	}
	if !tableExists {
		icebergCols := make([]ColumnDef, len(buf.Columns))
		for i, c := range buf.Columns {
			icebergCols[i] = ColumnDef{Name: c.Name, OID: c.OID}
		}
		if err := w.catalog.CreateTable(ctx, buf.Schema, buf.Table, icebergCols); err != nil {
			return fmt.Errorf("create table %s: %w", key, err)
		}
	}

	var dataFile *DataFile
	replace := false

	if delCount > 0 && len(buf.KeyColumns) > 0 {
		// Copy-on-write: read existing data, remove deleted rows, combine
		// with new inserts, and write a replacement snapshot.
		replace = true

		existingRows, err := w.readExistingRows(ctx, buf, cols)
		if err != nil {
			return fmt.Errorf("COW read for %s: %w", key, err)
		}

		// Filter out rows matching any delete key.
		filtered := filterDeletedRows(existingRows, buf.Deletes, buf.KeyColumns)

		// Combine surviving rows with new inserts.
		combined := append(filtered, buf.Rows...)

		w.logger.Info("COW merge",
			"table", key,
			"existing", len(existingRows),
			"after_filter", len(filtered),
			"new_rows", rowCount,
			"combined", len(combined),
			"deletes_applied", delCount,
		)

		// Always write a Parquet file, even with 0 rows. This preserves
		// the schema so DuckDB can still query the table (returning 0 rows)
		// instead of failing with "No snapshots found" or "table not found".
		parquetData, err := w.parquet.Build(cols, combined)
		if err != nil {
			return fmt.Errorf("build parquet for %s: %w", key, err)
		}
		dataFileName := fmt.Sprintf("data/%s.parquet", uuid.New().String())
		s3Key := fmt.Sprintf("%s/%s/%s/%s", w.catalog.prefix, buf.Schema, buf.Table, dataFileName)
		if err := w.storage.PutObject(ctx, s3Key, parquetData, "application/octet-stream"); err != nil {
			return fmt.Errorf("upload parquet for %s: %w", key, err)
		}
		dataFile = &DataFile{
			Path:     dataFileName,
			RowCount: int64(len(combined)),
			FileSize: int64(len(parquetData)),
		}
	} else if delCount > 0 {
		w.logger.Warn("dropping deletes for table without key columns",
			"table", key, "deletes", delCount)
		buf.Deletes = nil
	}

	// Append-only path: just write new rows.
	if !replace && rowCount > 0 {
		parquetData, err := w.parquet.Build(cols, buf.Rows)
		if err != nil {
			return fmt.Errorf("build parquet for %s: %w", key, err)
		}
		dataFileName := fmt.Sprintf("data/%s.parquet", uuid.New().String())
		s3Key := fmt.Sprintf("%s/%s/%s/%s", w.catalog.prefix, buf.Schema, buf.Table, dataFileName)
		if err := w.storage.PutObject(ctx, s3Key, parquetData, "application/octet-stream"); err != nil {
			return fmt.Errorf("upload parquet for %s: %w", key, err)
		}
		dataFile = &DataFile{
			Path:     dataFileName,
			RowCount: int64(rowCount),
			FileSize: int64(len(parquetData)),
		}
	}

	// Commit snapshot.
	if err := w.catalog.CommitChangeset(ctx, buf.Schema, buf.Table, dataFile, nil, replace); err != nil {
		return fmt.Errorf("commit snapshot for %s: %w", key, err)
	}

	w.logger.Info("syncing last LSN to state", "LSN", buf.LastLSN.String())

	if err := w.state.UpdateLastFlush(buf.LastLSN, buf.Schema, buf.Table); err != nil {
		return fmt.Errorf("unable to set flushed LSN in the store %s: %w", buf.LastLSN.String(), err)
	}

	// Send leastLSN (safest unflushed) to consumer for standby status.
	var leastLSN pglogrepl.LSN
	var maxLSN pglogrepl.LSN
	for _, row := range w.buffers {
		if row.LastLSN == 0 {
			continue
		}
		if row.LastLSN > maxLSN {
			maxLSN = row.LastLSN
		}
		if len(row.Rows) == 0 && len(row.Deletes) == 0 {
			continue
		}
		if leastLSN > row.LastLSN || leastLSN == 0 {
			leastLSN = row.LastLSN
		}
	}
	if leastLSN == 0 {
		leastLSN = maxLSN
	}

	select {
	case ackCh <- leastLSN:
	default:
	}

	duration := time.Since(start)
	var dataBytes int64
	if dataFile != nil {
		dataBytes = dataFile.FileSize
	}
	w.logger.Info("flush completed",
		"schema", buf.Schema,
		"table", buf.Table,
		"rows", rowCount,
		"deletes", delCount,
		"cow", replace,
		"data_bytes", dataBytes,
		"duration_ms", duration.Milliseconds(),
	)

	buf.Rows = nil
	buf.Deletes = nil

	return nil
}

// readExistingRows downloads all current data files for a table from S3
// and parses them back to Value rows using the parquet reader.
func (w *Writer) readExistingRows(ctx context.Context, buf *tableBuffer, cols []pqbuilder.ColumnDef) ([][]pqbuilder.Value, error) {
	dataFilePaths, err := w.catalog.GetDataFilePaths(ctx, buf.Schema, buf.Table)
	if err != nil {
		return nil, err
	}

	var allRows [][]pqbuilder.Value
	for _, filePath := range dataFilePaths {
		s3Key := s3KeyFromURI(filePath)
		fileData, err := w.storage.GetObject(ctx, s3Key)
		if err != nil {
			return nil, fmt.Errorf("download %s: %w", filePath, err)
		}
		rows, err := pqbuilder.ReadRows(fileData, cols)
		if err != nil {
			return nil, fmt.Errorf("read parquet %s: %w", filePath, err)
		}
		allRows = append(allRows, rows...)
	}
	return allRows, nil
}

// filterDeletedRows removes rows whose key-column values match any
// pending delete. Uses a string-keyed set for O(N+M) matching.
func filterDeletedRows(rows [][]pqbuilder.Value, deletes [][]pqbuilder.Value, keyColumns []int) [][]pqbuilder.Value {
	if len(deletes) == 0 || len(keyColumns) == 0 {
		return rows
	}

	// Build set of delete keys.
	deleteSet := make(map[string]bool, len(deletes))
	for _, del := range deletes {
		deleteSet[buildKeyString(del)] = true
	}

	// Keep rows that don't match any delete key.
	result := make([][]pqbuilder.Value, 0, len(rows))
	for _, row := range rows {
		keyVals := make([]pqbuilder.Value, len(keyColumns))
		for i, idx := range keyColumns {
			if idx < len(row) {
				keyVals[i] = row[idx]
			}
		}
		if !deleteSet[buildKeyString(keyVals)] {
			result = append(result, row)
		}
	}
	return result
}

// buildKeyString serializes key-column values into a comparable string.
func buildKeyString(vals []pqbuilder.Value) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		if v.IsNull {
			parts[i] = "\x00"
		} else {
			parts[i] = string(v.Data)
		}
	}
	return strings.Join(parts, "\x01")
}

func (w *Writer) flushAll(ctx context.Context, ackCh chan<- pglogrepl.LSN) error {
	for key := range w.buffers {
		b := w.buffers[key]
		if len(b.Rows) > 0 || len(b.Deletes) > 0 {
			if err := w.flush(ctx, key, ackCh); err != nil {
				return err
			}
		}
	}
	return nil
}
