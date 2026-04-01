package iceberg

import (
	"context"
	"fmt"
	"log/slog"
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
	Schema  string
	Table   string
	Columns []wal.Column
	Rows    [][]pqbuilder.Value
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
			if len(w.buffers[key].Rows) >= w.flushRows {
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
			Schema:  event.Schema,
			Table:   event.Table,
			Columns: event.Columns,
			LastLSN: event.WALStartLSN,
		}
		w.buffers[key] = buf

		// Register table in state store
		w.state.RegisterTable(event.Schema, event.Table, len(event.Columns), event.WALStartLSN)
		w.logger.Info("new table discovered",
			"schema", event.Schema,
			"table", event.Table,
			"columns", len(event.Columns),
		)
	}

	row := make([]pqbuilder.Value, len(event.Values))
	for i, v := range event.Values {
		row[i] = pqbuilder.Value{
			Data:   v.Value,
			IsNull: v.IsNull,
		}
	}
	buf.Rows = append(buf.Rows, row)
	if event.WALStartLSN > buf.LastLSN {
		buf.LastLSN = event.WALStartLSN
	}
}

func (w *Writer) flush(ctx context.Context, key string, ackCh chan<- pglogrepl.LSN) error {
	buf, exists := w.buffers[key]
	if !exists || len(buf.Rows) == 0 {
		return nil
	}

	start := time.Now()
	rowCount := len(buf.Rows)

	// Convert columns for parquet builder
	cols := make([]pqbuilder.ColumnDef, len(buf.Columns))
	for i, c := range buf.Columns {
		cols[i] = pqbuilder.ColumnDef{Name: c.Name, OID: c.OID}
	}

	// 1. Build Parquet file
	parquetData, err := w.parquet.Build(cols, buf.Rows)
	if err != nil {
		return fmt.Errorf("build parquet for %s: %w", key, err)
	}

	// 2. Upload to S3
	dataFileName := fmt.Sprintf("data/%s.parquet", uuid.New().String())
	s3Key := fmt.Sprintf("%s/%s/%s/%s", w.catalog.prefix, buf.Schema, buf.Table, dataFileName)

	if err := w.storage.PutObject(ctx, s3Key, parquetData, "application/octet-stream"); err != nil {
		return fmt.Errorf("upload parquet for %s: %w", key, err)
	}

	// 3. Ensure Iceberg table exists
	tableExists, err := w.catalog.TableExists(ctx, buf.Schema, buf.Table)
	if err != nil {
		return fmt.Errorf("check table %s: %w", key, err)
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

	// 4. Commit Iceberg snapshot
	if err := w.catalog.CommitSnapshot(ctx, buf.Schema, buf.Table, DataFile{
		Path:     dataFileName,
		RowCount: int64(rowCount),
		FileSize: int64(len(parquetData)),
	}); err != nil {
		return fmt.Errorf("commit snapshot for %s: %w", key, err)
	}

	w.logger.Info("syncing last LSN to state", "LSN", buf.LastLSN.String())

	// 5. Update state store
	// TODO: update per table LSN
	// if err := w.state.SetFlushedLSN(w.slotName, buf.LastLSN); err != nil {
	// 	return fmt.Errorf("unable to set flushed LSN in the store %s: %w", buf.LastLSN.String(), err)
	// }
	if err := w.state.UpdateLastFlush(buf.LastLSN, buf.Schema, buf.Table); err != nil {
		return fmt.Errorf("unable to set flushed LSN in the store %s: %w", buf.LastLSN.String(), err)
	}

	// 6. Every time we flush, send leastLSN (safest) in buffer to consumer
	var leastLSN pglogrepl.LSN
	var maxLSN pglogrepl.LSN
	for _, row := range w.buffers {
		if row.LastLSN == 0 {
			continue
		}
		if row.LastLSN > maxLSN {
			maxLSN = row.LastLSN
		}
		if len(row.Rows) == 0 {
			continue
		}
		if leastLSN > row.LastLSN || leastLSN == 0 {
			leastLSN = row.LastLSN
		}
	}
	if leastLSN == 0 {
		leastLSN = maxLSN // all flushed, safe to ack highest
	}

	select {
	case ackCh <- leastLSN:
	default:
	}

	duration := time.Since(start)
	w.logger.Info("flush completed",
		"schema", buf.Schema,
		"table", buf.Table,
		"rows", rowCount,
		"parquet_bytes", len(parquetData),
		"duration_ms", duration.Milliseconds(),
	)

	// 7. Clear buffer
	buf.Rows = nil

	return nil
}

func (w *Writer) flushAll(ctx context.Context, ackCh chan<- pglogrepl.LSN) error {
	for key := range w.buffers {
		if len(w.buffers[key].Rows) > 0 {
			if err := w.flush(ctx, key, ackCh); err != nil {
				return err
			}
		}
	}
	return nil
}
