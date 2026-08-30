package ducklake

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/wal"
)

type ResyncOptions struct {
	Schema, Table string
	FlushRows     int
	ReplConn      *pgconn.PgConn
	DataConn      *pgconn.PgConn
	State         *state.Store
	Writer        *Writer
	Logger        *slog.Logger
}

type ResyncStats struct {
	Rows         int64
	Batches      int
	BackfillLSN  string
	SnapshotName string
}

func RunResync(ctx context.Context, opts ResyncOptions) (ResyncStats, error) {
	var stats ResyncStats
	if opts.FlushRows <= 0 {
		opts.FlushRows = 10000
	}
	columns, err := wal.FetchTableColumns(ctx, opts.DataConn, opts.Schema, opts.Table)
	if err != nil {
		return stats, fmt.Errorf("fetch columns: %w", err)
	}
	walColumns := make([]wal.Column, len(columns))
	for i, c := range columns {
		walColumns[i] = wal.Column{Name: c.Name, OID: c.OID}
	}
	if err := opts.Writer.DropTable(ctx, opts.Schema, opts.Table); err != nil {
		return stats, fmt.Errorf("drop existing ducklake table: %w", err)
	}
	slotName := "sb_resync_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	tmp, err := wal.CreateTempSlotWithSnapshot(ctx, opts.ReplConn, slotName, opts.Logger)
	if err != nil {
		return stats, fmt.Errorf("create temp slot: %w", err)
	}
	stats.SnapshotName = tmp.SnapshotName
	stats.BackfillLSN = tmp.ConsistentPoint.String()

	batchRows := 0
	rowFn := func(row wal.BackfillRow) error {
		values := make([]wal.ColumnValue, len(row.Values))
		for i, v := range row.Values {
			values[i] = wal.ColumnValue{
				Name:   v.Name,
				OID:    v.OID,
				IsNull: v.IsNull,
			}
			if !v.IsNull {
				values[i].Value = append([]byte(nil), v.Value...)
			}
		}
		if _, err := opts.Writer.HandleEvent(ctx, wal.RowEvent{
			Schema:      opts.Schema,
			Table:       opts.Table,
			Columns:     walColumns,
			Op:          wal.OpInsert,
			Values:      values,
			WALStartLSN: tmp.ConsistentPoint,
		}); err != nil {
			return err
		}
		batchRows++
		if batchRows >= opts.FlushRows {
			if err := opts.Writer.FlushAll(ctx); err != nil {
				return err
			}
			stats.Batches++
			batchRows = 0
		}
		return nil
	}
	count, err := wal.CopyTableUnderSnapshot(ctx, opts.DataConn, tmp.SnapshotName, opts.Schema, opts.Table, columns, rowFn, opts.Logger)
	if err != nil {
		return stats, fmt.Errorf("copy under snapshot: %w", err)
	}
	stats.Rows = count
	if batchRows > 0 {
		if err := opts.Writer.FlushAll(ctx); err != nil {
			return stats, err
		}
		stats.Batches++
	}
	if err := opts.State.RegisterTable(opts.Schema, opts.Table, len(columns)); err != nil {
		return stats, fmt.Errorf("register table in state: %w", err)
	}
	if err := opts.State.SetBackfillLSN(opts.Schema, opts.Table, tmp.ConsistentPoint); err != nil {
		return stats, fmt.Errorf("set backfill_lsn: %w", err)
	}
	opts.Logger.Info("ducklake resync complete",
		"schema", opts.Schema,
		"table", opts.Table,
		"rows", stats.Rows,
		"batches", stats.Batches,
		"backfill_lsn", stats.BackfillLSN,
	)
	return stats, nil
}
