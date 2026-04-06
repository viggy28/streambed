// Package resync implements the one-shot table backfill used by the
// `streambed resync` command.
//
// Flow (Phase 3 spec):
//
//  1. Create a TEMPORARY logical replication slot with EXPORT_SNAPSHOT on a
//     dedicated replication connection. That gives us (snapshot id S, LSN L).
//  2. On a second connection, BEGIN REPEATABLE READ and
//     `SET TRANSACTION SNAPSHOT S`, then `COPY schema.table TO STDOUT`.
//  3. Stream rows through the parquet builder, uploading one parquet file
//     per FlushRows-sized batch. Create the Iceberg table on the first batch
//     and append a snapshot for every batch.
//  4. Commit the snapshot txn, close the replication conn (temp slot is then
//     dropped automatically by Postgres).
//  5. Record backfill_lsn = L in state so the sync daemon, on its next
//     startup, will drop any replayed main-slot events whose WAL position is
//     <= L (those rows are already in the COPY dump).
//
// This package owns nothing long-lived: callers provide the connections,
// storage client, state store, and catalog.
package resync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	pqbuilder "github.com/viggy28/streambed/internal/parquet"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/storage"
	"github.com/viggy28/streambed/internal/wal"

	"github.com/viggy28/streambed/internal/iceberg"
)

// Options bundles everything needed to backfill one table. Callers are
// responsible for the lifecycle of the connections and all handle types;
// Run does not close anything it did not create.
type Options struct {
	Schema, Table string

	// S3Prefix is the top-level prefix under which tables are laid out as
	// <prefix>/<schema>/<table>/. Must match what `streambed sync` uses.
	S3Prefix string

	// FlushRows caps how many rows are buffered before a parquet file is
	// written and an Iceberg snapshot is committed. A single COPY may
	// therefore produce multiple Iceberg snapshots.
	FlushRows int

	// ReplConn is a connection opened with `replication=database`. It is
	// used only to issue CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT and
	// must remain open for the entire Run() call — the exported snapshot
	// is bound to this connection's lifetime.
	ReplConn *pgconn.PgConn

	// DataConn is a regular (non-replication) connection. It runs the
	// snapshot transaction and the COPY.
	DataConn *pgconn.PgConn

	State   *state.Store
	S3      *storage.S3Client
	Catalog *iceberg.Catalog
	Logger  *slog.Logger
}

// Stats summarises a completed resync.
type Stats struct {
	Rows         int64
	Batches      int
	BackfillLSN  string
	SnapshotName string
}

// Run executes the resync flow for a single table. Preconditions:
//   - All existing S3 objects and state for the table have already been
//     deleted by the caller (see the `cleanup` command).
//   - The table exists in Postgres and is readable by the connection's role.
//
// On success, state.backfill_lsn for (schema,table) is set to the snapshot
// LSN, and Iceberg contains one or more snapshots covering all current rows.
func Run(ctx context.Context, opts Options) (Stats, error) {
	var stats Stats
	logger := opts.Logger

	if opts.FlushRows <= 0 {
		opts.FlushRows = 10000
	}

	// 1. Fetch column metadata from pg_attribute so we know the OIDs
	//    pgoutput would have sent us.
	columns, err := wal.FetchTableColumns(ctx, opts.DataConn, opts.Schema, opts.Table)
	if err != nil {
		return stats, fmt.Errorf("fetch columns: %w", err)
	}
	logger.Info("resync: resolved columns",
		"schema", opts.Schema,
		"table", opts.Table,
		"count", len(columns),
	)

	// 2. Create the temporary snapshot-exporting slot on the replication conn.
	slotName := "sb_resync_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	tmp, err := wal.CreateTempSlotWithSnapshot(ctx, opts.ReplConn, slotName, logger)
	if err != nil {
		return stats, fmt.Errorf("create temp slot: %w", err)
	}
	stats.SnapshotName = tmp.SnapshotName
	stats.BackfillLSN = tmp.ConsistentPoint.String()

	// 3. Prepare Iceberg column defs + parquet column defs once.
	icebergCols := make([]iceberg.ColumnDef, len(columns))
	parquetCols := make([]pqbuilder.ColumnDef, len(columns))
	walColumns := make([]wal.Column, len(columns))
	for i, c := range columns {
		icebergCols[i] = iceberg.ColumnDef{Name: c.Name, OID: c.OID}
		parquetCols[i] = pqbuilder.ColumnDef{Name: c.Name, OID: c.OID}
		walColumns[i] = wal.Column{Name: c.Name, OID: c.OID}
	}

	// 4. Stream COPY → batched parquet files. The table is created lazily
	//    on the first batch so an empty table produces no Iceberg writes.
	builder := &pqbuilder.Builder{}
	batch := make([][]pqbuilder.Value, 0, opts.FlushRows)
	tableCreated := false

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if !tableCreated {
			exists, err := opts.Catalog.TableExists(ctx, opts.Schema, opts.Table)
			if err != nil {
				return fmt.Errorf("check table exists: %w", err)
			}
			if !exists {
				if err := opts.Catalog.CreateTable(ctx, opts.Schema, opts.Table, icebergCols); err != nil {
					return fmt.Errorf("create iceberg table: %w", err)
				}
			}
			tableCreated = true
		}

		parquetData, err := builder.Build(parquetCols, batch)
		if err != nil {
			return fmt.Errorf("build parquet: %w", err)
		}
		dataFileName := fmt.Sprintf("data/%s.parquet", uuid.New().String())
		s3Key := fmt.Sprintf("%s/%s/%s/%s", opts.S3Prefix, opts.Schema, opts.Table, dataFileName)

		if err := opts.S3.PutObject(ctx, s3Key, parquetData, "application/octet-stream"); err != nil {
			return fmt.Errorf("upload parquet: %w", err)
		}
		if err := opts.Catalog.CommitSnapshot(ctx, opts.Schema, opts.Table, iceberg.DataFile{
			Path:     dataFileName,
			RowCount: int64(len(batch)),
			FileSize: int64(len(parquetData)),
		}); err != nil {
			return fmt.Errorf("commit snapshot: %w", err)
		}
		stats.Batches++
		logger.Info("resync: flushed batch",
			"schema", opts.Schema,
			"table", opts.Table,
			"rows", len(batch),
			"parquet_bytes", len(parquetData),
			"batch_num", stats.Batches,
		)
		// Reset slice without releasing capacity.
		batch = batch[:0]
		return nil
	}

	rowFn := func(row wal.BackfillRow) error {
		// COPY parser reuses buffers across rows, so we must copy byte
		// slices before appending them to our batch.
		values := make([]pqbuilder.Value, len(row.Values))
		for i, v := range row.Values {
			if v.IsNull {
				values[i] = pqbuilder.Value{IsNull: true}
				continue
			}
			data := make([]byte, len(v.Value))
			copy(data, v.Value)
			values[i] = pqbuilder.Value{Data: data}
		}
		batch = append(batch, values)
		if len(batch) >= opts.FlushRows {
			return flush()
		}
		return nil
	}

	count, err := wal.CopyTableUnderSnapshot(
		ctx, opts.DataConn, tmp.SnapshotName,
		opts.Schema, opts.Table,
		columns, rowFn, logger,
	)
	if err != nil {
		return stats, fmt.Errorf("copy under snapshot: %w", err)
	}
	stats.Rows = count

	// Flush any rows left in the final batch.
	if err := flush(); err != nil {
		return stats, err
	}

	// 5. Register the table in state (or refresh column count) and record
	//    the backfill LSN so the sync consumer filters overlapping events.
	if err := opts.State.RegisterTable(opts.Schema, opts.Table, len(columns), tmp.ConsistentPoint); err != nil {
		return stats, fmt.Errorf("register table in state: %w", err)
	}
	if err := opts.State.UpdateLastFlush(tmp.ConsistentPoint, opts.Schema, opts.Table); err != nil {
		return stats, fmt.Errorf("update last_flush: %w", err)
	}
	if err := opts.State.SetBackfillLSN(opts.Schema, opts.Table, tmp.ConsistentPoint); err != nil {
		return stats, fmt.Errorf("set backfill_lsn: %w", err)
	}

	logger.Info("resync: complete",
		"schema", opts.Schema,
		"table", opts.Table,
		"rows", stats.Rows,
		"batches", stats.Batches,
		"backfill_lsn", stats.BackfillLSN,
	)
	return stats, nil
}
