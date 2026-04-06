package wal

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// CreateOrReuseSlot creates a replication slot if it doesn't exist, or returns
// the existing slot's confirmed_flush_lsn for resumption.
func CreateOrReuseSlot(ctx context.Context, conn *pgconn.PgConn, slotName string, logger *slog.Logger) (pglogrepl.LSN, error) {
	// Check if slot already exists
	result := conn.Exec(ctx, fmt.Sprintf(
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'",
		slotName,
	))
	if result.NextResult() {
		rows := result.ResultReader()
		if rows.NextRow() {
			lsnStr := string(rows.Values()[0])
			lsn, err := pglogrepl.ParseLSN(lsnStr)
			if err != nil {
				rows.Close()
				return 0, fmt.Errorf("parse existing slot LSN: %w", err)
			}
			rows.Close()
			logger.Info("reusing existing replication slot",
				"slot", slotName,
				"confirmed_flush_lsn", lsn,
			)
			return lsn, result.Close()
		}
		rows.Close()
	}
	if err := result.Close(); err != nil {
		return 0, fmt.Errorf("check slot existence: %w", err)
	}

	// Create new slot
	createResult, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
		},
	)
	if err != nil {
		return 0, fmt.Errorf("create replication slot %q: %w", slotName, err)
	}

	lsn, err := pglogrepl.ParseLSN(createResult.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("parse consistent point: %w", err)
	}

	logger.Info("created replication slot",
		"slot", slotName,
		"consistent_point", lsn,
	)
	return lsn, nil
}

// TempSlotWithSnapshot holds the result of creating a temporary replication
// slot that exports a consistent snapshot for backfill / resync reads.
type TempSlotWithSnapshot struct {
	// SlotName is the generated slot name (randomised per invocation so
	// concurrent resyncs can't collide).
	SlotName string
	// SnapshotName is the exported snapshot id, suitable for passing to
	// `SET TRANSACTION SNAPSHOT '<name>'` on a separate connection.
	SnapshotName string
	// ConsistentPoint is the WAL LSN at which the snapshot was exported.
	// Any WAL record at or before this LSN is already reflected in the
	// snapshot — the consumer should drop main-slot events with WAL
	// position ≤ ConsistentPoint to avoid duplicating backfilled rows.
	ConsistentPoint pglogrepl.LSN
}

// CreateTempSlotWithSnapshot creates a TEMPORARY logical replication slot
// using the pgoutput plugin with EXPORT_SNAPSHOT. The exported snapshot is
// valid only for the lifetime of the replication connection used here; the
// caller MUST keep that connection open while reading via SET TRANSACTION
// SNAPSHOT on a second connection. When the replication connection closes
// the temp slot is dropped automatically by Postgres — no clean-up required.
func CreateTempSlotWithSnapshot(ctx context.Context, conn *pgconn.PgConn, slotName string, logger *slog.Logger) (TempSlotWithSnapshot, error) {
	res, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary:      true,
			SnapshotAction: "EXPORT_SNAPSHOT",
		},
	)
	if err != nil {
		return TempSlotWithSnapshot{}, fmt.Errorf("create temp slot %q: %w", slotName, err)
	}

	lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		return TempSlotWithSnapshot{}, fmt.Errorf("parse consistent point %q: %w", res.ConsistentPoint, err)
	}

	logger.Info("created temp snapshot slot",
		"slot", slotName,
		"snapshot", res.SnapshotName,
		"consistent_point", lsn,
	)

	return TempSlotWithSnapshot{
		SlotName:        slotName,
		SnapshotName:    res.SnapshotName,
		ConsistentPoint: lsn,
	}, nil
}

// CreatePublication creates a publication if it doesn't exist.
func CreatePublication(ctx context.Context, conn *pgconn.PgConn, pubName string, includeTables []string, logger *slog.Logger) error {
	// Check if publication exists
	result := conn.Exec(ctx, fmt.Sprintf(
		"SELECT 1 FROM pg_publication WHERE pubname = '%s'", pubName,
	))
	exists := false
	if result.NextResult() {
		rows := result.ResultReader()
		if rows.NextRow() {
			exists = true
		}
		rows.Close()
	}
	if err := result.Close(); err != nil {
		return fmt.Errorf("check publication: %w", err)
	}

	if exists {
		logger.Info("reusing existing publication", "publication", pubName)
		return nil
	}

	var sql string
	if len(includeTables) > 0 {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, joinTables(includeTables))
	} else {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pubName)
	}

	result = conn.Exec(ctx, sql)
	_, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("create publication: %w", err)
	}

	logger.Info("created publication", "publication", pubName, "tables", includeTables)
	return nil
}

func joinTables(tables []string) string {
	s := ""
	for i, t := range tables {
		if i > 0 {
			s += ", "
		}
		s += t
	}
	return s
}
