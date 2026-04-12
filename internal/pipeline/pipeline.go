package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/wal"
)

const (
	standbyTimeout = 10 * time.Second
)

// Pipeline is a single-goroutine sync engine that reads WAL events from
// Postgres and writes them to S3/Iceberg. It replaces the previous
// two-goroutine Consumer+Writer architecture, eliminating the channels
// between them and simplifying ack bookkeeping.
type Pipeline struct {
	conn          *pgconn.PgConn
	slotName      string
	publication   string
	decoder       *wal.Decoder
	startLSN      pglogrepl.LSN
	excludeTables map[string]bool
	state         *state.Store
	tableFlushLSN map[string]pglogrepl.LSN

	writer        *iceberg.Writer
	flushInterval time.Duration
	logger        *slog.Logger
}

// New creates a Pipeline that reads WAL from conn and flushes to the
// provided Writer. tableFlushLSN is the per-table last flushed LSN read
// from Iceberg at startup, keyed by "schema.table".
func New(
	conn *pgconn.PgConn,
	slotName, publication string,
	startLSN pglogrepl.LSN,
	excludeTables []string,
	logger *slog.Logger,
	stateStore *state.Store,
	tableFlushLSN map[string]pglogrepl.LSN,
	writer *iceberg.Writer,
	flushInterval time.Duration,
) *Pipeline {
	exclude := make(map[string]bool)
	for _, t := range excludeTables {
		exclude[t] = true
	}
	return &Pipeline{
		conn:          conn,
		slotName:      slotName,
		publication:   publication,
		decoder:       wal.NewDecoder(logger),
		startLSN:      startLSN,
		excludeTables: exclude,
		state:         stateStore,
		tableFlushLSN: tableFlushLSN,
		writer:        writer,
		flushInterval: flushInterval,
		logger:        logger,
	}
}

// Run starts the pipeline. It blocks until ctx is cancelled, then
// performs a final flush before returning.
func (p *Pipeline) Run(ctx context.Context) error {
	err := pglogrepl.StartReplication(ctx, p.conn, p.slotName, p.startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", p.publication),
			},
		},
	)
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	p.logger.Info("replication started",
		"slot", p.slotName,
		"start_lsn", p.startLSN,
	)

	nextStandbyDeadline := time.Now().Add(standbyTimeout)
	nextFlushDeadline := time.Now().Add(p.flushInterval)

	// receivedLSN — highest WAL position observed from Postgres.
	receivedLSN := p.startLSN
	tableFlushLSN := p.tableFlushLSN

	// Load backfill filters for overlap suppression after resync.
	backfillFilters, err := p.state.GetBackfillLSNs()
	if err != nil {
		return fmt.Errorf("load backfill filters: %w", err)
	}
	if len(backfillFilters) > 0 {
		p.logger.Info("backfill overlap filters active",
			"tables", len(backfillFilters),
		)
	}

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("shutdown: flushing remaining buffers")
			if err := p.writer.FlushAll(context.Background()); err != nil {
				return fmt.Errorf("final flush: %w", err)
			}
			return ctx.Err()
		default:
		}

		// Compute next wakeup: whichever timer fires first.
		nextDeadline := nextStandbyDeadline
		if nextFlushDeadline.Before(nextDeadline) {
			nextDeadline = nextFlushDeadline
		}

		// Receive the next WAL message, timing out at the nearest
		// deadline so we can handle standby and flush timers.
		recvCtx, recvCancel := context.WithDeadline(ctx, nextDeadline)
		rawMsg, err := p.conn.ReceiveMessage(recvCtx)
		recvCancel()

		if err != nil {
			if pgconn.Timeout(err) {
				// Timer-driven work: flush and/or standby.
				now := time.Now()
				if !now.Before(nextFlushDeadline) {
					if err := p.writer.FlushAll(ctx); err != nil {
						return err
					}
					nextFlushDeadline = now.Add(p.flushInterval)
				}
				if !now.Before(nextStandbyDeadline) {
					if err := p.sendStandby(ctx, receivedLSN); err != nil {
						return err
					}
					nextStandbyDeadline = now.Add(standbyTimeout)
				}
				continue
			}
			if ctx.Err() != nil {
				// Parent context cancelled — do final flush.
				p.logger.Info("shutdown: flushing remaining buffers")
				if fErr := p.writer.FlushAll(context.Background()); fErr != nil {
					return fmt.Errorf("final flush: %w", fErr)
				}
				return ctx.Err()
			}
			return fmt.Errorf("receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("postgres error: %s: %s", errMsg.Code, errMsg.Message)
		}

		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ServerWALEnd > receivedLSN {
				receivedLSN = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandbyDeadline = time.Time{} // force immediate reply
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog data: %w", err)
			}

			msg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("parse wal message: %w", err)
			}

			decoded, err := p.decoder.Decode(msg)
			if err != nil {
				p.logger.Error("decode error", "error", err)
				continue
			}

			// shouldProcess applies the exclude list, backfill-overlap
			// filter, and already-flushed dedup check.
			shouldProcess := func(relID uint32) (*wal.RelationMessage, bool) {
				rel := p.decoder.Relations()[relID]
				if rel == nil {
					return nil, false
				}
				key := fmt.Sprintf("%s.%s", rel.Namespace, rel.Name)
				if p.excludeTables[key] {
					return rel, false
				}
				if filterLSN, hasFilter := backfillFilters[key]; hasFilter {
					if xld.WALStart <= filterLSN {
						p.logger.Debug("dropping backfill-overlap event",
							"table", key,
							"event_lsn", xld.WALStart,
							"filter_lsn", filterLSN,
						)
						return rel, false
					}
					delete(backfillFilters, key)
					if err := p.state.ClearBackfillLSN(rel.Namespace, rel.Name); err != nil {
						p.logger.Warn("clear backfill_lsn", "table", key, "error", err)
					} else {
						p.logger.Info("backfill overlap filter cleared",
							"table", key,
							"at_lsn", xld.WALStart,
						)
					}
				}
				if storeLsn := tableFlushLSN[key]; storeLsn >= xld.WALStart {
					return rel, false
				}
				return rel, true
			}

			switch v := decoded.(type) {
			case *wal.RelationMessage:
				key := fmt.Sprintf("%s.%s", v.Namespace, v.Name)
				if p.excludeTables[key] {
					p.logger.Debug("excluding table", "table", key)
				}

			case *wal.InsertMessage:
				p.logger.Info("received insert message", "lsn", xld.WALStart.String())
				rel, ok := shouldProcess(v.RelationID)
				if !ok {
					continue
				}
				if _, err := p.writer.HandleEvent(ctx, wal.RowEvent{
					Schema:      rel.Namespace,
					Table:       rel.Name,
					Columns:     rel.Columns,
					KeyColumns:  rel.KeyColumnIndexes,
					Op:          wal.OpInsert,
					Values:      v.Row,
					WALStartLSN: xld.WALStart,
				}); err != nil {
					return err
				}

			case *wal.UpdateMessage:
				p.logger.Info("received update message", "lsn", xld.WALStart.String())
				rel, ok := shouldProcess(v.RelationID)
				if !ok {
					continue
				}
				if len(rel.KeyColumnIndexes) == 0 {
					p.logger.Warn("skipping UPDATE on table with no REPLICA IDENTITY key",
						"table", fmt.Sprintf("%s.%s", rel.Namespace, rel.Name),
						"lsn", xld.WALStart.String(),
					)
					continue
				}
				if _, err := p.writer.HandleEvent(ctx, wal.RowEvent{
					Schema:      rel.Namespace,
					Table:       rel.Name,
					Columns:     rel.Columns,
					KeyColumns:  rel.KeyColumnIndexes,
					Op:          wal.OpUpdate,
					Values:      v.NewRow,
					OldKey:      v.OldKey,
					WALStartLSN: xld.WALStart,
				}); err != nil {
					return err
				}

			case *wal.DeleteMessage:
				p.logger.Info("received delete message", "lsn", xld.WALStart.String())
				rel, ok := shouldProcess(v.RelationID)
				if !ok {
					continue
				}
				if len(rel.KeyColumnIndexes) == 0 || v.OldKey == nil {
					p.logger.Warn("skipping DELETE without usable key",
						"table", fmt.Sprintf("%s.%s", rel.Namespace, rel.Name),
						"lsn", xld.WALStart.String(),
					)
					continue
				}
				if _, err := p.writer.HandleEvent(ctx, wal.RowEvent{
					Schema:      rel.Namespace,
					Table:       rel.Name,
					Columns:     rel.Columns,
					KeyColumns:  rel.KeyColumnIndexes,
					Op:          wal.OpDelete,
					OldKey:      v.OldKey,
					WALStartLSN: xld.WALStart,
				}); err != nil {
					return err
				}

			case *wal.TruncateMessage:
				p.logger.Info("received truncate message",
					"lsn", xld.WALStart.String(),
					"relations", len(v.RelationIDs),
				)
				for _, relID := range v.RelationIDs {
					rel, ok := shouldProcess(relID)
					if !ok {
						continue
					}
					if _, err := p.writer.HandleEvent(ctx, wal.RowEvent{
						Schema:      rel.Namespace,
						Table:       rel.Name,
						Columns:     rel.Columns,
						KeyColumns:  rel.KeyColumnIndexes,
						Op:          wal.OpTruncate,
						WALStartLSN: xld.WALStart,
					}); err != nil {
						return err
					}
				}

			case *pglogrepl.CommitMessage:
				p.logger.Info("received commit message", "commitLSN", v.CommitLSN.String())

			case *pglogrepl.BeginMessage:
				// no-op
			}

			// Advance receivedLSN past this message.
			endLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			if endLSN > receivedLSN {
				receivedLSN = endLSN
			}
		}
	}
}

// sendStandby computes the safe ack position and sends a standby status
// update to Postgres. The ack is held back to the oldest unflushed
// buffer's FirstLSN-1 so the slot won't advance past data we haven't
// durably written to Iceberg yet.
func (p *Pipeline) sendStandby(ctx context.Context, receivedLSN pglogrepl.LSN) error {
	ack := receivedLSN
	pendingMinLSN := p.writer.ComputePendingMinLSN()
	if pendingMinLSN != 0 {
		hold := pendingMinLSN - 1
		if hold < ack {
			ack = hold
		}
	}
	err := pglogrepl.SendStandbyStatusUpdate(ctx, p.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: ack,
			WALFlushPosition: ack,
			WALApplyPosition: ack,
		},
	)
	if err != nil {
		return fmt.Errorf("send standby status: %w", err)
	}
	p.logger.Debug("standby status sent",
		"ack", ack,
		"received_lsn", receivedLSN,
		"pending_min_lsn", pendingMinLSN,
	)
	return nil
}
