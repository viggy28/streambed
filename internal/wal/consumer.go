package wal

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/viggy28/streambed/internal/state"
)

const (
	standbyTimeout = 10 * time.Second
)

// Consumer reads WAL events from a Postgres replication slot.
type Consumer struct {
	conn          *pgconn.PgConn
	slotName      string
	publication   string
	decoder       *Decoder
	startLSN      pglogrepl.LSN
	clientXLogPos pglogrepl.LSN
	logger        *slog.Logger
	excludeTables map[string]bool
	state         *state.Store
}

// NewConsumer creates a new WAL consumer.
func NewConsumer(conn *pgconn.PgConn, slotName, publication string, startLSN pglogrepl.LSN, excludeTables []string, logger *slog.Logger, state *state.Store) *Consumer {
	exclude := make(map[string]bool)
	for _, t := range excludeTables {
		exclude[t] = true
	}
	return &Consumer{
		conn:          conn,
		slotName:      slotName,
		publication:   publication,
		decoder:       NewDecoder(logger),
		startLSN:      startLSN,
		clientXLogPos: startLSN,
		logger:        logger,
		excludeTables: exclude,
		state:         state,
	}
}

// Start begins consuming WAL events. Blocks until ctx is cancelled.
// Sends RowEvents to the events channel.
// Reads flushed LSN from ackCh to send standby status updates.
func (c *Consumer) Start(ctx context.Context, events chan<- RowEvent, ackCh <-chan pglogrepl.LSN) error {
	err := pglogrepl.StartReplication(ctx, c.conn, c.slotName, c.startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", c.publication),
			},
		},
	)
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	c.logger.Info("replication started",
		"slot", c.slotName,
		"start_lsn", c.startLSN,
	)

	nextStandbyDeadline := time.Now().Add(standbyTimeout)
	var flushedLSN pglogrepl.LSN
	tableFlushLSN := make(map[string]pglogrepl.LSN)
	flushLSN, err := c.state.GetFlushedLSN()
	if err != nil {
		return fmt.Errorf("error getting flush LSN for all tables: %w", err)
	}
	for table, lsnStr := range flushLSN {
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			return fmt.Errorf("parse LSN %q: %w", lsnStr, err)
		}
		tableFlushLSN[table] = lsn
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check for flushed LSN updates (non-blocking)
		select {
		case lsn := <-ackCh:
			flushedLSN = lsn
		default:
		}

		// Send standby status if needed
		if time.Now().After(nextStandbyDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, c.conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: flushedLSN,
					WALFlushPosition: flushedLSN,
					WALApplyPosition: flushedLSN,
				},
			)
			if err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}
			c.logger.Debug("standby status sent", "flushed_lsn", flushedLSN)
			nextStandbyDeadline = time.Now().Add(standbyTimeout)
		}

		// Receive message with timeout
		rawMsg, err := c.conn.ReceiveMessage(ctx)
		if err != nil {
			if pgconn.Timeout(err) {
				continue
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

			decoded, err := c.decoder.Decode(msg)
			if err != nil {
				c.logger.Error("decode error", "error", err)
				continue
			}

			switch v := decoded.(type) {
			case *RelationMessage:
				// Check exclude filter
				key := fmt.Sprintf("%s.%s", v.Namespace, v.Name)
				if c.excludeTables[key] {
					c.logger.Debug("excluding table", "table", key)
				}

			case *InsertMessage:
				c.logger.Info("received insert message", "lsn", xld.WALStart.String())
				rel := c.decoder.Relations()[v.RelationID]
				if rel == nil {
					continue
				}
				key := fmt.Sprintf("%s.%s", rel.Namespace, rel.Name)
				if c.excludeTables[key] {
					continue
				}
				// dedup logic - if the table is already processed skip it
				storeLsn := tableFlushLSN[rel.Name]
				if storeLsn >= xld.WALStart {
					continue
				}

				event := RowEvent{
					Schema:      rel.Namespace,
					Table:       rel.Name,
					Columns:     rel.Columns,
					Values:      v.Row,
					WALStartLSN: xld.WALStart,
				}
				select {
				case events <- event:
				case <-ctx.Done():
					return ctx.Err()
				}

			case *pglogrepl.CommitMessage:
				c.logger.Info("received commit message", "commitLSN", v.CommitLSN.String())

			case *pglogrepl.BeginMessage:
				// no-op
			}

			c.clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}
