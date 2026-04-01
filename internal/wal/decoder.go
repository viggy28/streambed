package wal

import (
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
)

// Decoder parses pgoutput WAL messages into typed events.
type Decoder struct {
	relations map[uint32]*RelationMessage
	logger    *slog.Logger
}

func NewDecoder(logger *slog.Logger) *Decoder {
	return &Decoder{
		relations: make(map[uint32]*RelationMessage),
		logger:    logger,
	}
}

// Relations returns the cached relation map.
func (d *Decoder) Relations() map[uint32]*RelationMessage {
	return d.relations
}

// Decode takes a pglogrepl.Message and returns a typed event.
// Returns nil for messages we don't handle in Phase 1.
func (d *Decoder) Decode(msg pglogrepl.Message) (interface{}, error) {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		return d.decodeRelation(m), nil

	case *pglogrepl.InsertMessage:
		return d.decodeInsert(m)

	case *pglogrepl.BeginMessage:
		return m, nil

	case *pglogrepl.CommitMessage:
		return m, nil

	case *pglogrepl.UpdateMessage:
		d.logger.Warn("UPDATE message received, skipping (Phase 2)")
		return nil, nil

	case *pglogrepl.DeleteMessage:
		d.logger.Warn("DELETE message received, skipping (Phase 2)")
		return nil, nil

	case *pglogrepl.TruncateMessage:
		d.logger.Warn("TRUNCATE message received, skipping (Phase 2)")
		return nil, nil

	case *pglogrepl.TypeMessage:
		return nil, nil

	case *pglogrepl.OriginMessage:
		return nil, nil

	default:
		return nil, nil
	}
}

func (d *Decoder) decodeRelation(m *pglogrepl.RelationMessage) *RelationMessage {
	cols := make([]Column, len(m.Columns))
	for i, c := range m.Columns {
		cols[i] = Column{
			Name:     c.Name,
			OID:      c.DataType,
			Modifier: c.TypeModifier,
		}
	}
	rel := &RelationMessage{
		RelationID: m.RelationID,
		Namespace:  m.Namespace,
		Name:       m.RelationName,
		Columns:    cols,
	}
	d.relations[m.RelationID] = rel
	d.logger.Info("relation discovered",
		"schema", rel.Namespace,
		"table", rel.Name,
		"columns", len(rel.Columns),
	)
	return rel
}

func (d *Decoder) decodeInsert(m *pglogrepl.InsertMessage) (*InsertMessage, error) {
	rel, ok := d.relations[m.RelationID]
	if !ok {
		return nil, fmt.Errorf("insert for unknown relation %d", m.RelationID)
	}

	row := make([]ColumnValue, len(m.Tuple.Columns))
	for i, col := range m.Tuple.Columns {
		cv := ColumnValue{
			Name: rel.Columns[i].Name,
			OID:  rel.Columns[i].OID,
		}
		switch col.DataType {
		case 'n': // null
			cv.IsNull = true
		case 't': // text
			cv.Value = col.Data
		case 'u': // unchanged TOAST
			cv.IsNull = true // treat as null in Phase 1
			d.logger.Debug("unchanged TOAST value treated as null",
				"table", rel.Name,
				"column", cv.Name,
			)
		}
		row[i] = cv
	}

	return &InsertMessage{
		RelationID: m.RelationID,
		Row:        row,
	}, nil
}
