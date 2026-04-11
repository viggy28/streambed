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
		return d.decodeUpdate(m)

	case *pglogrepl.DeleteMessage:
		return d.decodeDelete(m)

	case *pglogrepl.TruncateMessage:
		return d.decodeTruncate(m), nil

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
	var keyIdx []int
	for i, c := range m.Columns {
		isKey := c.Flags&1 != 0
		cols[i] = Column{
			Name:     c.Name,
			OID:      c.DataType,
			Modifier: c.TypeModifier,
			IsKey:    isKey,
		}
		if isKey {
			keyIdx = append(keyIdx, i)
		}
	}
	rel := &RelationMessage{
		RelationID:       m.RelationID,
		Namespace:        m.Namespace,
		Name:             m.RelationName,
		Columns:          cols,
		KeyColumnIndexes: keyIdx,
	}
	d.relations[m.RelationID] = rel
	d.logger.Info("relation discovered",
		"schema", rel.Namespace,
		"table", rel.Name,
		"columns", len(rel.Columns),
		"key_columns", len(keyIdx),
	)
	return rel
}

// decodeTuple turns a pgoutput TupleData into ColumnValue slice using the
// relation's column metadata for names/OIDs. Unchanged-TOAST markers are
// translated to NULL to keep Phase 1 semantics; the caller is responsible
// for skipping events where that would be incorrect.
func (d *Decoder) decodeTuple(rel *RelationMessage, tup *pglogrepl.TupleData) []ColumnValue {
	row := make([]ColumnValue, len(tup.Columns))
	for i, col := range tup.Columns {
		cv := ColumnValue{
			Name: rel.Columns[i].Name,
			OID:  rel.Columns[i].OID,
		}
		switch col.DataType {
		case 'n':
			cv.IsNull = true
		case 't':
			cv.Value = col.Data
		case 'u':
			cv.IsNull = true
			d.logger.Debug("unchanged TOAST value treated as null",
				"table", rel.Name,
				"column", cv.Name,
			)
		}
		row[i] = cv
	}
	return row
}

func (d *Decoder) decodeInsert(m *pglogrepl.InsertMessage) (*InsertMessage, error) {
	rel, ok := d.relations[m.RelationID]
	if !ok {
		return nil, fmt.Errorf("insert for unknown relation %d", m.RelationID)
	}
	return &InsertMessage{
		RelationID: m.RelationID,
		Row:        d.decodeTuple(rel, m.Tuple),
	}, nil
}

// decodeUpdate extracts both the OLD key tuple (for the equality delete) and
// the NEW full tuple from a pgoutput update message.
//
// Postgres sends the OLD tuple in two situations:
//   - 'K': REPLICA IDENTITY DEFAULT and the update modified a key column.
//     Only key columns are present in the tuple.
//   - 'O': REPLICA IDENTITY FULL. All columns are present.
//
// When neither 'K' nor 'O' is sent, the update did not change key columns;
// we can reuse the key-column values from the NEW tuple to identify the
// pre-update row (they are identical by definition).
func (d *Decoder) decodeUpdate(m *pglogrepl.UpdateMessage) (*UpdateMessage, error) {
	rel, ok := d.relations[m.RelationID]
	if !ok {
		return nil, fmt.Errorf("update for unknown relation %d", m.RelationID)
	}
	newRow := d.decodeTuple(rel, m.NewTuple)

	var oldKey []ColumnValue
	if len(rel.KeyColumnIndexes) == 0 {
		// No replica identity key → caller will skip this event.
		return &UpdateMessage{RelationID: m.RelationID, NewRow: newRow}, nil
	}
	if m.OldTuple != nil {
		oldFull := d.decodeTuple(rel, m.OldTuple)
		oldKey = make([]ColumnValue, len(rel.KeyColumnIndexes))
		for i, idx := range rel.KeyColumnIndexes {
			oldKey[i] = oldFull[idx]
		}
	} else {
		// Key unchanged: derive from NEW tuple.
		oldKey = make([]ColumnValue, len(rel.KeyColumnIndexes))
		for i, idx := range rel.KeyColumnIndexes {
			oldKey[i] = newRow[idx]
		}
	}
	return &UpdateMessage{
		RelationID: m.RelationID,
		OldKey:     oldKey,
		NewRow:     newRow,
	}, nil
}

// decodeTruncate captures the list of relations affected by a pgoutput
// Truncate message. Unknown relation IDs are logged and dropped here so
// the consumer only sees relations we have metadata for.
func (d *Decoder) decodeTruncate(m *pglogrepl.TruncateMessage) *TruncateMessage {
	ids := make([]uint32, 0, len(m.RelationIDs))
	for _, relID := range m.RelationIDs {
		if _, ok := d.relations[relID]; !ok {
			d.logger.Warn("TRUNCATE for unknown relation, dropping", "relation_id", relID)
			continue
		}
		ids = append(ids, relID)
	}
	return &TruncateMessage{RelationIDs: ids}
}

// decodeDelete extracts the key tuple from a pgoutput delete message.
func (d *Decoder) decodeDelete(m *pglogrepl.DeleteMessage) (*DeleteMessage, error) {
	rel, ok := d.relations[m.RelationID]
	if !ok {
		return nil, fmt.Errorf("delete for unknown relation %d", m.RelationID)
	}
	if m.OldTuple == nil || len(rel.KeyColumnIndexes) == 0 {
		// No key available → caller will skip this event.
		return &DeleteMessage{RelationID: m.RelationID}, nil
	}
	oldFull := d.decodeTuple(rel, m.OldTuple)
	oldKey := make([]ColumnValue, len(rel.KeyColumnIndexes))
	for i, idx := range rel.KeyColumnIndexes {
		oldKey[i] = oldFull[idx]
	}
	return &DeleteMessage{
		RelationID: m.RelationID,
		OldKey:     oldKey,
	}, nil
}
