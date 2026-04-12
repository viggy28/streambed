package wal

import "github.com/jackc/pglogrepl"

// Column describes a single column from a Postgres Relation message.
// IsKey is true when the column is part of the table's REPLICA IDENTITY
// (the "key" flag in pgoutput). These are the columns we can safely use
// to build Iceberg equality deletes for UPDATE/DELETE events.
type Column struct {
	Name     string
	OID      uint32
	Modifier int32
	IsKey    bool
}

// RelationMessage is emitted when Postgres sends a Relation message.
//
// KeyColumnIndexes lists the positions (into Columns) of the columns that
// form the REPLICA IDENTITY key. When empty, the table has no usable key
// and UPDATE/DELETE events for it cannot be expressed as Iceberg equality
// deletes — callers should log and skip those events.
type RelationMessage struct {
	RelationID       uint32
	Namespace        string
	Name             string
	Columns          []Column
	KeyColumnIndexes []int
}

// ColumnValue holds a single decoded column value from a WAL tuple.
type ColumnValue struct {
	Name   string
	OID    uint32
	Value  []byte
	IsNull bool
}

// InsertMessage represents a single INSERT decoded from the WAL.
type InsertMessage struct {
	RelationID uint32
	Row        []ColumnValue
}

// UpdateMessage represents a single UPDATE decoded from the WAL.
//
// OldKey carries the REPLICA IDENTITY columns of the row as it existed
// *before* the update. It may be nil when the UPDATE did not modify any
// key column (in which case the new-row values at the key positions are
// guaranteed to match the old-row values and can be used for the
// equality delete). NewRow always carries the full new tuple.
type UpdateMessage struct {
	RelationID uint32
	OldKey     []ColumnValue
	NewRow     []ColumnValue
}

// DeleteMessage represents a single DELETE decoded from the WAL.
// OldKey carries the REPLICA IDENTITY columns of the deleted row.
type DeleteMessage struct {
	RelationID uint32
	OldKey     []ColumnValue
}

// TruncateMessage represents a TRUNCATE affecting one or more tables.
// A single pgoutput Truncate carries every relation that was truncated
// in the same SQL statement (TRUNCATE a, b, c).
type TruncateMessage struct {
	RelationIDs []uint32
}

// Op identifies the kind of change a RowEvent represents.
type Op uint8

const (
	OpInsert Op = iota
	OpUpdate
	OpDelete
	// OpTruncate represents a TRUNCATE of a single table. The event
	// carries no row data; the writer handles it by discarding any
	// buffered rows for the table and committing an empty-replacement
	// Iceberg snapshot at the TRUNCATE's WAL position.
	OpTruncate
)

func (o Op) String() string {
	switch o {
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	case OpTruncate:
		return "TRUNCATE"
	default:
		return "UNKNOWN"
	}
}

// RowEvent is sent from the consumer to the writer via channel. It carries
// one decoded row change.
//
// Semantics by Op:
//   - OpInsert: Values is the new row; OldKey is nil.
//   - OpUpdate: Values is the new row; OldKey holds the REPLICA IDENTITY
//     values of the pre-update row (used to issue an equality delete for
//     the old image before appending the new).
//   - OpDelete: Values is nil; OldKey holds the REPLICA IDENTITY values
//     of the deleted row.
//
// KeyColumns lists the positions (into Columns) of the key columns. It
// is carried on every event so the writer doesn't need to track relation
// metadata separately.
type RowEvent struct {
	Schema      string
	Table       string
	Columns     []Column
	KeyColumns  []int
	Op          Op
	Values      []ColumnValue
	OldKey      []ColumnValue
	WALStartLSN pglogrepl.LSN
}
