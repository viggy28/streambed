package wal

import "github.com/jackc/pglogrepl"

// Column describes a single column from a Postgres Relation message.
type Column struct {
	Name     string
	OID      uint32
	Modifier int32
}

// RelationMessage is emitted when Postgres sends a Relation message.
type RelationMessage struct {
	RelationID uint32
	Namespace  string
	Name       string
	Columns    []Column
}

// ColumnValue holds a single decoded column value from an Insert message.
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

// RowEvent is sent from the consumer to the writer via channel.
type RowEvent struct {
	Schema      string
	Table       string
	Columns     []Column
	Values      []ColumnValue
	WALStartLSN pglogrepl.LSN
}
