package wal

import (
	"log/slog"
	"os"
	"testing"

	"github.com/jackc/pglogrepl"
)

func testDecoder() *Decoder {
	return NewDecoder(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
}

func TestDecodeRelation(t *testing.T) {
	d := testDecoder()
	msg := &pglogrepl.RelationMessage{
		RelationID:   16384,
		Namespace:    "public",
		RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23, TypeModifier: -1},
			{Name: "customer", DataType: 25, TypeModifier: -1},
			{Name: "amount", DataType: 1700, TypeModifier: -1},
		},
	}

	result, err := d.Decode(msg)
	if err != nil {
		t.Fatal(err)
	}

	rel, ok := result.(*RelationMessage)
	if !ok {
		t.Fatalf("expected *RelationMessage, got %T", result)
	}
	if rel.Namespace != "public" {
		t.Errorf("expected namespace 'public', got %q", rel.Namespace)
	}
	if rel.Name != "orders" {
		t.Errorf("expected name 'orders', got %q", rel.Name)
	}
	if len(rel.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(rel.Columns))
	}
	if rel.Columns[0].OID != 23 {
		t.Errorf("expected first column OID 23, got %d", rel.Columns[0].OID)
	}

	// Verify it's cached
	if _, exists := d.Relations()[16384]; !exists {
		t.Error("relation not cached")
	}
}

func TestDecodeInsert(t *testing.T) {
	d := testDecoder()

	// First, register the relation
	d.Decode(&pglogrepl.RelationMessage{
		RelationID:   16384,
		Namespace:    "public",
		RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},
			{Name: "customer", DataType: 25},
		},
	})

	// Now decode an insert
	result, err := d.Decode(&pglogrepl.InsertMessage{
		RelationID: 16384,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("1")},
				{DataType: 't', Data: []byte("alice")},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ins, ok := result.(*InsertMessage)
	if !ok {
		t.Fatalf("expected *InsertMessage, got %T", result)
	}
	if len(ins.Row) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ins.Row))
	}
	if string(ins.Row[0].Value) != "1" {
		t.Errorf("expected id '1', got %q", ins.Row[0].Value)
	}
	if string(ins.Row[1].Value) != "alice" {
		t.Errorf("expected customer 'alice', got %q", ins.Row[1].Value)
	}
}

func TestDecodeInsertWithNull(t *testing.T) {
	d := testDecoder()

	d.Decode(&pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "test",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},
			{Name: "value", DataType: 25},
		},
	})

	result, err := d.Decode(&pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("1")},
				{DataType: 'n'}, // null
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ins := result.(*InsertMessage)
	if !ins.Row[1].IsNull {
		t.Error("expected null value")
	}
}

func TestDecodeInsertUnknownRelation(t *testing.T) {
	d := testDecoder()
	_, err := d.Decode(&pglogrepl.InsertMessage{
		RelationID: 99999,
		Tuple:      &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{}},
	})
	if err == nil {
		t.Error("expected error for unknown relation")
	}
}

func TestDecodeBeginCommit(t *testing.T) {
	d := testDecoder()

	result, err := d.Decode(&pglogrepl.BeginMessage{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.(*pglogrepl.BeginMessage); !ok {
		t.Errorf("expected *BeginMessage, got %T", result)
	}

	result, err = d.Decode(&pglogrepl.CommitMessage{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.(*pglogrepl.CommitMessage); !ok {
		t.Errorf("expected *CommitMessage, got %T", result)
	}
}

func TestDecodeUpdateDeleteSkipped(t *testing.T) {
	d := testDecoder()

	result, err := d.Decode(&pglogrepl.UpdateMessage{})
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Error("expected nil for Update in Phase 1")
	}

	result, err = d.Decode(&pglogrepl.DeleteMessage{})
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Error("expected nil for Delete in Phase 1")
	}
}
