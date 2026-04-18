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

// registerTwoColRelation sets up a relation where column 0 ("id") is the
// REPLICA IDENTITY key (Flags&1 != 0) and column 1 ("name") is a regular
// column. Used by the UPDATE/DELETE tests below.
func registerTwoColRelation(d *Decoder, relID uint32) {
	d.Decode(&pglogrepl.RelationMessage{
		RelationID:   relID,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Flags: 1, Name: "id", DataType: 23}, // key
			{Flags: 0, Name: "name", DataType: 25},
		},
	})
}

func TestDecodeRelation_CapturesKeyColumns(t *testing.T) {
	d := testDecoder()
	registerTwoColRelation(d, 100)

	rel := d.Relations()[100]
	if len(rel.KeyColumnIndexes) != 1 || rel.KeyColumnIndexes[0] != 0 {
		t.Fatalf("expected KeyColumnIndexes=[0], got %v", rel.KeyColumnIndexes)
	}
	if !rel.Columns[0].IsKey {
		t.Error("column 0 should be marked IsKey")
	}
	if rel.Columns[1].IsKey {
		t.Error("column 1 should not be marked IsKey")
	}
}

// Unchanged key (no old tuple): the decoder must reconstruct OldKey from
// the NEW tuple's key-column values.
func TestDecodeUpdate_KeyUnchangedUsesNewTuple(t *testing.T) {
	d := testDecoder()
	registerTwoColRelation(d, 1)

	result, err := d.Decode(&pglogrepl.UpdateMessage{
		RelationID: 1,
		// No OldTuple: key was not modified.
		NewTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("42")},
				{DataType: 't', Data: []byte("bob")},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	upd, ok := result.(*UpdateMessage)
	if !ok {
		t.Fatalf("expected *UpdateMessage, got %T", result)
	}
	if len(upd.OldKey) != 1 {
		t.Fatalf("expected 1 key column, got %d", len(upd.OldKey))
	}
	if string(upd.OldKey[0].Value) != "42" {
		t.Errorf("OldKey[0]=%q, want 42", upd.OldKey[0].Value)
	}
	if string(upd.NewRow[1].Value) != "bob" {
		t.Errorf("NewRow[1]=%q, want bob", upd.NewRow[1].Value)
	}
}

// Key changed ('K' old-tuple): the decoder picks the OLD key value out of
// the key-only old tuple, not the new tuple.
func TestDecodeUpdate_KeyChangedUsesOldTuple(t *testing.T) {
	d := testDecoder()
	registerTwoColRelation(d, 1)

	result, err := d.Decode(&pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: 'K',
		OldTuple: &pglogrepl.TupleData{
			// With 'K', pglogrepl still gives us a tuple sized to the
			// relation's columns; non-key positions appear as nulls.
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("7")}, // old id
				{DataType: 'n'},
			},
		},
		NewTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("8")}, // new id
				{DataType: 't', Data: []byte("carol")},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	upd := result.(*UpdateMessage)
	if string(upd.OldKey[0].Value) != "7" {
		t.Errorf("OldKey[0]=%q, want 7 (old id)", upd.OldKey[0].Value)
	}
	if string(upd.NewRow[0].Value) != "8" {
		t.Errorf("NewRow[0]=%q, want 8 (new id)", upd.NewRow[0].Value)
	}
}

// REPLICA IDENTITY FULL ('O' old-tuple): all columns present in the old
// tuple, but we still only extract the key-column positions.
func TestDecodeUpdate_FullOldTuple(t *testing.T) {
	d := testDecoder()
	registerTwoColRelation(d, 1)

	result, err := d.Decode(&pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: 'O',
		OldTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("9")},
				{DataType: 't', Data: []byte("dave-old")},
			},
		},
		NewTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("9")},
				{DataType: 't', Data: []byte("dave-new")},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	upd := result.(*UpdateMessage)
	if len(upd.OldKey) != 1 || string(upd.OldKey[0].Value) != "9" {
		t.Errorf("OldKey=%+v, want single value '9'", upd.OldKey)
	}
}

// Tables without REPLICA IDENTITY key columns: decoder returns the
// message with no OldKey. The consumer is responsible for skipping these.
func TestDecodeUpdate_NoKeyColumns(t *testing.T) {
	d := testDecoder()
	d.Decode(&pglogrepl.RelationMessage{
		RelationID:   2,
		Namespace:    "public",
		RelationName: "nokey",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Flags: 0, Name: "val", DataType: 23},
		},
	})

	result, err := d.Decode(&pglogrepl.UpdateMessage{
		RelationID: 2,
		NewTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte("1")}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	upd := result.(*UpdateMessage)
	if upd.OldKey != nil {
		t.Errorf("expected nil OldKey, got %+v", upd.OldKey)
	}
}

func TestDecodeDelete(t *testing.T) {
	d := testDecoder()
	registerTwoColRelation(d, 3)

	result, err := d.Decode(&pglogrepl.DeleteMessage{
		RelationID:   3,
		OldTupleType: 'K',
		OldTuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("55")},
				{DataType: 'n'},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	del, ok := result.(*DeleteMessage)
	if !ok {
		t.Fatalf("expected *DeleteMessage, got %T", result)
	}
	if len(del.OldKey) != 1 || string(del.OldKey[0].Value) != "55" {
		t.Errorf("OldKey=%+v, want ['55']", del.OldKey)
	}
}

func TestDecodeDelete_NoOldTuple(t *testing.T) {
	d := testDecoder()
	registerTwoColRelation(d, 4)

	// Postgres would not normally send a delete with no old tuple, but
	// the decoder must degrade gracefully rather than panic.
	result, err := d.Decode(&pglogrepl.DeleteMessage{RelationID: 4})
	if err != nil {
		t.Fatal(err)
	}
	del := result.(*DeleteMessage)
	if del.OldKey != nil {
		t.Errorf("expected nil OldKey when OldTuple missing, got %+v", del.OldKey)
	}
}

func TestDecodeTruncate_SingleTable(t *testing.T) {
	d := testDecoder()
	registerTwoColRelation(d, 10)

	result, err := d.Decode(&pglogrepl.TruncateMessage{
		RelationNum: 1,
		RelationIDs: []uint32{10},
	})
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := result.(*TruncateMessage)
	if !ok {
		t.Fatalf("expected *TruncateMessage, got %T", result)
	}
	if len(tr.RelationIDs) != 1 || tr.RelationIDs[0] != 10 {
		t.Errorf("RelationIDs=%v, want [10]", tr.RelationIDs)
	}
}

func TestDecodeTruncate_MultiTable(t *testing.T) {
	d := testDecoder()
	registerTwoColRelation(d, 20)
	d.Decode(&pglogrepl.RelationMessage{
		RelationID:   21,
		Namespace:    "public",
		RelationName: "other",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Flags: 1, Name: "id", DataType: 23},
		},
	})

	result, err := d.Decode(&pglogrepl.TruncateMessage{
		RelationNum: 2,
		RelationIDs: []uint32{20, 21},
	})
	if err != nil {
		t.Fatal(err)
	}
	tr := result.(*TruncateMessage)
	if len(tr.RelationIDs) != 2 {
		t.Fatalf("expected 2 relations, got %d", len(tr.RelationIDs))
	}
}

// A TRUNCATE carrying a relation the decoder hasn't seen yet is dropped
// at decode time — the consumer only sees known relations.
func TestDecodeTruncate_DropsUnknownRelation(t *testing.T) {
	d := testDecoder()
	registerTwoColRelation(d, 30)

	result, err := d.Decode(&pglogrepl.TruncateMessage{
		RelationNum: 2,
		RelationIDs: []uint32{30, 99999},
	})
	if err != nil {
		t.Fatal(err)
	}
	tr := result.(*TruncateMessage)
	if len(tr.RelationIDs) != 1 || tr.RelationIDs[0] != 30 {
		t.Errorf("RelationIDs=%v, want [30]", tr.RelationIDs)
	}
}

func TestDecodeUpdate_UnknownRelation(t *testing.T) {
	d := testDecoder()
	_, err := d.Decode(&pglogrepl.UpdateMessage{
		RelationID: 999,
		NewTuple:   &pglogrepl.TupleData{},
	})
	if err == nil {
		t.Error("expected error for unknown relation")
	}
}

func TestDiffRelationColumns_AddColumn(t *testing.T) {
	old := []Column{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	new := []Column{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
		{Name: "email", OID: 25},
	}
	changes := diffRelationColumns(old, new, []int{0}, []int{0})
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %+v", len(changes), changes)
	}
	if changes[0].Type != SchemaChangeAdd || changes[0].Column != "email" {
		t.Errorf("expected ADD email, got %+v", changes[0])
	}
}

func TestDiffRelationColumns_DropColumn(t *testing.T) {
	old := []Column{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
		{Name: "email", OID: 25},
	}
	new := []Column{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	changes := diffRelationColumns(old, new, []int{0}, []int{0})
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %+v", len(changes), changes)
	}
	if changes[0].Type != SchemaChangeDrop || changes[0].Column != "email" {
		t.Errorf("expected DROP email, got %+v", changes[0])
	}
}

func TestDiffRelationColumns_TypeChange(t *testing.T) {
	old := []Column{
		{Name: "id", OID: 23}, // int4
		{Name: "count", OID: 23},
	}
	new := []Column{
		{Name: "id", OID: 23},
		{Name: "count", OID: 20}, // int8
	}
	changes := diffRelationColumns(old, new, []int{0}, []int{0})
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %+v", len(changes), changes)
	}
	if changes[0].Type != SchemaChangeTypeChange || changes[0].OldOID != 23 || changes[0].NewOID != 20 {
		t.Errorf("expected TYPE_CHANGE int4→int8, got %+v", changes[0])
	}
}

func TestDiffRelationColumns_KeyChange(t *testing.T) {
	cols := []Column{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	changes := diffRelationColumns(cols, cols, []int{0}, []int{0, 1})
	found := false
	for _, ch := range changes {
		if ch.Type == SchemaChangeKeyChange {
			found = true
		}
	}
	if !found {
		t.Error("expected KEY_CHANGE, got none")
	}
}

func TestDiffRelationColumns_NoChanges(t *testing.T) {
	cols := []Column{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	changes := diffRelationColumns(cols, cols, []int{0}, []int{0})
	if len(changes) != 0 {
		t.Errorf("expected no changes, got %+v", changes)
	}
}

func TestDecodeRelation_DetectsSchemaChange(t *testing.T) {
	d := testDecoder()

	// First relation — no changes expected.
	d.Decode(&pglogrepl.RelationMessage{
		RelationID:   16384,
		Namespace:    "public",
		RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},
			{Name: "name", DataType: 25},
		},
	})

	// Second relation with same ID but ADD COLUMN.
	result, _ := d.Decode(&pglogrepl.RelationMessage{
		RelationID:   16384,
		Namespace:    "public",
		RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},
			{Name: "name", DataType: 25},
			{Name: "email", DataType: 25},
		},
	})

	rel := result.(*RelationMessage)
	if len(rel.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(rel.Changes))
	}
	if rel.Changes[0].Type != SchemaChangeAdd || rel.Changes[0].Column != "email" {
		t.Errorf("expected ADD email, got %+v", rel.Changes[0])
	}
}

func TestDecodeRelation_FirstDiscoveryNoChanges(t *testing.T) {
	d := testDecoder()

	result, _ := d.Decode(&pglogrepl.RelationMessage{
		RelationID:   16384,
		Namespace:    "public",
		RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},
		},
	})

	rel := result.(*RelationMessage)
	if len(rel.Changes) != 0 {
		t.Errorf("expected no changes on first discovery, got %+v", rel.Changes)
	}
}
