package iceberg

import (
	"testing"

	pqbuilder "github.com/viggy28/streambed/internal/parquet"
)

func val(s string) pqbuilder.Value {
	return pqbuilder.Value{Data: []byte(s)}
}

func nullVal() pqbuilder.Value {
	return pqbuilder.Value{IsNull: true}
}

// row builds a full row from string values for testing.
func row(vals ...string) []pqbuilder.Value {
	r := make([]pqbuilder.Value, len(vals))
	for i, v := range vals {
		r[i] = val(v)
	}
	return r
}

func TestDedupRows_MultipleUpdates(t *testing.T) {
	// 3 UPDATEs on key "1": should collapse to last version.
	rows := [][]pqbuilder.Value{
		row("1", "alice"),
		row("1", "bob"),
		row("1", "charlie"),
	}
	keyColumns := []int{0}

	result := dedupRows(rows, keyColumns, nil)

	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if string(result[0][1].Data) != "charlie" {
		t.Fatalf("expected last version 'charlie', got %q", string(result[0][1].Data))
	}
}

func TestDedupRows_MultipleKeys(t *testing.T) {
	// Updates on 3 different keys, some repeated.
	rows := [][]pqbuilder.Value{
		row("1", "a1"),
		row("2", "b1"),
		row("1", "a2"), // supersedes first
		row("3", "c1"),
		row("2", "b2"), // supersedes second
	}
	keyColumns := []int{0}

	result := dedupRows(rows, keyColumns, nil)

	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}
	// Order preserves the position of the LAST occurrence of each key:
	// key 1 last at index 2, key 2 last at index 4, key 3 at index 3
	// → output order: key 1 (a2), key 3 (c1), key 2 (b2)
	want := map[string]string{"1": "a2", "2": "b2", "3": "c1"}
	for _, r := range result {
		k := string(r[0].Data)
		v := string(r[1].Data)
		if want[k] != v {
			t.Errorf("key %s = %q, want %q", k, v, want[k])
		}
	}
}

func TestDedupRows_NoKeyColumns(t *testing.T) {
	// Append-only table with no key — dedup should be a no-op.
	rows := [][]pqbuilder.Value{
		row("1", "a"),
		row("1", "b"),
	}

	result := dedupRows(rows, nil, nil)

	if len(result) != 2 {
		t.Fatalf("expected 2 rows (no dedup), got %d", len(result))
	}
}

func TestDedupRows_EmptyRows(t *testing.T) {
	result := dedupRows(nil, []int{0}, nil)
	if len(result) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result))
	}
}

func TestDedupRows_DeletedKeys(t *testing.T) {
	// Row for key "1" exists but key was net-deleted in the batch.
	rows := [][]pqbuilder.Value{
		row("1", "val"),
		row("2", "keep"),
	}
	keyColumns := []int{0}
	deletedKeys := map[string]bool{
		buildKeyString([]pqbuilder.Value{val("1")}): true,
	}

	result := dedupRows(rows, keyColumns, deletedKeys)

	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if string(result[0][0].Data) != "2" {
		t.Fatalf("expected key '2' to survive, got %q", string(result[0][0].Data))
	}
}

func TestDedupRows_UpdateThenDelete(t *testing.T) {
	// UPDATE key=1 (adds row), then DELETE key=1 (marks as deleted).
	// The row should NOT appear in deduped output.
	rows := [][]pqbuilder.Value{
		row("1", "updated"),
	}
	keyColumns := []int{0}
	deletedKeys := map[string]bool{
		buildKeyString([]pqbuilder.Value{val("1")}): true,
	}

	result := dedupRows(rows, keyColumns, deletedKeys)

	if len(result) != 0 {
		t.Fatalf("expected 0 rows (key was deleted), got %d", len(result))
	}
}

func TestDedupRows_CompositeKey(t *testing.T) {
	// 2-column composite key.
	rows := [][]pqbuilder.Value{
		row("a", "1", "v1"),
		row("a", "2", "v2"),
		row("a", "1", "v3"), // supersedes first
	}
	keyColumns := []int{0, 1}

	result := dedupRows(rows, keyColumns, nil)

	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	// key (a,2) last at index 1, key (a,1) last at index 2
	// → output order: (a,2) then (a,1)
	if string(result[0][2].Data) != "v2" {
		t.Errorf("expected 'v2' for key (a,2), got %q", string(result[0][2].Data))
	}
	if string(result[1][2].Data) != "v3" {
		t.Errorf("expected 'v3' for key (a,1), got %q", string(result[1][2].Data))
	}
}

func TestExtractKey(t *testing.T) {
	r := row("a", "b", "c", "d")
	kv := extractKey(r, []int{1, 3})
	if len(kv) != 2 {
		t.Fatalf("expected 2 key values, got %d", len(kv))
	}
	if string(kv[0].Data) != "b" || string(kv[1].Data) != "d" {
		t.Errorf("got key (%q, %q), want (b, d)", string(kv[0].Data), string(kv[1].Data))
	}
}

func TestBuildKeyString_NoCollision(t *testing.T) {
	// Keys with separator bytes in values must not collide.
	// Old implementation using \x01 separator would cause
	// ("a\x01b", "c") == ("a", "b\x01c").
	key1 := buildKeyString([]pqbuilder.Value{
		val("a\x01b"),
		val("c"),
	})
	key2 := buildKeyString([]pqbuilder.Value{
		val("a"),
		val("b\x01c"),
	})
	if key1 == key2 {
		t.Fatal("keys with separator bytes in values must not collide")
	}
}

func TestBuildKeyString_NullVsEmpty(t *testing.T) {
	// NULL and empty string must produce different keys.
	key1 := buildKeyString([]pqbuilder.Value{
		nullVal(),
		val("x"),
	})
	key2 := buildKeyString([]pqbuilder.Value{
		val(""),
		val("x"),
	})
	if key1 == key2 {
		t.Fatal("NULL and empty string must produce different keys")
	}
}

func TestBuildKeyString_NullSentinelInValue(t *testing.T) {
	// Value containing \x00 must not collide with NULL.
	key1 := buildKeyString([]pqbuilder.Value{
		val("\x00"),
	})
	key2 := buildKeyString([]pqbuilder.Value{
		nullVal(),
	})
	if key1 == key2 {
		t.Fatal("value containing \\x00 must not collide with NULL")
	}
}

func TestDedupRows_CompositeKeyWithNulls(t *testing.T) {
	// Composite key where one component is NULL. NULL keys must be handled
	// correctly (NULL != NULL in SQL, but for dedup purposes we treat them
	// as equal since they represent the same row).
	rows := [][]pqbuilder.Value{
		{val("a"), nullVal(), val("v1")},
		{val("a"), val("1"), val("v2")},
		{val("a"), nullVal(), val("v3")}, // supersedes first (same composite key)
	}
	keyColumns := []int{0, 1}

	result := dedupRows(rows, keyColumns, nil)

	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	// Verify the NULL-keyed row was deduped to v3.
	for _, r := range result {
		if r[1].IsNull && string(r[2].Data) != "v3" {
			t.Errorf("NULL-keyed row should be v3, got %q", string(r[2].Data))
		}
	}
}

func TestDedupRows_LargeKeyValues(t *testing.T) {
	// Key values exceeding 1KB. Ensures buildKeyString handles large values.
	largeKey1 := make([]byte, 2000)
	for i := range largeKey1 {
		largeKey1[i] = 'A'
	}
	largeKey2 := make([]byte, 2000)
	for i := range largeKey2 {
		largeKey2[i] = 'B'
	}

	rows := [][]pqbuilder.Value{
		{pqbuilder.Value{Data: largeKey1}, val("v1")},
		{pqbuilder.Value{Data: largeKey2}, val("v2")},
		{pqbuilder.Value{Data: largeKey1}, val("v3")}, // supersedes first
	}
	keyColumns := []int{0}

	result := dedupRows(rows, keyColumns, nil)

	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}

	// Find the row with largeKey1 and verify it's v3.
	for _, r := range result {
		if len(r[0].Data) == 2000 && r[0].Data[0] == 'A' {
			if string(r[1].Data) != "v3" {
				t.Errorf("large key A row should be v3, got %q", string(r[1].Data))
			}
		}
	}
}

func TestFilterDeletedRows(t *testing.T) {
	// Verify existing function still works correctly.
	existing := [][]pqbuilder.Value{
		row("1", "old"),
		row("2", "keep"),
		row("3", "old"),
	}
	deletes := [][]pqbuilder.Value{
		{val("1")},
		{val("3")},
	}
	keyColumns := []int{0}

	result := filterDeletedRows(existing, deletes, keyColumns)

	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if string(result[0][0].Data) != "2" {
		t.Errorf("expected key '2', got %q", string(result[0][0].Data))
	}
}
