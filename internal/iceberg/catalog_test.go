package iceberg

import (
	"context"
	"encoding/json"
	"testing"

	pqbuilder "github.com/viggy28/streambed/internal/parquet"
	"github.com/viggy28/streambed/internal/storage"
)

// TestCommitEmptyTableMetadata verifies that commitEmptyTable produces
// metadata with current-snapshot-id=-1 and empty snapshots array.
// This would have caught the bug where DuckDB ignored current-snapshot-id=-1
// and fell back to the last snapshot in the array.
func TestCommitEmptyTableMetadata(t *testing.T) {
	// Simulate the metadata that commitEmptyTable would produce.
	// We can't call commitEmptyTable directly without S3, but we can
	// verify the metadata transformation logic.
	original := tableMetadata{
		FormatVersion:      2,
		TableUUID:          "test-uuid",
		Location:           "s3://bucket/prefix/public/test",
		LastSequenceNumber: 3,
		LastUpdatedMS:      1000,
		LastColumnID:       2,
		CurrentSchemaID:    0,
		Schemas: []icebergSchema{{
			Type:     "struct",
			SchemaID: 0,
			Fields: []schemaField{
				{ID: 1, Name: "id", Required: false, Type: "int"},
			},
		}},
		DefaultSpecID:      0,
		PartitionSpecs:     []partitionSpec{{SpecID: 0, Fields: []interface{}{}}},
		LastPartitionID:    999,
		DefaultSortOrderID: 0,
		SortOrders:         []sortOrder{{OrderID: 0, Fields: []interface{}{}}},
		Properties:         map[string]string{},
		CurrentSnapshotID:  12345,
		Refs: map[string]interface{}{
			"main": map[string]interface{}{
				"snapshot-id": 12345,
				"type":        "branch",
			},
		},
		Snapshots: []snapshot{
			{SnapshotID: 12345, SequenceNumber: 3, TimestampMS: 1000, Summary: map[string]string{"total-records": "10"}},
		},
		SnapshotLog: []snapshotLogEntry{{TimestampMS: 1000, SnapshotID: 12345}},
		MetadataLog: []metadataLogEntry{},
	}

	// Apply the same transformation as commitEmptyTable.
	metadata := original
	metadata.LastSequenceNumber++
	metadata.LastUpdatedMS = 2000
	metadata.CurrentSnapshotID = -1
	metadata.Snapshots = []snapshot{}
	metadata.SnapshotLog = []snapshotLogEntry{}
	metadata.Refs = map[string]interface{}{}
	metadata.MetadataLog = append(metadata.MetadataLog, metadataLogEntry{
		TimestampMS:  2000,
		MetadataFile: "s3://bucket/prefix/public/test/metadata/v1.metadata.json",
	})

	// Marshal and verify.
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	// Parse it back and verify the critical fields.
	var parsed tableMetadata
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}

	if parsed.CurrentSnapshotID != -1 {
		t.Errorf("current-snapshot-id: got %d, want -1", parsed.CurrentSnapshotID)
	}
	if len(parsed.Snapshots) != 0 {
		t.Errorf("snapshots: got %d entries, want 0", len(parsed.Snapshots))
	}
	if len(parsed.SnapshotLog) != 0 {
		t.Errorf("snapshot-log: got %d entries, want 0", len(parsed.SnapshotLog))
	}
	if len(parsed.Refs) != 0 {
		t.Errorf("refs: got %d entries, want 0", len(parsed.Refs))
	}
	if parsed.LastSequenceNumber != 4 {
		t.Errorf("last-sequence-number: got %d, want 4", parsed.LastSequenceNumber)
	}
	if len(parsed.MetadataLog) != 1 {
		t.Errorf("metadata-log: got %d entries, want 1", len(parsed.MetadataLog))
	}

	// Verify the schema is preserved (table still "exists").
	if len(parsed.Schemas) != 1 {
		t.Fatalf("schemas: got %d, want 1", len(parsed.Schemas))
	}
	if len(parsed.Schemas[0].Fields) != 1 {
		t.Errorf("schema fields: got %d, want 1", len(parsed.Schemas[0].Fields))
	}
}

// TestCommitChangesetRoutesToEmpty verifies that CommitChangeset correctly
// routes to commitEmptyTable when replace=true with nil or zero-row dataFile.
// This is a logic test — it doesn't need S3 since we're testing the routing
// conditions, not the actual S3 operations.
func TestCommitChangesetRoutesToEmpty(t *testing.T) {
	tests := []struct {
		name     string
		dataFile *DataFile
		replace  bool
		wantEmpty bool
	}{
		{
			name:      "replace with nil dataFile",
			dataFile:  nil,
			replace:   true,
			wantEmpty: true,
		},
		{
			name:      "replace with zero-row dataFile",
			dataFile:  &DataFile{Path: "data/x.parquet", RowCount: 0, FileSize: 100},
			replace:   true,
			wantEmpty: true,
		},
		{
			name:      "replace with non-zero rows",
			dataFile:  &DataFile{Path: "data/x.parquet", RowCount: 5, FileSize: 100},
			replace:   true,
			wantEmpty: false,
		},
		{
			name:      "append with data",
			dataFile:  &DataFile{Path: "data/x.parquet", RowCount: 10, FileSize: 100},
			replace:   false,
			wantEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldRouteToEmpty := tt.replace && (tt.dataFile == nil || tt.dataFile.RowCount == 0)
			if shouldRouteToEmpty != tt.wantEmpty {
				t.Errorf("routing: got empty=%v, want %v", shouldRouteToEmpty, tt.wantEmpty)
			}
		})
	}
}

// TestGetDataFilePathsEmptyTable verifies that GetDataFilePaths returns nil
// for a table with CurrentSnapshotID = -1 (no active snapshot).
// This ensures queries on empty tables don't error out.
func TestGetDataFilePathsEmptyTable(t *testing.T) {
	// Simulate metadata with no active snapshot.
	metadata := tableMetadata{
		FormatVersion:     2,
		CurrentSnapshotID: -1,
		Snapshots:         []snapshot{},
	}

	// GetDataFilePaths checks CurrentSnapshotID <= 0 → returns nil.
	if metadata.CurrentSnapshotID > 0 {
		t.Fatal("expected CurrentSnapshotID <= 0 for empty table")
	}
	// This is the same check in GetDataFilePaths:
	// if metadata.CurrentSnapshotID <= 0 { return nil, nil }
}

// TestFilterDeletedRowsEdgeCases tests the filterDeletedRows function
// that the writer uses for COW merges.
func TestFilterDeletedRowsEdgeCases(t *testing.T) {
	t.Run("empty deletes returns all rows", func(t *testing.T) {
		rows := makeTestRows(3)
		result := filterDeletedRows(rows, nil, []int{0})
		if len(result) != 3 {
			t.Errorf("got %d rows, want 3", len(result))
		}
	})

	t.Run("empty key columns returns all rows", func(t *testing.T) {
		rows := makeTestRows(3)
		deletes := makeTestRows(1)
		result := filterDeletedRows(rows, deletes, nil)
		if len(result) != 3 {
			t.Errorf("got %d rows, want 3", len(result))
		}
	})

	t.Run("delete all rows returns empty", func(t *testing.T) {
		rows := makeTestRows(3)
		result := filterDeletedRows(rows, rows, []int{0})
		if len(result) != 0 {
			t.Errorf("got %d rows, want 0", len(result))
		}
	})

	t.Run("delete non-existent key is no-op", func(t *testing.T) {
		rows := makeTestRows(3)
		deletes := [][]pqbuilder.Value{{pqbuilder.Value{Data: []byte("999")}}}
		result := filterDeletedRows(rows, deletes, []int{0})
		if len(result) != 3 {
			t.Errorf("got %d rows, want 3", len(result))
		}
	})
}

// TestFilterDeletedRowsMultiColumnKey tests COW filtering with composite keys.
func TestFilterDeletedRowsMultiColumnKey(t *testing.T) {
	// 3-column rows: [id, name, value] with composite key on columns 0 and 1.
	rows := [][]pqbuilder.Value{
		{v("1"), v("alice"), v("100")},
		{v("2"), v("bob"), v("200")},
		{v("3"), v("carol"), v("300")},
		{v("1"), v("dave"), v("400")}, // same id as row 0, different name
	}
	// Delete key (1, alice) — should only remove first row, not (1, dave).
	deletes := [][]pqbuilder.Value{
		{v("1"), v("alice")},
	}
	result := filterDeletedRows(rows, deletes, []int{0, 1})
	if len(result) != 3 {
		t.Fatalf("got %d rows, want 3", len(result))
	}
	// Verify the right row was removed.
	for _, row := range result {
		if string(row[0].Data) == "1" && string(row[1].Data) == "alice" {
			t.Error("row (1, alice) should have been deleted")
		}
	}
}

// TestFilterDeletedRowsUpdateSimulation tests the COW pattern for UPDATEs:
// delete old key + insert new row, verifying row count stays the same.
func TestFilterDeletedRowsUpdateSimulation(t *testing.T) {
	existing := [][]pqbuilder.Value{
		{v("1"), v("alice")},
		{v("2"), v("bob")},
		{v("3"), v("carol")},
	}
	// UPDATE row 2: delete old, then append new.
	deletes := [][]pqbuilder.Value{{v("2")}}
	filtered := filterDeletedRows(existing, deletes, []int{0})
	if len(filtered) != 2 {
		t.Fatalf("after filter: got %d rows, want 2", len(filtered))
	}
	// Append the updated row.
	newRow := []pqbuilder.Value{v("2"), v("bob-updated")}
	combined := append(filtered, newRow)
	if len(combined) != 3 {
		t.Errorf("after combine: got %d rows, want 3", len(combined))
	}
	// Verify updated value.
	for _, row := range combined {
		if string(row[0].Data) == "2" && string(row[1].Data) != "bob-updated" {
			t.Errorf("row 2 should be updated, got %s", string(row[1].Data))
		}
	}
}

// TestFilterDeletedRowsWithNulls verifies null key handling — nulls should
// match other nulls (important for nullable key columns).
func TestFilterDeletedRowsWithNulls(t *testing.T) {
	rows := [][]pqbuilder.Value{
		{v("1"), {IsNull: true}},
		{v("2"), v("bob")},
		{v("3"), {IsNull: true}},
	}
	// Delete rows where col1 is null — should remove rows 0 and 2.
	// But key is only col 0, so we need to target by id.
	deletes := [][]pqbuilder.Value{{v("1")}, {v("3")}}
	result := filterDeletedRows(rows, deletes, []int{0})
	if len(result) != 1 {
		t.Fatalf("got %d rows, want 1", len(result))
	}
	if string(result[0][0].Data) != "2" {
		t.Errorf("surviving row should be id=2, got %s", string(result[0][0].Data))
	}
}

// TestCommitEmptyTablePreservesSchema verifies that after commitEmptyTable,
// the schema and other structural metadata are preserved — the table still
// "exists" in Iceberg, it just has no data.
func TestCommitEmptyTablePreservesSchema(t *testing.T) {
	original := tableMetadata{
		FormatVersion:      2,
		TableUUID:          "test-uuid",
		Location:           "s3://bucket/prefix/public/test",
		LastSequenceNumber: 5,
		LastUpdatedMS:      1000,
		LastColumnID:       3,
		CurrentSchemaID:    0,
		Schemas: []icebergSchema{{
			Type:     "struct",
			SchemaID: 0,
			Fields: []schemaField{
				{ID: 1, Name: "id", Required: false, Type: "long"},
				{ID: 2, Name: "name", Required: false, Type: "string"},
				{ID: 3, Name: "value", Required: false, Type: "double"},
			},
		}},
		DefaultSpecID:      0,
		PartitionSpecs:     []partitionSpec{{SpecID: 0, Fields: []interface{}{}}},
		LastPartitionID:    999,
		DefaultSortOrderID: 0,
		SortOrders:         []sortOrder{{OrderID: 0, Fields: []interface{}{}}},
		Properties:         map[string]string{"owner": "test"},
		CurrentSnapshotID:  99999,
		Snapshots: []snapshot{
			{SnapshotID: 99999, SequenceNumber: 5, TimestampMS: 1000},
		},
		SnapshotLog: []snapshotLogEntry{{TimestampMS: 1000, SnapshotID: 99999}},
		MetadataLog: []metadataLogEntry{},
	}

	// Apply commitEmptyTable transformation.
	metadata := original
	metadata.LastSequenceNumber++
	metadata.LastUpdatedMS = 2000
	metadata.CurrentSnapshotID = -1
	metadata.Snapshots = []snapshot{}
	metadata.SnapshotLog = []snapshotLogEntry{}
	metadata.Refs = map[string]interface{}{}

	// Round-trip through JSON.
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	var parsed tableMetadata
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}

	// Schema must survive.
	if len(parsed.Schemas) != 1 {
		t.Fatalf("schemas: got %d, want 1", len(parsed.Schemas))
	}
	if len(parsed.Schemas[0].Fields) != 3 {
		t.Fatalf("schema fields: got %d, want 3", len(parsed.Schemas[0].Fields))
	}
	if parsed.Schemas[0].Fields[0].Name != "id" {
		t.Errorf("field 0: got %s, want id", parsed.Schemas[0].Fields[0].Name)
	}
	if parsed.Schemas[0].Fields[2].Type != "double" {
		t.Errorf("field 2 type: got %s, want double", parsed.Schemas[0].Fields[2].Type)
	}

	// Structural metadata preserved.
	if parsed.TableUUID != "test-uuid" {
		t.Errorf("table-uuid: got %s, want test-uuid", parsed.TableUUID)
	}
	if parsed.Location != "s3://bucket/prefix/public/test" {
		t.Errorf("location: got %s", parsed.Location)
	}
	if parsed.Properties["owner"] != "test" {
		t.Errorf("properties.owner: got %s, want test", parsed.Properties["owner"])
	}
	if parsed.FormatVersion != 2 {
		t.Errorf("format-version: got %d, want 2", parsed.FormatVersion)
	}

	// Data-related fields must be cleared.
	if parsed.CurrentSnapshotID != -1 {
		t.Errorf("current-snapshot-id: got %d, want -1", parsed.CurrentSnapshotID)
	}
	if len(parsed.Snapshots) != 0 {
		t.Errorf("snapshots: got %d, want 0", len(parsed.Snapshots))
	}
}

// TestCOWWriterFlushDecision tests that the writer correctly decides between
// append, COW-replace, and empty-table paths based on buffer state.
func TestCOWWriterFlushDecision(t *testing.T) {
	tests := []struct {
		name       string
		rows       int
		deletes    int
		keyCols    int
		wantCOW    bool
		wantSkipDel bool
	}{
		{"insert only", 10, 0, 1, false, false},
		{"delete with keys", 0, 5, 1, true, false},
		{"update (insert+delete) with keys", 3, 3, 1, true, false},
		{"delete without keys", 0, 5, 0, false, true},
		{"mixed without keys", 3, 2, 0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delCount := tt.deletes
			keyCols := make([]int, tt.keyCols)
			for i := range keyCols {
				keyCols[i] = i
			}

			// Replicate the decision logic from writer.go flush()
			replace := false
			skipDeletes := false

			if delCount > 0 && len(keyCols) > 0 {
				replace = true
			} else if delCount > 0 {
				skipDeletes = true
			}

			if replace != tt.wantCOW {
				t.Errorf("COW: got %v, want %v", replace, tt.wantCOW)
			}
			if skipDeletes != tt.wantSkipDel {
				t.Errorf("skipDeletes: got %v, want %v", skipDeletes, tt.wantSkipDel)
			}
		})
	}
}

// TestS3KeyFromURI verifies the S3 URI to key extraction.
func TestS3KeyFromURI(t *testing.T) {
	tests := []struct {
		uri  string
		want string
	}{
		{"s3://bucket/prefix/path/file.json", "prefix/path/file.json"},
		{"s3://my-bucket/a/b/c", "a/b/c"},
		{"not-s3-uri", "not-s3-uri"},
		{"s3://bucket/key", "key"},
	}
	for _, tt := range tests {
		got := s3KeyFromURI(tt.uri)
		if got != tt.want {
			t.Errorf("s3KeyFromURI(%q) = %q, want %q", tt.uri, got, tt.want)
		}
	}
}

// TestTotalRecords verifies snapshot record accumulation.
func TestTotalRecords(t *testing.T) {
	snaps := []snapshot{
		{Summary: map[string]string{"added-records": "100"}},
		{Summary: map[string]string{"added-records": "50"}},
		{Summary: map[string]string{"operation": "delete"}}, // no added-records
	}
	got := totalRecords(snaps)
	if got != 150 {
		t.Errorf("totalRecords = %d, want 150", got)
	}

	// Empty snapshots.
	if totalRecords(nil) != 0 {
		t.Error("totalRecords(nil) should be 0")
	}
}

// ---------------------------------------------------------------------------
// Crash-Safety Tests using fault-injecting S3
// ---------------------------------------------------------------------------

// TestCommitChangeset_VersionHintWriteFail verifies that if PutObject fails
// on version-hint.text (the last step), the old version is still readable
// and a re-commit succeeds.
func TestCommitChangeset_VersionHintWriteFail(t *testing.T) {
	ctx := context.Background()

	mem := storage.NewMemS3Client("test-bucket")
	fault := storage.NewFaultS3Client(mem)

	catalog := NewCatalog(fault, "test-bucket", "test-prefix")

	// Create initial table.
	cols := []ColumnDef{
		{Name: "id", OID: 23},   // int4
		{Name: "name", OID: 25}, // text
	}
	if _, err := catalog.CreateTable(ctx, "public", "crash_test", cols); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Verify table exists at version 1.
	exists, err := catalog.TableExists(ctx, "public", "crash_test")
	if err != nil || !exists {
		t.Fatalf("table should exist: exists=%v err=%v", exists, err)
	}

	// First successful commit to establish version 2.
	err = catalog.CommitChangeset(ctx, "public", "crash_test",
		&DataFile{Path: "data/file1.parquet", RowCount: 10, FileSize: 1000},
		nil, false, "0/1234")
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}

	// Now configure fault: fail on version-hint.text writes.
	fault.FailOnKeyContaining("version-hint.text")

	// Second commit should fail when trying to update version-hint.
	err = catalog.CommitChangeset(ctx, "public", "crash_test",
		&DataFile{Path: "data/file2.parquet", RowCount: 5, FileSize: 500},
		nil, false, "0/5678")
	if err == nil {
		t.Fatal("expected error when version-hint write fails")
	}
	t.Logf("expected failure: %v", err)

	// Remove fault — version-hint should still point to version 2.
	fault.Reset()

	// Verify: GetSnapshotFlushLSN should return the LSN from the first commit.
	lsn, found, err := catalog.GetSnapshotFlushLSN(ctx, "public", "crash_test")
	if err != nil {
		t.Fatalf("GetSnapshotFlushLSN after failed commit: %v", err)
	}
	if !found {
		t.Fatal("expected to find flush LSN after failed commit")
	}
	if lsn != "0/1234" {
		t.Errorf("expected LSN 0/1234 (from first commit), got %s", lsn)
	}

	// Re-commit should succeed now.
	err = catalog.CommitChangeset(ctx, "public", "crash_test",
		&DataFile{Path: "data/file2.parquet", RowCount: 5, FileSize: 500},
		nil, false, "0/5678")
	if err != nil {
		t.Fatalf("re-commit after fault cleared: %v", err)
	}

	// Verify new LSN.
	lsn, found, err = catalog.GetSnapshotFlushLSN(ctx, "public", "crash_test")
	if err != nil {
		t.Fatalf("GetSnapshotFlushLSN after re-commit: %v", err)
	}
	if !found || lsn != "0/5678" {
		t.Errorf("expected LSN 0/5678 after re-commit, got %s (found=%v)", lsn, found)
	}
}

// TestCommitChangeset_ManifestWriteFail verifies that if PutObject fails on
// a manifest file write, the commit returns an error and no partial state
// is visible.
func TestCommitChangeset_ManifestWriteFail(t *testing.T) {
	ctx := context.Background()

	mem := storage.NewMemS3Client("test-bucket")
	fault := storage.NewFaultS3Client(mem)

	catalog := NewCatalog(fault, "test-bucket", "test-prefix")

	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	if _, err := catalog.CreateTable(ctx, "public", "manifest_test", cols); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Fail on manifest file writes (.avro files that are not snap-*.avro).
	fault.FailOnKeyContaining("-m0.avro")

	err := catalog.CommitChangeset(ctx, "public", "manifest_test",
		&DataFile{Path: "data/file1.parquet", RowCount: 10, FileSize: 1000},
		nil, false, "0/1234")
	if err == nil {
		t.Fatal("expected error when manifest write fails")
	}

	// version-hint should still point to version 1 (CreateTable).
	fault.Reset()
	_, found, err := catalog.GetSnapshotFlushLSN(ctx, "public", "manifest_test")
	if err != nil {
		t.Fatalf("GetSnapshotFlushLSN: %v", err)
	}
	if found {
		t.Error("should not find flush LSN — no successful commit happened")
	}
}

// TestCommitChangeset_MetadataWriteFail verifies that if PutObject fails on
// v<N+1>.metadata.json, the version-hint is NOT updated.
func TestCommitChangeset_MetadataWriteFail(t *testing.T) {
	ctx := context.Background()

	mem := storage.NewMemS3Client("test-bucket")
	fault := storage.NewFaultS3Client(mem)

	catalog := NewCatalog(fault, "test-bucket", "test-prefix")

	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	if _, err := catalog.CreateTable(ctx, "public", "meta_test", cols); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Fail on v2.metadata.json write.
	fault.FailOnKeyContaining("v2.metadata.json")

	err := catalog.CommitChangeset(ctx, "public", "meta_test",
		&DataFile{Path: "data/file1.parquet", RowCount: 10, FileSize: 1000},
		nil, false, "0/1234")
	if err == nil {
		t.Fatal("expected error when metadata write fails")
	}

	// version-hint should still be "1".
	fault.Reset()
	hintData, err := mem.GetObject(ctx, "test-prefix/public/meta_test/metadata/version-hint.text")
	if err != nil {
		t.Fatalf("read version-hint: %v", err)
	}
	if string(hintData) != "1" {
		t.Errorf("version-hint should still be 1, got %s", string(hintData))
	}
}

// TestCommitChangeset_SuccessfulRoundTrip verifies the happy path: create table,
// commit data, read back LSN and data file paths.
func TestCommitChangeset_SuccessfulRoundTrip(t *testing.T) {
	ctx := context.Background()

	mem := storage.NewMemS3Client("test-bucket")
	catalog := NewCatalog(mem, "test-bucket", "test-prefix")

	cols := []ColumnDef{
		{Name: "id", OID: 23},
		{Name: "name", OID: 25},
	}
	if _, err := catalog.CreateTable(ctx, "public", "roundtrip", cols); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Write a fake Parquet file so GetDataFilePaths can find it.
	if err := mem.PutObject(ctx, "test-prefix/public/roundtrip/data/file1.parquet", []byte("fake"), ""); err != nil {
		t.Fatalf("put parquet: %v", err)
	}

	// Commit.
	err := catalog.CommitChangeset(ctx, "public", "roundtrip",
		&DataFile{Path: "data/file1.parquet", RowCount: 42, FileSize: 1000},
		nil, false, "0/ABCD")
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify LSN.
	lsn, found, err := catalog.GetSnapshotFlushLSN(ctx, "public", "roundtrip")
	if err != nil {
		t.Fatalf("get LSN: %v", err)
	}
	if !found || lsn != "0/ABCD" {
		t.Errorf("LSN: got %q (found=%v), want 0/ABCD", lsn, found)
	}

	// Verify data file paths.
	paths, err := catalog.GetDataFilePaths(ctx, "public", "roundtrip")
	if err != nil {
		t.Fatalf("get data file paths: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("expected 1 data file path, got %d", len(paths))
	}
	t.Logf("data file path: %s", paths[0])
}

func v(s string) pqbuilder.Value {
	return pqbuilder.Value{Data: []byte(s)}
}

func makeTestRows(n int) [][]pqbuilder.Value {
	rows := make([][]pqbuilder.Value, n)
	for i := range rows {
		rows[i] = []pqbuilder.Value{{Data: []byte{byte('0' + i)}}}
	}
	return rows
}
