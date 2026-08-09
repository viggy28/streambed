package iceberg

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	pqbuilder "github.com/viggy28/streambed/internal/parquet"
	"github.com/viggy28/streambed/internal/storage"
)

func TestTargetSizeMultiFileOutput(t *testing.T) {
	w, _ := testWriter(t)
	w.targetFileSizeBytes = 700
	ctx := context.Background()
	for i := 1; i <= 80; i++ {
		if _, err := w.HandleEvent(ctx, insertEvent("public", "mf", pglogrepl.LSN(i), strconv.Itoa(i), fmt.Sprintf("name-%04d-xxxxxxxxxxxxxxxxxxxxxxxx", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	files, err := w.catalog.GetActiveDataFiles(ctx, "public", "mf")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 2 {
		t.Fatalf("active data files=%d, want multi-file output", len(files))
	}
	var rows int64
	for _, f := range files {
		rows += f.RowCount
		if len(f.LowerBounds) == 0 || len(f.UpperBounds) == 0 {
			t.Fatalf("file %s missing key bounds", f.Path)
		}
	}
	if rows != 80 {
		t.Fatalf("rows=%d, want 80", rows)
	}
}

func TestCOWFileLevelRewritePrunesUnaffectedFiles(t *testing.T) {
	ctx := context.Background()
	mem := storage.NewMemS3Client("test-bucket")
	counting := &countingStorage{inner: mem}
	catalog := NewCatalog(counting, "test-bucket", "test-prefix")
	store := testStateStore(t)
	w := NewWriter(catalog, counting, store, "test_slot", 10000, 2*time.Second, testLogger(), WithTargetFileSizeBytes(700))

	for i := 1; i <= 80; i++ {
		if _, err := w.HandleEvent(ctx, insertEvent("public", "acct", pglogrepl.LSN(i), strconv.Itoa(i), fmt.Sprintf("name-%04d-xxxxxxxxxxxxxxxxxxxxxxxx", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	filesBefore, err := catalog.GetActiveDataFiles(ctx, "public", "acct")
	if err != nil {
		t.Fatal(err)
	}
	if len(filesBefore) < 2 {
		t.Fatalf("setup active files=%d, want >=2", len(filesBefore))
	}
	counting.parquetGets = 0
	counting.parquetGetBytes = 0
	if _, err := w.HandleEvent(ctx, updateEvent("public", "acct", 1000, "2", "2", "updated")); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if counting.parquetGets >= len(filesBefore) {
		t.Fatalf("COW read %d parquet files, want fewer than all %d", counting.parquetGets, len(filesBefore))
	}
	paths, err := catalog.GetDataFilePaths(ctx, "public", "acct")
	if err != nil {
		t.Fatal(err)
	}
	cols := []pqbuilder.ColumnDef{{Name: "id", OID: 23, FieldID: 1}, {Name: "name", OID: 25, FieldID: 2}}
	var total int
	seenUpdated := false
	for _, p := range paths {
		data, err := mem.GetObject(ctx, s3KeyFromURI(p))
		if err != nil {
			t.Fatal(err)
		}
		rows, err := pqbuilder.ReadRows(data, cols)
		if err != nil {
			t.Fatal(err)
		}
		total += len(rows)
		for _, row := range rows {
			if string(row[0].Data) == "2" && string(row[1].Data) == "updated" {
				seenUpdated = true
			}
		}
	}
	if total != 80 || !seenUpdated {
		t.Fatalf("total=%d updated=%v", total, seenUpdated)
	}
}

func TestSmallFileCompactionReducesActiveFiles(t *testing.T) {
	w, mem := testWriter(t)
	ctx := context.Background()
	for i := 1; i <= 24; i++ {
		if _, err := w.HandleEvent(ctx, insertEvent("public", "compact", pglogrepl.LSN(i), strconv.Itoa(i), fmt.Sprintf("name-%04d-xxxxxxxxxxxxxxxxxxxxxxxx", i))); err != nil {
			t.Fatal(err)
		}
		if err := w.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	before, err := w.catalog.GetActiveDataFiles(ctx, "public", "compact")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) < 10 {
		t.Fatalf("setup active files=%d, want many small files", len(before))
	}
	plan, result, err := CompactSmallFiles(ctx, w.catalog, w.state, CompactionOptions{
		TargetFileSizeBytes:     700,
		SmallFileThresholdBytes: 10 * 1024,
		MaxInputFiles:           100,
		DryRun:                  false,
	}, "public", "compact")
	if err != nil {
		t.Fatal(err)
	}
	if result.Aborted {
		t.Fatalf("compaction aborted: %s", result.AbortReason)
	}
	if len(plan.InputFiles) != len(before) {
		t.Fatalf("input files=%d, before=%d", len(plan.InputFiles), len(before))
	}
	after, err := w.catalog.GetActiveDataFiles(ctx, "public", "compact")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) >= len(before) {
		t.Fatalf("active files not reduced: before=%d after=%d", len(before), len(after))
	}
	cols := []pqbuilder.ColumnDef{{Name: "id", OID: 23, FieldID: 1}, {Name: "name", OID: 25, FieldID: 2}}
	var total int
	for _, f := range after {
		data, err := mem.GetObject(ctx, s3KeyFromURI(w.catalog.dataFileURI(w.catalog.tablePath("public", "compact"), f)))
		if err != nil {
			t.Fatal(err)
		}
		rows, err := pqbuilder.ReadRows(data, cols)
		if err != nil {
			t.Fatal(err)
		}
		total += len(rows)
		if len(f.LowerBounds) == 0 || len(f.UpperBounds) == 0 {
			t.Fatalf("compacted file %s missing bounds", f.Path)
		}
	}
	if total != 24 {
		t.Fatalf("rows after compaction=%d, want 24", total)
	}
	lsn, found, err := w.catalog.GetSnapshotFlushLSN(ctx, "public", "compact")
	if err != nil {
		t.Fatal(err)
	}
	if !found || lsn == "" {
		t.Fatalf("compaction did not preserve last flush LSN: found=%v lsn=%q", found, lsn)
	}
}

func TestSmallFileCompactionCarriesConcurrentAppend(t *testing.T) {
	w, _ := testWriter(t)
	ctx := context.Background()
	for i := 1; i <= 6; i++ {
		if _, err := w.HandleEvent(ctx, insertEvent("public", "append", pglogrepl.LSN(i), strconv.Itoa(i), "old")); err != nil {
			t.Fatal(err)
		}
		if err := w.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := w.catalog.PlanSmallFileCompaction(ctx, "public", "append", CompactionOptions{TargetFileSizeBytes: 700, SmallFileThresholdBytes: 10 * 1024, MaxInputFiles: 3})
	if err != nil {
		t.Fatal(err)
	}
	outputs, err := w.catalog.writeCompactedDataFiles(ctx, "public", "append", plan, CompactionOptions{TargetFileSizeBytes: 700})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.HandleEvent(ctx, insertEvent("public", "append", 100, "99", "new")); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	lock, err := w.state.AcquireTableCommitLock(ctx, "public", "append", "test", 5*time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	result, err := w.catalog.CommitSmallFileCompaction(ctx, "public", "append", plan, outputs)
	_ = w.state.ReleaseTableCommitLock(ctx, lock)
	if err != nil {
		t.Fatal(err)
	}
	if result.ValidatedSnapshotID == plan.PlannedSnapshotID {
		t.Fatal("validated snapshot did not advance after concurrent append")
	}
	paths, err := w.catalog.GetDataFilePaths(ctx, "public", "append")
	if err != nil {
		t.Fatal(err)
	}
	cols := []pqbuilder.ColumnDef{{Name: "id", OID: 23, FieldID: 1}, {Name: "name", OID: 25, FieldID: 2}}
	seenNew := false
	var total int
	for _, p := range paths {
		data, err := w.storage.GetObject(ctx, s3KeyFromURI(p))
		if err != nil {
			t.Fatal(err)
		}
		rows, err := pqbuilder.ReadRows(data, cols)
		if err != nil {
			t.Fatal(err)
		}
		total += len(rows)
		for _, r := range rows {
			if string(r[0].Data) == "99" {
				seenNew = true
			}
		}
	}
	if total != 7 || !seenNew {
		t.Fatalf("total=%d seenNew=%v", total, seenNew)
	}
}

func TestSmallFileCompactionAbortsWhenInputRewritten(t *testing.T) {
	w, _ := testWriter(t)
	ctx := context.Background()
	for i := 1; i <= 6; i++ {
		if _, err := w.HandleEvent(ctx, insertEvent("public", "conflict", pglogrepl.LSN(i), strconv.Itoa(i), "old")); err != nil {
			t.Fatal(err)
		}
		if err := w.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := w.catalog.PlanSmallFileCompaction(ctx, "public", "conflict", CompactionOptions{TargetFileSizeBytes: 700, SmallFileThresholdBytes: 10 * 1024, MaxInputFiles: 3})
	if err != nil {
		t.Fatal(err)
	}
	outputs, err := w.catalog.writeCompactedDataFiles(ctx, "public", "conflict", plan, CompactionOptions{TargetFileSizeBytes: 700})
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite one planned input file through COW before the compaction commit.
	// Updating all known setup IDs guarantees at least one planned input file is
	// replaced regardless of manifest ordering.
	for i := 1; i <= 6; i++ {
		id := strconv.Itoa(i)
		if _, err := w.HandleEvent(ctx, updateEvent("public", "conflict", pglogrepl.LSN(100+i), id, id, "updated")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	lock, err := w.state.AcquireTableCommitLock(ctx, "public", "conflict", "test", 5*time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	result, err := w.catalog.CommitSmallFileCompaction(ctx, "public", "conflict", plan, outputs)
	_ = w.state.ReleaseTableCommitLock(ctx, lock)
	if err == nil || !result.Aborted {
		t.Fatalf("expected abort, result=%+v err=%v", result, err)
	}
}

func TestSmallFileCompactionSkipsActiveEqualityDeletes(t *testing.T) {
	ctx := context.Background()
	mem := storage.NewMemS3Client("test-bucket")
	catalog := NewCatalog(mem, "test-bucket", "test-prefix")
	store := testStateStore(t)
	w := NewWriter(catalog, mem, store, "test_slot", 10000, 2*time.Second, testLogger(), WithMutationMode(MutationModeMOR))
	if _, err := w.HandleEvent(ctx, insertEvent("public", "morcompact", 1, "1", "old")); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := w.HandleEvent(ctx, updateEvent("public", "morcompact", 2, "1", "1", "new")); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	plan, result, err := CompactSmallFiles(ctx, catalog, store, CompactionOptions{TargetFileSizeBytes: 1024, SmallFileThresholdBytes: 10 * 1024}, "public", "morcompact")
	if err != nil {
		t.Fatal(err)
	}
	if plan.ActiveDeleteFileCount == 0 || !result.Aborted {
		t.Fatalf("expected active deletes skip, plan=%+v result=%+v", plan, result)
	}
}

func TestCompactionSortRowsByFieldsIsTypeAware(t *testing.T) {
	cols := []pqbuilder.ColumnDef{{Name: "id", OID: 23, FieldID: 1}}
	rows := [][]pqbuilder.Value{
		{{Data: []byte("10")}},
		{{Data: []byte("2")}},
		{{Data: []byte("1")}},
	}
	sortRowsByFields(rows, cols, []int{1})
	got := []string{string(rows[0][0].Data), string(rows[1][0].Data), string(rows[2][0].Data)}
	want := []string{"1", "2", "10"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sorted ids=%v, want %v", got, want)
		}
	}
}

func TestSnapshotExpirationAndOrphanDryRun(t *testing.T) {
	w, mem := testWriter(t)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if _, err := w.HandleEvent(ctx, insertEvent("public", "gc", pglogrepl.LSN(i), strconv.Itoa(i), "live")); err != nil {
			t.Fatal(err)
		}
		if err := w.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := w.catalog.ExpireSnapshots(ctx, "public", "gc", 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExpiredSnapshots == 0 || len(plan.DeleteObjects) == 0 {
		t.Fatalf("dry-run plan did not find expired objects: %+v", plan)
	}
	before, _ := mem.ListPrefix(ctx, "test-prefix/public/gc")
	plan, err = w.catalog.ExpireSnapshots(ctx, "public", "gc", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := mem.ListPrefix(ctx, "test-prefix/public/gc")
	if len(after) >= len(before) {
		t.Fatalf("expiration did not delete objects: before=%d after=%d plan=%+v", len(before), len(after), plan)
	}
	paths, err := w.catalog.GetDataFilePaths(ctx, "public", "gc")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("current snapshot data file was deleted")
	}
}
