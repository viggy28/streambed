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
