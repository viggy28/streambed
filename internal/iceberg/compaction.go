package iceberg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"sort"
	"strings"
	"time"

	ice "github.com/apache/iceberg-go"
	"github.com/google/uuid"
	pqbuilder "github.com/viggy28/streambed/internal/parquet"
	"github.com/viggy28/streambed/internal/state"
)

// CompactionOptions configures small-file compaction for one table.
type CompactionOptions struct {
	TargetFileSizeBytes     int64
	SmallFileThresholdBytes int64
	MaxInputFiles           int
	DryRun                  bool
}

// CompactionPlan describes a planned small-file compaction. PlannedSnapshotID
// is the snapshot used to choose input files and perform the rewrite.
type CompactionPlan struct {
	Schema                string
	Table                 string
	PlannedSnapshotID     int64
	PlannedSchemaID       int
	Columns               []pqbuilder.ColumnDef
	InputFiles            []DataFile
	InputBytes            int64
	EstimatedOutputFiles  int
	ActiveDeleteFileCount int
	DryRun                bool
}

// CompactionResult describes the outcome of a small-file compaction.
type CompactionResult struct {
	PlannedSnapshotID   int64
	ValidatedSnapshotID int64
	NewSnapshotID       int64
	InputFiles          int
	OutputFiles         int
	InputBytes          int64
	OutputBytes         int64
	CarriedFiles        int
	Aborted             bool
	AbortReason         string
}

// CompactSmallFiles plans and optionally applies clean data-file compaction.
// It skips tables with active equality-delete files; MOR delete compaction is
// handled separately.
func CompactSmallFiles(ctx context.Context, catalog *Catalog, store *state.Store, opts CompactionOptions, schema, table string) (CompactionPlan, CompactionResult, error) {
	plan, err := catalog.PlanSmallFileCompaction(ctx, schema, table, opts)
	if err != nil || opts.DryRun || len(plan.InputFiles) == 0 || plan.ActiveDeleteFileCount > 0 {
		return plan, CompactionResult{PlannedSnapshotID: plan.PlannedSnapshotID, InputFiles: len(plan.InputFiles), InputBytes: plan.InputBytes, Aborted: plan.ActiveDeleteFileCount > 0, AbortReason: abortReason(plan)}, err
	}
	outputs, err := catalog.writeCompactedDataFiles(ctx, schema, table, plan, opts)
	if err != nil {
		return plan, CompactionResult{PlannedSnapshotID: plan.PlannedSnapshotID, InputFiles: len(plan.InputFiles), InputBytes: plan.InputBytes, Aborted: true, AbortReason: err.Error()}, err
	}
	lock, err := store.AcquireTableCommitLock(ctx, schema, table, "maintenance-compact", 5*time.Minute, 30*time.Second)
	if err != nil {
		_ = catalog.deleteDataFiles(ctx, schema, table, outputs)
		return plan, CompactionResult{PlannedSnapshotID: plan.PlannedSnapshotID, InputFiles: len(plan.InputFiles), OutputFiles: len(outputs), InputBytes: plan.InputBytes, Aborted: true, AbortReason: err.Error()}, err
	}
	defer store.ReleaseTableCommitLock(context.Background(), lock)
	ok, err := store.RefreshTableCommitLock(ctx, lock, 5*time.Minute)
	if err != nil {
		return plan, CompactionResult{PlannedSnapshotID: plan.PlannedSnapshotID, InputFiles: len(plan.InputFiles), OutputFiles: len(outputs), InputBytes: plan.InputBytes, Aborted: true, AbortReason: err.Error()}, err
	}
	if !ok {
		err := errors.New("commit lock expired or was stolen")
		return plan, CompactionResult{PlannedSnapshotID: plan.PlannedSnapshotID, InputFiles: len(plan.InputFiles), OutputFiles: len(outputs), InputBytes: plan.InputBytes, Aborted: true, AbortReason: err.Error()}, err
	}
	result, err := catalog.CommitSmallFileCompaction(ctx, schema, table, plan, outputs)
	// Do not delete outputs after a commit attempt returns an error: the publish
	// outcome can be unknown if the final metadata pointer write timed out after
	// succeeding. Definite pre-commit aborts are left for safe orphan cleanup.
	return plan, result, err
}

func abortReason(plan CompactionPlan) string {
	if plan.ActiveDeleteFileCount > 0 {
		return "active equality delete files found; MOR compaction required"
	}
	return ""
}

func (c *Catalog) PlanSmallFileCompaction(ctx context.Context, schema, table string, opts CompactionOptions) (CompactionPlan, error) {
	meta, _, _, err := c.readCurrentMetadata(ctx, schema, table)
	if err != nil {
		return CompactionPlan{}, err
	}
	cols, err := parquetColumnsFromMetadata(meta)
	if err != nil {
		return CompactionPlan{}, err
	}
	plan := CompactionPlan{Schema: schema, Table: table, PlannedSnapshotID: meta.CurrentSnapshotID, PlannedSchemaID: meta.CurrentSchemaID, Columns: cols, DryRun: opts.DryRun}
	if meta.CurrentSnapshotID <= 0 {
		return plan, nil
	}
	deleteCount, err := c.activeDeleteFileCount(ctx, meta)
	if err != nil {
		return plan, err
	}
	plan.ActiveDeleteFileCount = deleteCount
	if deleteCount > 0 {
		return plan, nil
	}
	files, err := c.activeDataFilesFromMetadata(ctx, meta)
	if err != nil {
		return plan, err
	}
	threshold := opts.SmallFileThresholdBytes
	if threshold <= 0 {
		threshold = opts.TargetFileSizeBytes / 4
	}
	if threshold <= 0 {
		threshold = 32 * 1024 * 1024
	}
	maxFiles := opts.MaxInputFiles
	if maxFiles <= 0 {
		maxFiles = 1000
	}
	for _, f := range files {
		if f.RowCount <= 0 || f.FileSize <= 0 || f.FileSize >= threshold {
			continue
		}
		plan.InputFiles = append(plan.InputFiles, f)
		plan.InputBytes += f.FileSize
		if len(plan.InputFiles) >= maxFiles {
			break
		}
	}
	if opts.TargetFileSizeBytes > 0 && plan.InputBytes > 0 {
		plan.EstimatedOutputFiles = int(math.Ceil(float64(plan.InputBytes) / float64(opts.TargetFileSizeBytes)))
		if plan.EstimatedOutputFiles < 1 {
			plan.EstimatedOutputFiles = 1
		}
	} else if len(plan.InputFiles) > 0 {
		plan.EstimatedOutputFiles = 1
	}
	return plan, nil
}

func (c *Catalog) CommitSmallFileCompaction(ctx context.Context, schema, table string, plan CompactionPlan, outputs []DataFile) (CompactionResult, error) {
	meta, _, _, err := c.readCurrentMetadata(ctx, schema, table)
	if err != nil {
		return CompactionResult{PlannedSnapshotID: plan.PlannedSnapshotID, Aborted: true, AbortReason: err.Error()}, err
	}
	result := CompactionResult{PlannedSnapshotID: plan.PlannedSnapshotID, ValidatedSnapshotID: meta.CurrentSnapshotID, InputFiles: len(plan.InputFiles), OutputFiles: len(outputs), InputBytes: plan.InputBytes}
	if meta.CurrentSnapshotID <= 0 {
		result.Aborted = true
		result.AbortReason = "table has no current snapshot"
		return result, errors.New(result.AbortReason)
	}
	if meta.CurrentSchemaID != plan.PlannedSchemaID {
		result.Aborted = true
		result.AbortReason = fmt.Sprintf("schema changed during compaction: planned=%d current=%d", plan.PlannedSchemaID, meta.CurrentSchemaID)
		return result, errors.New(result.AbortReason)
	}
	deleteCount, err := c.activeDeleteFileCount(ctx, meta)
	if err != nil {
		result.Aborted = true
		result.AbortReason = err.Error()
		return result, err
	}
	if deleteCount > 0 {
		result.Aborted = true
		result.AbortReason = "active equality delete files found; MOR compaction required"
		return result, errors.New(result.AbortReason)
	}
	latest, err := c.activeDataFilesFromMetadata(ctx, meta)
	if err != nil {
		result.Aborted = true
		result.AbortReason = err.Error()
		return result, err
	}
	inputSet := make(map[string]struct{}, len(plan.InputFiles))
	for _, f := range plan.InputFiles {
		inputSet[canonicalDataFilePath(f.Path)] = struct{}{}
	}
	for _, f := range plan.InputFiles {
		if !dataFileInSet(latest, f.Path) {
			result.Aborted = true
			result.AbortReason = fmt.Sprintf("input file no longer active: %s", f.Path)
			return result, errors.New(result.AbortReason)
		}
	}
	carried := make([]DataFile, 0, len(latest))
	for _, f := range latest {
		if _, ok := inputSet[canonicalDataFilePath(f.Path)]; ok {
			continue
		}
		carried = append(carried, f)
	}
	for _, f := range outputs {
		result.OutputBytes += f.FileSize
	}
	result.CarriedFiles = len(carried)
	flushLSN := currentSnapshotFlushLSN(meta)
	if err := c.CommitChangesetFiles(ctx, schema, table, outputs, nil, carried, true, flushLSN); err != nil {
		result.Aborted = true
		result.AbortReason = err.Error()
		return result, err
	}
	latestMeta, _, _, err := c.readCurrentMetadata(ctx, schema, table)
	if err == nil {
		result.NewSnapshotID = latestMeta.CurrentSnapshotID
	}
	return result, nil
}

func (c *Catalog) writeCompactedDataFiles(ctx context.Context, schema, table string, plan CompactionPlan, opts CompactionOptions) ([]DataFile, error) {
	cols := plan.Columns
	if len(cols) == 0 {
		return nil, errors.New("compaction plan has no schema columns")
	}
	rows, err := c.readRowsFromDataFiles(ctx, schema, table, plan.InputFiles, cols)
	if err != nil {
		return nil, err
	}
	fieldIDs := boundedFieldIDs(plan.InputFiles)
	if len(fieldIDs) > 0 {
		sortRowsByFields(rows, cols, fieldIDs)
	}
	chunks, err := splitRowsByTarget(&pqbuilder.Builder{}, cols, rows, opts.TargetFileSizeBytes)
	if err != nil {
		return nil, err
	}
	basePath := c.tablePath(schema, table)
	out := make([]DataFile, 0, len(chunks))
	for _, chunk := range chunks {
		if len(chunk) == 0 {
			continue
		}
		data, err := (&pqbuilder.Builder{}).Build(cols, chunk)
		if err != nil {
			return nil, err
		}
		name := fmt.Sprintf("data/%s.parquet", uuid.NewString())
		key := path.Join(basePath, name)
		if err := c.storage.PutObject(ctx, key, data, "application/octet-stream"); err != nil {
			return nil, err
		}
		lower, upper := computeBoundsForFields(cols, chunk, fieldIDs)
		out = append(out, DataFile{Path: name, RowCount: int64(len(chunk)), FileSize: int64(len(data)), LowerBounds: lower, UpperBounds: upper})
	}
	return out, nil
}

func (c *Catalog) readRowsFromDataFiles(ctx context.Context, schema, table string, files []DataFile, cols []pqbuilder.ColumnDef) ([][]pqbuilder.Value, error) {
	var rows [][]pqbuilder.Value
	basePath := c.tablePath(schema, table)
	for _, f := range files {
		data, err := c.storage.GetObject(ctx, s3KeyFromURI(c.dataFileURI(basePath, f)))
		if err != nil {
			return nil, fmt.Errorf("download %s: %w", f.Path, err)
		}
		r, err := pqbuilder.ReadRows(data, cols)
		if err != nil {
			return nil, fmt.Errorf("read parquet %s: %w", f.Path, err)
		}
		rows = append(rows, r...)
	}
	return rows, nil
}

func (c *Catalog) deleteDataFiles(ctx context.Context, schema, table string, files []DataFile) error {
	if len(files) == 0 {
		return nil
	}
	basePath := c.tablePath(schema, table)
	keys := make([]string, 0, len(files))
	for _, f := range files {
		keys = append(keys, s3KeyFromURI(c.dataFileURI(basePath, f)))
	}
	return c.storage.DeleteObjects(ctx, keys)
}

func splitRowsByTarget(builder *pqbuilder.Builder, cols []pqbuilder.ColumnDef, rows [][]pqbuilder.Value, target int64) ([][][]pqbuilder.Value, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	if target <= 0 || len(rows) <= 1 {
		return [][][]pqbuilder.Value{rows}, nil
	}
	all, err := builder.Build(cols, rows)
	if err != nil {
		return nil, err
	}
	if int64(len(all)) <= target {
		return [][][]pqbuilder.Value{rows}, nil
	}
	rowsPerFile := int(math.Floor(float64(len(rows)) * float64(target) / float64(len(all))))
	if rowsPerFile < 1 {
		rowsPerFile = 1
	}
	var chunks [][][]pqbuilder.Value
	for start := 0; start < len(rows); start += rowsPerFile {
		end := start + rowsPerFile
		if end > len(rows) {
			end = len(rows)
		}
		chunks = append(chunks, rows[start:end])
	}
	return chunks, nil
}

func (c *Catalog) activeDataFilesFromMetadata(ctx context.Context, meta tableMetadata) ([]DataFile, error) {
	if meta.CurrentSnapshotID <= 0 {
		return nil, nil
	}
	var manifestListURI string
	for _, snap := range meta.Snapshots {
		if snap.SnapshotID == meta.CurrentSnapshotID {
			manifestListURI = snap.ManifestList
			break
		}
	}
	if manifestListURI == "" {
		return nil, nil
	}
	return c.dataFilesFromManifestList(ctx, manifestListURI)
}

func (c *Catalog) dataFilesFromManifestList(ctx context.Context, manifestListURI string) ([]DataFile, error) {
	manifestListData, err := c.storage.GetObject(ctx, s3KeyFromURI(manifestListURI))
	if err != nil {
		return nil, err
	}
	manifests, err := readManifestListAvro(manifestListData)
	if err != nil {
		return nil, err
	}
	var files []DataFile
	for _, mf := range manifests {
		if mf.ManifestContent() != 0 { // data manifests only
			continue
		}
		mfData, err := c.storage.GetObject(ctx, s3KeyFromURI(mf.FilePath()))
		if err != nil {
			return nil, err
		}
		entries, err := ice.ReadManifest(mf, bytes.NewReader(mfData), true)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.Status() == ice.EntryStatusDELETED {
				continue
			}
			df := entry.DataFile()
			files = append(files, DataFile{Path: df.FilePath(), RowCount: df.Count(), FileSize: df.FileSizeBytes(), LowerBounds: cloneBounds(df.LowerBoundValues()), UpperBounds: cloneBounds(df.UpperBoundValues())})
		}
	}
	return files, nil
}

func (c *Catalog) activeDeleteFileCount(ctx context.Context, meta tableMetadata) (int, error) {
	if meta.CurrentSnapshotID <= 0 {
		return 0, nil
	}
	var manifestListURI string
	for _, snap := range meta.Snapshots {
		if snap.SnapshotID == meta.CurrentSnapshotID {
			manifestListURI = snap.ManifestList
			break
		}
	}
	if manifestListURI == "" {
		return 0, nil
	}
	manifestListData, err := c.storage.GetObject(ctx, s3KeyFromURI(manifestListURI))
	if err != nil {
		return 0, err
	}
	manifests, err := readManifestListAvro(manifestListData)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, mf := range manifests {
		if mf.ManifestContent() != ice.ManifestContentDeletes {
			continue
		}
		mfData, err := c.storage.GetObject(ctx, s3KeyFromURI(mf.FilePath()))
		if err != nil {
			return 0, err
		}
		entries, err := ice.ReadManifest(mf, bytes.NewReader(mfData), true)
		if err != nil {
			return 0, err
		}
		for _, entry := range entries {
			if entry.Status() != ice.EntryStatusDELETED {
				count++
			}
		}
	}
	return count, nil
}

func currentSnapshotFlushLSN(meta tableMetadata) string {
	for _, snap := range meta.Snapshots {
		if snap.SnapshotID == meta.CurrentSnapshotID {
			return snap.Summary["streambed.last_flush_lsn"]
		}
	}
	if meta.Properties != nil && meta.CurrentSnapshotID <= 0 {
		return meta.Properties["streambed.last_flush_lsn"]
	}
	return ""
}

func dataFileInSet(files []DataFile, p string) bool {
	want := canonicalDataFilePath(p)
	for _, f := range files {
		if canonicalDataFilePath(f.Path) == want {
			return true
		}
	}
	return false
}

func canonicalDataFilePath(p string) string {
	if strings.HasPrefix(p, "s3://") {
		return p
	}
	return strings.TrimPrefix(p, "/")
}

func parquetColumnsFromMetadata(meta tableMetadata) ([]pqbuilder.ColumnDef, error) {
	var fields []schemaField
	for _, s := range meta.Schemas {
		if s.SchemaID == meta.CurrentSchemaID {
			fields = s.Fields
			break
		}
	}
	if fields == nil {
		return nil, fmt.Errorf("current schema %d not found", meta.CurrentSchemaID)
	}
	cols := make([]pqbuilder.ColumnDef, len(fields))
	for i, f := range fields {
		cols[i] = pqbuilder.ColumnDef{Name: f.Name, OID: oidFromIcebergType(f.Type), FieldID: f.ID}
	}
	return cols, nil
}

func oidFromIcebergType(t string) uint32 {
	switch t {
	case TypeBoolean:
		return 16
	case TypeInt:
		return 23
	case TypeLong:
		return 20
	case TypeFloat:
		return 700
	case TypeDouble:
		return 701
	case TypeDate:
		return 1082
	case TypeTimestamp:
		return 1114
	case TypeTimestampTZ:
		return 1184
	case TypeUUID:
		return 2950
	case TypeBinary:
		return 17
	default:
		return 25
	}
}

func boundedFieldIDs(files []DataFile) []int {
	if len(files) == 0 {
		return nil
	}
	set := make(map[int]struct{})
	for id := range files[0].LowerBounds {
		if _, ok := files[0].UpperBounds[id]; ok {
			set[id] = struct{}{}
		}
	}
	for _, f := range files[1:] {
		for id := range set {
			if _, ok := f.LowerBounds[id]; !ok {
				delete(set, id)
				continue
			}
			if _, ok := f.UpperBounds[id]; !ok {
				delete(set, id)
			}
		}
	}
	ids := make([]int, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func computeBoundsForFields(cols []pqbuilder.ColumnDef, rows [][]pqbuilder.Value, fieldIDs []int) (map[int][]byte, map[int][]byte) {
	if len(fieldIDs) == 0 || len(rows) == 0 {
		return nil, nil
	}
	idSet := make(map[int]struct{}, len(fieldIDs))
	for _, id := range fieldIDs {
		idSet[id] = struct{}{}
	}
	lower, upper := make(map[int][]byte), make(map[int][]byte)
	for _, row := range rows {
		for i, col := range cols {
			if _, ok := idSet[col.FieldID]; !ok || i >= len(row) {
				continue
			}
			b, ok := encodeBoundValue(col.OID, row[i])
			if !ok {
				delete(idSet, col.FieldID)
				delete(lower, col.FieldID)
				delete(upper, col.FieldID)
				continue
			}
			if cur, ok := lower[col.FieldID]; !ok || compareEncodedBound(col.OID, b, cur) < 0 {
				lower[col.FieldID] = append([]byte(nil), b...)
			}
			if cur, ok := upper[col.FieldID]; !ok || compareEncodedBound(col.OID, b, cur) > 0 {
				upper[col.FieldID] = append([]byte(nil), b...)
			}
		}
	}
	if len(lower) == 0 || len(upper) == 0 {
		return nil, nil
	}
	return lower, upper
}

func sortRowsByFields(rows [][]pqbuilder.Value, cols []pqbuilder.ColumnDef, fieldIDs []int) {
	if len(rows) < 2 || len(fieldIDs) == 0 {
		return
	}
	indexes := make([]int, 0, len(fieldIDs))
	for _, id := range fieldIDs {
		for i, col := range cols {
			if col.FieldID == id {
				indexes = append(indexes, i)
				break
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for _, idx := range indexes {
			if idx >= len(rows[i]) || idx >= len(rows[j]) {
				continue
			}
			cmp := compareRowValue(cols[idx].OID, rows[i][idx], rows[j][idx])
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
}

func compareRowValue(oid uint32, a, b pqbuilder.Value) int {
	if a.IsNull && b.IsNull {
		return 0
	}
	if a.IsNull {
		return -1
	}
	if b.IsNull {
		return 1
	}
	ab, aok := encodeBoundValue(oid, a)
	bb, bok := encodeBoundValue(oid, b)
	if aok && bok {
		return compareEncodedBound(oid, ab, bb)
	}
	return strings.Compare(string(a.Data), string(b.Data))
}
