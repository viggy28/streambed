package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	ice "github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/viggy28/streambed/internal/failpoint"
	"github.com/viggy28/streambed/internal/storage"
	"github.com/viggy28/streambed/internal/wal"
)

// ColumnDef describes a table column for Iceberg metadata.
type ColumnDef struct {
	Name string
	OID  uint32
}

// DataFile describes a Parquet data file to be committed.
type DataFile struct {
	Path        string // relative path under the table directory (e.g., "data/xxx.parquet") or full s3:// URI for existing files
	RowCount    int64
	FileSize    int64
	LowerBounds map[int][]byte
	UpperBounds map[int][]byte
}

// EqDeleteFile describes a Parquet equality-delete file to be committed.
// The file contains one row per deletion, with only the equality columns
// present. EqualityFieldIDs must list the Iceberg field IDs of those
// columns in the same order as the file's schema.
type EqDeleteFile struct {
	Path             string // relative path under the table directory (e.g., "data/xxx-delete.parquet")
	RowCount         int64
	FileSize         int64
	EqualityFieldIDs []int
}

// Catalog manages Iceberg metadata on S3 using the filesystem catalog pattern.
type Catalog struct {
	storage storage.ObjectStorage
	bucket  string
	prefix  string
}

func NewCatalog(s3Client storage.ObjectStorage, bucket, prefix string) *Catalog {
	return &Catalog{
		storage: s3Client,
		bucket:  bucket,
		prefix:  prefix,
	}
}

func (c *Catalog) tablePath(schema, table string) string {
	return path.Join(c.prefix, schema, table)
}

// TableExists checks if version-hint.text exists for this table.
func (c *Catalog) TableExists(ctx context.Context, schema, table string) (bool, error) {
	key := path.Join(c.tablePath(schema, table), "metadata", "version-hint.text")
	return c.storage.HeadObject(ctx, key)
}

// CreateTable creates initial Iceberg v2 metadata for a new table (no snapshots).
// Returns the field ID mapping (column name → Iceberg field ID) assigned to the table.
func (c *Catalog) CreateTable(ctx context.Context, schema, table string, columns []ColumnDef) (map[string]int, error) {
	tableLoc := fmt.Sprintf("s3://%s/%s", c.bucket, c.tablePath(schema, table))
	tableUUID := uuid.New().String()
	now := time.Now().UnixMilli()

	fieldIDs := make(map[string]int, len(columns))
	fields := make([]schemaField, len(columns))
	for i, col := range columns {
		id := i + 1
		fields[i] = schemaField{
			ID:       id,
			Name:     col.Name,
			Required: false,
			Type:     PgOIDToIcebergType(col.OID),
		}
		fieldIDs[col.Name] = id
	}

	metadata := tableMetadata{
		FormatVersion:      2,
		TableUUID:          tableUUID,
		Location:           tableLoc,
		LastSequenceNumber: 0,
		LastUpdatedMS:      now,
		LastColumnID:       len(columns),
		CurrentSchemaID:    0,
		Schemas: []icebergSchema{{
			Type:            "struct",
			SchemaID:        0,
			Fields:          fields,
			IdentifierField: []int{},
		}},
		DefaultSpecID: 0,
		PartitionSpecs: []partitionSpec{
			{SpecID: 0, Fields: []interface{}{}},
		},
		LastPartitionID:    999,
		DefaultSortOrderID: 0,
		SortOrders: []sortOrder{
			{OrderID: 0, Fields: []interface{}{}},
		},
		Properties:        map[string]string{},
		CurrentSnapshotID: -1,
		Refs:              map[string]interface{}{},
		Snapshots:         []snapshot{},
		SnapshotLog:       []snapshotLogEntry{},
		MetadataLog:       []metadataLogEntry{},
	}

	metadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}

	basePath := c.tablePath(schema, table)

	// Write v1.metadata.json (DuckDB looks for v<N>.metadata.json)
	metaKey := path.Join(basePath, "metadata", "v1.metadata.json")
	if err := c.storage.PutObject(ctx, metaKey, metadataJSON, "application/json"); err != nil {
		return nil, err
	}

	// Write version-hint.text
	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	if err := c.storage.PutObject(ctx, hintKey, []byte("1"), "text/plain"); err != nil {
		return nil, err
	}
	return fieldIDs, nil
}

// SchemaFieldInfo describes a single field in the current Iceberg schema.
// Exported so the writer can diff WAL columns against Iceberg on restart.
type SchemaFieldInfo struct {
	ID   int
	Name string
	Type IcebergType
}

// GetFieldIDs reads the current Iceberg metadata for a table and returns
// the field ID mapping (column name → Iceberg field ID) and the lastColumnID.
// Returns an error if the table does not exist or metadata is unreadable.
func (c *Catalog) GetFieldIDs(ctx context.Context, schema, table string) (map[string]int, int, error) {
	basePath := c.tablePath(schema, table)

	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	hintData, err := c.storage.GetObject(ctx, hintKey)
	if err != nil {
		return nil, 0, fmt.Errorf("read version-hint: %w", err)
	}
	version, err := strconv.Atoi(string(hintData))
	if err != nil {
		return nil, 0, fmt.Errorf("parse version hint: %w", err)
	}

	metaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", version))
	metaData, err := c.storage.GetObject(ctx, metaKey)
	if err != nil {
		return nil, 0, fmt.Errorf("read metadata: %w", err)
	}
	var metadata tableMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		return nil, 0, fmt.Errorf("parse metadata: %w", err)
	}

	if len(metadata.Schemas) == 0 {
		return nil, 0, fmt.Errorf("no schemas in metadata")
	}

	currentSchema := metadata.Schemas[metadata.CurrentSchemaID]
	fieldIDs := make(map[string]int, len(currentSchema.Fields))
	for _, f := range currentSchema.Fields {
		fieldIDs[f.Name] = f.ID
	}
	return fieldIDs, metadata.LastColumnID, nil
}

// GetSchemaFields reads the current Iceberg schema for a table and returns
// the list of fields with their IDs, names, and types. Used by the writer
// to reconcile WAL-provided columns against Iceberg on restart.
func (c *Catalog) GetSchemaFields(ctx context.Context, schema, table string) ([]SchemaFieldInfo, error) {
	basePath := c.tablePath(schema, table)

	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	hintData, err := c.storage.GetObject(ctx, hintKey)
	if err != nil {
		return nil, fmt.Errorf("read version-hint: %w", err)
	}
	version, err := strconv.Atoi(string(hintData))
	if err != nil {
		return nil, fmt.Errorf("parse version hint: %w", err)
	}

	metaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", version))
	metaData, err := c.storage.GetObject(ctx, metaKey)
	if err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	}
	var metadata tableMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		return nil, fmt.Errorf("parse metadata: %w", err)
	}

	if len(metadata.Schemas) == 0 {
		return nil, fmt.Errorf("no schemas in metadata")
	}

	currentSchema := metadata.Schemas[metadata.CurrentSchemaID]
	fields := make([]SchemaFieldInfo, len(currentSchema.Fields))
	for i, f := range currentSchema.Fields {
		fields[i] = SchemaFieldInfo{ID: f.ID, Name: f.Name, Type: f.Type}
	}
	return fields, nil
}

// EvolveSchema applies schema changes (ADD, DROP, TYPE_CHANGE) to the
// Iceberg table metadata. It reads the current metadata, computes the new
// schema, writes a new metadata version, and returns the updated field ID
// mapping.
//
// For ADD columns, defaults maps column names to their Postgres default
// expression (used to set Iceberg's initial-default). Pass nil if no
// defaults are available.
func (c *Catalog) EvolveSchema(
	ctx context.Context,
	schema, table string,
	changes []wal.SchemaChange,
	newColumns []wal.Column,
	defaults map[string]string,
) (map[string]int, error) {
	basePath := c.tablePath(schema, table)

	// 1. Read current version.
	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	hintData, err := c.storage.GetObject(ctx, hintKey)
	if err != nil {
		return nil, fmt.Errorf("read version-hint: %w", err)
	}
	currentVersion, err := strconv.Atoi(string(hintData))
	if err != nil {
		return nil, fmt.Errorf("parse version hint: %w", err)
	}

	// 2. Read current metadata.
	currentMetaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", currentVersion))
	metaData, err := c.storage.GetObject(ctx, currentMetaKey)
	if err != nil {
		return nil, fmt.Errorf("read metadata v%d: %w", currentVersion, err)
	}
	var metadata tableMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		return nil, fmt.Errorf("parse metadata: %w", err)
	}

	if len(metadata.Schemas) == 0 {
		return nil, fmt.Errorf("no schemas in metadata for %s.%s", schema, table)
	}

	// 3. Build the evolved schema from the current one.
	oldSchema := metadata.Schemas[metadata.CurrentSchemaID]
	oldFieldsByName := make(map[string]schemaField, len(oldSchema.Fields))
	for _, f := range oldSchema.Fields {
		oldFieldsByName[f.Name] = f
	}

	lastColID := metadata.LastColumnID
	var newFields []schemaField

	// Start with existing fields, applying DROP and TYPE_CHANGE.
	dropSet := make(map[string]bool)
	typeChanges := make(map[string]wal.SchemaChange)
	for _, ch := range changes {
		switch ch.Type {
		case wal.SchemaChangeDrop:
			dropSet[ch.Column] = true
		case wal.SchemaChangeTypeChange:
			typeChanges[ch.Column] = ch
		}
	}

	for _, f := range oldSchema.Fields {
		if dropSet[f.Name] {
			continue // dropped
		}
		if ch, changed := typeChanges[f.Name]; changed {
			oldType := f.Type
			newType := PgOIDToIcebergType(ch.NewOID)
			if err := ValidateTypeWidening(oldType, newType); err != nil {
				return nil, fmt.Errorf("column %q: %w", f.Name, err)
			}
			f.Type = newType
		}
		newFields = append(newFields, f)
	}

	// Apply ADD columns.
	for _, ch := range changes {
		if ch.Type != wal.SchemaChangeAdd {
			continue
		}
		lastColID++
		sf := schemaField{
			ID:       lastColID,
			Name:     ch.Column,
			Required: false,
			Type:     PgOIDToIcebergType(ch.NewOID),
		}
		if defaults != nil {
			if defVal, ok := defaults[ch.Column]; ok && defVal != "" {
				sf.InitialDefault = defVal
				sf.WriteDefault = defVal
			}
		}
		newFields = append(newFields, sf)
	}

	// 4. Create new schema entry.
	newSchemaID := oldSchema.SchemaID + 1
	evolvedSchema := icebergSchema{
		Type:            "struct",
		SchemaID:        newSchemaID,
		Fields:          newFields,
		IdentifierField: []int{},
	}

	metadata.Schemas = append(metadata.Schemas, evolvedSchema)
	metadata.CurrentSchemaID = newSchemaID
	metadata.LastColumnID = lastColID
	metadata.LastUpdatedMS = time.Now().UnixMilli()
	metadata.MetadataLog = append(metadata.MetadataLog, metadataLogEntry{
		TimestampMS:  metadata.LastUpdatedMS,
		MetadataFile: fmt.Sprintf("s3://%s/%s", c.bucket, currentMetaKey),
	})

	// 5. Write new metadata version.
	newVersion := currentVersion + 1
	newMetadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal evolved metadata: %w", err)
	}

	newMetaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", newVersion))
	if err := c.storage.PutObject(ctx, newMetaKey, newMetadataJSON, "application/json"); err != nil {
		return nil, err
	}
	if err := c.storage.PutObject(ctx, hintKey, []byte(strconv.Itoa(newVersion)), "text/plain"); err != nil {
		return nil, err
	}

	// 6. Build and return field ID mapping.
	fieldIDs := make(map[string]int, len(newFields))
	for _, f := range newFields {
		fieldIDs[f.Name] = f.ID
	}
	return fieldIDs, nil
}

// GetActiveDataFiles returns all active data files in the current snapshot.
// Bounds are populated when present in the Iceberg manifest. Returns nil if
// the table has no current snapshot.
func (c *Catalog) GetActiveDataFiles(ctx context.Context, schema, table string) ([]DataFile, error) {
	basePath := c.tablePath(schema, table)

	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	hintData, err := c.storage.GetObject(ctx, hintKey)
	if err != nil {
		return nil, fmt.Errorf("read version-hint: %w", err)
	}
	version, err := strconv.Atoi(string(hintData))
	if err != nil {
		return nil, fmt.Errorf("parse version hint: %w", err)
	}

	metaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", version))
	metaData, err := c.storage.GetObject(ctx, metaKey)
	if err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	}
	var metadata tableMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		return nil, fmt.Errorf("parse metadata: %w", err)
	}

	if metadata.CurrentSnapshotID <= 0 {
		return nil, nil
	}

	var manifestListURI string
	for _, snap := range metadata.Snapshots {
		if snap.SnapshotID == metadata.CurrentSnapshotID {
			manifestListURI = snap.ManifestList
			break
		}
	}
	if manifestListURI == "" {
		return nil, nil
	}

	manifestListData, err := c.storage.GetObject(ctx, s3KeyFromURI(manifestListURI))
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}
	manifests, err := readManifestListAvro(manifestListData)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list: %w", err)
	}

	var files []DataFile
	for _, mf := range manifests {
		if mf.ManifestContent() != ice.ManifestContentData {
			continue
		}
		mfData, err := c.storage.GetObject(ctx, s3KeyFromURI(mf.FilePath()))
		if err != nil {
			return nil, fmt.Errorf("read manifest %s: %w", mf.FilePath(), err)
		}
		entries, err := ice.ReadManifest(mf, bytes.NewReader(mfData), true)
		if err != nil {
			return nil, fmt.Errorf("parse manifest: %w", err)
		}
		for _, entry := range entries {
			if entry.Status() == ice.EntryStatusDELETED {
				continue
			}
			df := entry.DataFile()
			files = append(files, DataFile{
				Path:        df.FilePath(),
				RowCount:    df.Count(),
				FileSize:    df.FileSizeBytes(),
				LowerBounds: cloneBounds(df.LowerBoundValues()),
				UpperBounds: cloneBounds(df.UpperBoundValues()),
			})
		}
	}
	return files, nil
}

// GetDataFilePaths returns the S3 paths of all data files in the current
// snapshot for the given table. Returns nil if the table has no snapshots.
func (c *Catalog) GetDataFilePaths(ctx context.Context, schema, table string) ([]string, error) {
	files, err := c.GetActiveDataFiles(ctx, schema, table)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(files))
	for _, f := range files {
		paths = append(paths, f.Path)
	}
	return paths, nil
}

func cloneBounds(in map[int][]byte) map[int][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int][]byte, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

// ValidateMutationMode fails fast when COW is selected but an existing table
// has active delete manifests. COW does not apply those deletes while reading
// existing data and could otherwise resurrect rows on the first mutation.
func (c *Catalog) ValidateMutationMode(ctx context.Context, mode MutationMode) error {
	if mode != MutationModeCOW {
		return nil
	}

	keys, err := c.storage.ListPrefix(ctx, c.prefix)
	if err != nil {
		return fmt.Errorf("discover Iceberg tables for mutation-mode preflight: %w", err)
	}

	prefix := strings.Trim(path.Clean(c.prefix), "/")
	var incompatible []string
	for _, key := range keys {
		cleanKey := strings.TrimPrefix(path.Clean(key), "/")
		rel := cleanKey
		if prefix != "." && prefix != "" {
			var ok bool
			rel, ok = strings.CutPrefix(cleanKey, prefix+"/")
			if !ok {
				continue
			}
		}
		parts := strings.Split(rel, "/")
		if len(parts) != 4 || parts[2] != "metadata" || parts[3] != "version-hint.text" {
			continue
		}
		hasDeletes, err := c.HasEqualityDeleteFiles(ctx, parts[0], parts[1])
		if err != nil {
			return fmt.Errorf("inspect %s.%s for mutation-mode preflight: %w", parts[0], parts[1], err)
		}
		if hasDeletes {
			incompatible = append(incompatible, parts[0]+"."+parts[1])
		}
	}
	if len(incompatible) == 0 {
		return nil
	}
	sort.Strings(incompatible)
	return fmt.Errorf("cannot start with mutation-mode=cow: active equality deletes exist for %s; restart with --mutation-mode=mor", strings.Join(incompatible, ", "))
}

// HasEqualityDeleteFiles reports whether the current snapshot references any
// equality/position delete manifests. The current COW reader cannot apply
// those deletes, so switching an MOR table back to COW must fail safely.
func (c *Catalog) HasEqualityDeleteFiles(ctx context.Context, schema, table string) (bool, error) {
	basePath := c.tablePath(schema, table)
	hintData, err := c.storage.GetObject(ctx, path.Join(basePath, "metadata", "version-hint.text"))
	if err != nil {
		return false, fmt.Errorf("read version-hint: %w", err)
	}
	version, err := strconv.Atoi(string(hintData))
	if err != nil {
		return false, fmt.Errorf("parse version hint: %w", err)
	}
	metaData, err := c.storage.GetObject(ctx, path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", version)))
	if err != nil {
		return false, fmt.Errorf("read metadata: %w", err)
	}
	var metadata tableMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		return false, fmt.Errorf("parse metadata: %w", err)
	}
	for _, snap := range metadata.Snapshots {
		if snap.SnapshotID != metadata.CurrentSnapshotID {
			continue
		}
		manifests, err := c.readManifestList(ctx, snap.ManifestList)
		if err != nil {
			return false, err
		}
		for _, manifest := range manifests {
			if manifest.ManifestContent() == ice.ManifestContentDeletes {
				return true, nil
			}
		}
	}
	return false, nil
}

// CommitSnapshot appends a new data file via a new Iceberg snapshot.
// This is a thin wrapper around CommitChangeset for append-only callers
// (see resync and initial backfill paths).
func (c *Catalog) CommitSnapshot(ctx context.Context, schema, table string, dataFile DataFile, flushLSN string) error {
	return c.CommitChangesetFiles(ctx, schema, table, []DataFile{dataFile}, nil, nil, false, flushLSN)
}

// CommitChangeset atomically adds one snapshot that may contain a data
// file and/or an equality-delete file. Kept as a compatibility wrapper for
// single-file callers.
func (c *Catalog) CommitChangeset(
	ctx context.Context,
	schema, table string,
	dataFile *DataFile,
	eqDel *EqDeleteFile,
	replace bool,
	flushLSN string,
) error {
	var dataFiles []DataFile
	if dataFile != nil {
		dataFiles = []DataFile{*dataFile}
	}
	var deleteFiles []EqDeleteFile
	if eqDel != nil {
		deleteFiles = []EqDeleteFile{*eqDel}
	}
	return c.CommitChangesetFiles(ctx, schema, table, dataFiles, deleteFiles, nil, replace, flushLSN)
}

// CommitChangesetFiles commits a snapshot with multiple data/delete files.
// In replace mode, carriedDataFiles are active existing data files that should
// remain visible without being rewritten; dataFiles are newly written
// replacements. In append mode, carriedDataFiles must be nil because prior
// manifests are carried forward automatically.
func (c *Catalog) CommitChangesetFiles(
	ctx context.Context,
	schema, table string,
	dataFiles []DataFile,
	eqDeleteFiles []EqDeleteFile,
	carriedDataFiles []DataFile,
	replace bool,
	flushLSN string,
) error {
	if len(dataFiles) == 0 && len(eqDeleteFiles) == 0 && len(carriedDataFiles) == 0 && !replace {
		return fmt.Errorf("commit changeset: no data or delete files")
	}
	if replace && len(dataFiles) == 0 && len(carriedDataFiles) == 0 {
		return c.commitEmptyTable(ctx, schema, table, flushLSN)
	}

	basePath := c.tablePath(schema, table)
	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	hintData, err := c.storage.GetObject(ctx, hintKey)
	if err != nil {
		return fmt.Errorf("read version-hint: %w", err)
	}
	currentVersion, err := strconv.Atoi(string(hintData))
	if err != nil {
		return fmt.Errorf("parse version hint %q: %w", hintData, err)
	}

	currentMetaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", currentVersion))
	metaData, err := c.storage.GetObject(ctx, currentMetaKey)
	if err != nil {
		return fmt.Errorf("read metadata v%d: %w", currentVersion, err)
	}
	var metadata tableMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		return fmt.Errorf("parse metadata: %w", err)
	}

	snapID := time.Now().UnixNano()
	seqNum := metadata.LastSequenceNumber + 1
	iceSchema := schemaFromMetadata(metadata)
	var manifestFiles []ice.ManifestFile

	allDataFiles := append([]DataFile{}, carriedDataFiles...)
	allDataFiles = append(allDataFiles, dataFiles...)
	if len(allDataFiles) > 0 {
		manifestUUID := uuid.New().String()
		manifestFilename := fmt.Sprintf("%s-m0.avro", manifestUUID)
		manifestKey := path.Join(basePath, "metadata", manifestFilename)
		manifestS3Path := fmt.Sprintf("s3://%s/%s", c.bucket, manifestKey)
		entries := make([]dataManifestEntry, 0, len(allDataFiles))
		for _, f := range allDataFiles {
			if f.RowCount <= 0 {
				continue
			}
			entries = append(entries, dataManifestEntry{FilePath: c.dataFileURI(basePath, f), File: f})
		}
		if len(entries) > 0 {
			if err := failpoint.Check(ctx, "before_manifest_write"); err != nil {
				return err
			}
			manifestBytes, mf, err := writeDataManifestAvro(manifestS3Path, iceSchema, snapID, seqNum, entries)
			if err != nil {
				return fmt.Errorf("write data manifest avro: %w", err)
			}
			if err := c.storage.PutObject(ctx, manifestKey, manifestBytes, "application/octet-stream"); err != nil {
				return err
			}
			manifestFiles = append(manifestFiles, mf)
		}
	}

	for _, eqDel := range eqDeleteFiles {
		delFilePath := c.dataFileURI(basePath, DataFile{Path: eqDel.Path})
		manifestUUID := uuid.New().String()
		manifestFilename := fmt.Sprintf("%s-m0-del.avro", manifestUUID)
		manifestKey := path.Join(basePath, "metadata", manifestFilename)
		manifestS3Path := fmt.Sprintf("s3://%s/%s", c.bucket, manifestKey)
		manifestBytes, mf, err := writeEqDeleteManifestAvro(
			manifestS3Path, iceSchema, snapID, seqNum,
			delFilePath, eqDel.RowCount, eqDel.FileSize, eqDel.EqualityFieldIDs,
		)
		if err != nil {
			return fmt.Errorf("write eq-delete manifest avro: %w", err)
		}
		if err := c.storage.PutObject(ctx, manifestKey, manifestBytes, "application/octet-stream"); err != nil {
			return err
		}
		manifestFiles = append(manifestFiles, mf)
	}

	// Carry forward previous manifest files in append mode only. Replace mode
	// publishes a new active data manifest containing carried + replacement
	// files, so old manifests are intentionally not active.
	if !replace && metadata.CurrentSnapshotID > 0 {
		for _, snap := range metadata.Snapshots {
			if snap.SnapshotID == metadata.CurrentSnapshotID {
				prevFiles, err := c.readManifestList(ctx, snap.ManifestList)
				if err != nil {
					return fmt.Errorf("carry forward manifest list from snapshot %d: %w", snap.SnapshotID, err)
				}
				manifestFiles = append(manifestFiles, prevFiles...)
				break
			}
		}
	}

	var parentSnapID *int64
	if metadata.CurrentSnapshotID > 0 {
		parentSnapID = &metadata.CurrentSnapshotID
	}
	manifestListBytes, err := writeManifestListAvro(snapID, seqNum, parentSnapID, manifestFiles)
	if err != nil {
		return fmt.Errorf("write manifest list avro: %w", err)
	}
	snapUUID := uuid.New().String()
	manifestListKey := path.Join(basePath, "metadata", fmt.Sprintf("snap-%d-%s.avro", snapID, snapUUID))
	if err := c.storage.PutObject(ctx, manifestListKey, manifestListBytes, "application/octet-stream"); err != nil {
		return err
	}

	newVersion := currentVersion + 1
	previousTotal, previousTotalExact := currentSnapshotTotalRecords(metadata)
	metadata.LastSequenceNumber = seqNum
	metadata.LastUpdatedMS = time.Now().UnixMilli()
	metadata.CurrentSnapshotID = snapID

	var addedRows, totalRows int64
	for _, f := range dataFiles {
		addedRows += f.RowCount
	}
	for _, f := range allDataFiles {
		totalRows += f.RowCount
	}
	summary := map[string]string{}
	if len(dataFiles) > 0 {
		summary["added-data-files"] = strconv.Itoa(len(dataFiles))
		summary["added-records"] = strconv.FormatInt(addedRows, 10)
	}
	var deleteRows int64
	for _, f := range eqDeleteFiles {
		deleteRows += f.RowCount
	}
	if len(eqDeleteFiles) > 0 {
		summary["added-delete-files"] = strconv.Itoa(len(eqDeleteFiles))
		summary["added-equality-deletes"] = strconv.FormatInt(deleteRows, 10)
	}
	switch {
	case replace:
		summary["operation"] = "overwrite"
	case len(dataFiles) > 0 && len(eqDeleteFiles) > 0:
		summary["operation"] = "overwrite"
	case len(eqDeleteFiles) > 0:
		summary["operation"] = "delete"
	default:
		summary["operation"] = "append"
	}
	if flushLSN != "" {
		summary["streambed.last_flush_lsn"] = flushLSN
	}
	if replace {
		summary["total-records"] = strconv.FormatInt(totalRows, 10)
	} else if len(eqDeleteFiles) == 0 && !hasEqualityDeletes(metadata.Snapshots) && previousTotalExact {
		summary["total-records"] = strconv.FormatInt(previousTotal+addedRows, 10)
	}

	newSnapshot := snapshot{SnapshotID: snapID, SequenceNumber: seqNum, TimestampMS: time.Now().UnixMilli(), ManifestList: fmt.Sprintf("s3://%s/%s", c.bucket, manifestListKey), Summary: summary}
	metadata.Snapshots = append(metadata.Snapshots, newSnapshot)
	metadata.SnapshotLog = append(metadata.SnapshotLog, snapshotLogEntry{TimestampMS: newSnapshot.TimestampMS, SnapshotID: snapID})
	metadata.Refs = map[string]interface{}{"main": map[string]interface{}{"snapshot-id": snapID, "type": "branch"}}
	metadata.MetadataLog = append(metadata.MetadataLog, metadataLogEntry{TimestampMS: metadata.LastUpdatedMS, MetadataFile: fmt.Sprintf("s3://%s/%s", c.bucket, currentMetaKey)})

	newMetadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal new metadata: %w", err)
	}
	newMetaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", newVersion))
	if err := failpoint.Check(ctx, "before_metadata_commit"); err != nil {
		return err
	}
	if err := c.storage.PutObject(ctx, newMetaKey, newMetadataJSON, "application/json"); err != nil {
		return err
	}
	if err := c.storage.PutObject(ctx, hintKey, []byte(strconv.Itoa(newVersion)), "text/plain"); err != nil {
		return err
	}
	return failpoint.Check(ctx, "after_metadata_commit_before_state_or_ack")
}

func (c *Catalog) dataFileURI(basePath string, f DataFile) string {
	if strings.HasPrefix(f.Path, "s3://") {
		return f.Path
	}
	return fmt.Sprintf("s3://%s/%s/%s", c.bucket, basePath, f.Path)
}

// commitEmptyTable bumps the metadata version with current-snapshot-id = -1,
// effectively marking the table as "exists but has no data". This is used when
// a COW delete removes all rows — it avoids both iceberg-go's record_count>0
// validation and DuckDB's empty-manifest-list crash.
func (c *Catalog) commitEmptyTable(ctx context.Context, schema, table, flushLSN string) error {
	basePath := c.tablePath(schema, table)

	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	hintData, err := c.storage.GetObject(ctx, hintKey)
	if err != nil {
		return fmt.Errorf("read version-hint: %w", err)
	}
	currentVersion, err := strconv.Atoi(string(hintData))
	if err != nil {
		return fmt.Errorf("parse version hint: %w", err)
	}

	currentMetaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", currentVersion))
	metaData, err := c.storage.GetObject(ctx, currentMetaKey)
	if err != nil {
		return fmt.Errorf("read metadata v%d: %w", currentVersion, err)
	}

	var metadata tableMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		return fmt.Errorf("parse metadata: %w", err)
	}

	// Reset to no active snapshot. Clear both current-snapshot-id AND
	// the snapshots array — DuckDB's iceberg_scan ignores current-snapshot-id=-1
	// and falls back to the last entry in the snapshots list if present.
	newVersion := currentVersion + 1
	metadata.LastSequenceNumber++
	metadata.LastUpdatedMS = time.Now().UnixMilli()
	metadata.CurrentSnapshotID = -1
	metadata.Snapshots = []snapshot{}
	metadata.SnapshotLog = []snapshotLogEntry{}
	metadata.Refs = map[string]interface{}{}
	// Persist flushLSN in table properties since there are no snapshots
	// to carry it in the summary. GetSnapshotFlushLSN reads this as a
	// fallback when current-snapshot-id == -1.
	if flushLSN != "" {
		if metadata.Properties == nil {
			metadata.Properties = map[string]string{}
		}
		metadata.Properties["streambed.last_flush_lsn"] = flushLSN
	}
	metadata.MetadataLog = append(metadata.MetadataLog, metadataLogEntry{
		TimestampMS:  metadata.LastUpdatedMS,
		MetadataFile: fmt.Sprintf("s3://%s/%s", c.bucket, currentMetaKey),
	})

	newMetadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal empty-table metadata: %w", err)
	}

	newMetaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", newVersion))
	if err := failpoint.Check(ctx, "before_metadata_commit"); err != nil {
		return err
	}
	if err := c.storage.PutObject(ctx, newMetaKey, newMetadataJSON, "application/json"); err != nil {
		return err
	}
	if err := c.storage.PutObject(ctx, hintKey, []byte(strconv.Itoa(newVersion)), "text/plain"); err != nil {
		return err
	}
	return failpoint.Check(ctx, "after_metadata_commit_before_state_or_ack")
}

// GetSnapshotFlushLSN reads the streambed.last_flush_lsn from the current
// Iceberg snapshot's summary (or from table properties for empty tables).
// Returns ("", false, nil) if the table exists but has no recorded LSN.
// Returns an error if the table doesn't exist or metadata is unreadable.
func (c *Catalog) GetSnapshotFlushLSN(ctx context.Context, schema, table string) (string, bool, error) {
	basePath := c.tablePath(schema, table)

	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	hintData, err := c.storage.GetObject(ctx, hintKey)
	if err != nil {
		return "", false, fmt.Errorf("read version-hint: %w", err)
	}
	version, err := strconv.Atoi(string(hintData))
	if err != nil {
		return "", false, fmt.Errorf("parse version hint: %w", err)
	}

	metaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", version))
	metaData, err := c.storage.GetObject(ctx, metaKey)
	if err != nil {
		return "", false, fmt.Errorf("read metadata: %w", err)
	}
	var metadata tableMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		return "", false, fmt.Errorf("parse metadata: %w", err)
	}

	// Empty table (post-TRUNCATE / commitEmptyTable): LSN is in properties.
	if metadata.CurrentSnapshotID <= 0 {
		if lsn, ok := metadata.Properties["streambed.last_flush_lsn"]; ok {
			return lsn, true, nil
		}
		return "", false, nil
	}

	// Find current snapshot and read summary.
	for _, snap := range metadata.Snapshots {
		if snap.SnapshotID == metadata.CurrentSnapshotID {
			if lsn, ok := snap.Summary["streambed.last_flush_lsn"]; ok {
				return lsn, true, nil
			}
			return "", false, nil
		}
	}
	return "", false, nil
}

// readManifestList reads a manifest list Avro file using iceberg-go.
func (c *Catalog) readManifestList(ctx context.Context, uri string) ([]ice.ManifestFile, error) {
	key := s3KeyFromURI(uri)
	data, err := c.storage.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	return readManifestListAvro(data)
}

// schemaFromMetadata constructs an iceberg-go Schema from our metadata JSON schema.
func schemaFromMetadata(meta tableMetadata) *ice.Schema {
	if len(meta.Schemas) == 0 {
		return ice.NewSchema(0)
	}
	s := meta.Schemas[meta.CurrentSchemaID]
	fields := make([]ice.NestedField, len(s.Fields))
	for i, f := range s.Fields {
		fields[i] = ice.NestedField{
			ID:       f.ID,
			Name:     f.Name,
			Required: f.Required,
			Type:     icebergTypeStringToPrimitive(f.Type),
		}
	}
	return ice.NewSchema(s.SchemaID, fields...)
}

// icebergTypeStringToPrimitive converts our metadata type string to iceberg-go Type.
func icebergTypeStringToPrimitive(t string) ice.Type {
	switch t {
	case "boolean":
		return ice.PrimitiveTypes.Bool
	case "int":
		return ice.PrimitiveTypes.Int32
	case "long":
		return ice.PrimitiveTypes.Int64
	case "float":
		return ice.PrimitiveTypes.Float32
	case "double":
		return ice.PrimitiveTypes.Float64
	case "date":
		return ice.PrimitiveTypes.Date
	case "timestamp":
		return ice.PrimitiveTypes.Timestamp
	case "timestamptz":
		return ice.PrimitiveTypes.TimestampTz
	case "uuid":
		return ice.PrimitiveTypes.UUID
	case "binary":
		return ice.PrimitiveTypes.Binary
	default:
		return ice.PrimitiveTypes.String
	}
}

func hasEqualityDeletes(snapshots []snapshot) bool {
	for _, s := range snapshots {
		if v, ok := s.Summary["added-equality-deletes"]; ok && v != "0" {
			return true
		}
	}
	return false
}

func currentSnapshotTotalRecords(metadata tableMetadata) (int64, bool) {
	if metadata.CurrentSnapshotID <= 0 {
		return 0, true
	}
	for _, snap := range metadata.Snapshots {
		if snap.SnapshotID != metadata.CurrentSnapshotID {
			continue
		}
		value, ok := snap.Summary["total-records"]
		if !ok {
			return 0, false
		}
		total, err := strconv.ParseInt(value, 10, 64)
		return total, err == nil
	}
	return 0, false
}

func totalRecords(snapshots []snapshot) int64 {
	var total int64
	for _, s := range snapshots {
		if v, ok := s.Summary["added-records"]; ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				total += n
			}
		}
	}
	return total
}

func s3KeyFromURI(uri string) string {
	if len(uri) > 5 && uri[:5] == "s3://" {
		rest := uri[5:]
		for i := 0; i < len(rest); i++ {
			if rest[i] == '/' {
				return rest[i+1:]
			}
		}
	}
	return uri
}

// Iceberg metadata JSON structures

type tableMetadata struct {
	FormatVersion      int                    `json:"format-version"`
	TableUUID          string                 `json:"table-uuid"`
	Location           string                 `json:"location"`
	LastSequenceNumber int64                  `json:"last-sequence-number"`
	LastUpdatedMS      int64                  `json:"last-updated-ms"`
	LastColumnID       int                    `json:"last-column-id"`
	CurrentSchemaID    int                    `json:"current-schema-id"`
	Schemas            []icebergSchema        `json:"schemas"`
	DefaultSpecID      int                    `json:"default-spec-id"`
	PartitionSpecs     []partitionSpec        `json:"partition-specs"`
	LastPartitionID    int                    `json:"last-partition-id"`
	DefaultSortOrderID int                    `json:"default-sort-order-id"`
	SortOrders         []sortOrder            `json:"sort-orders"`
	Properties         map[string]string      `json:"properties"`
	CurrentSnapshotID  int64                  `json:"current-snapshot-id"`
	Refs               map[string]interface{} `json:"refs"`
	Snapshots          []snapshot             `json:"snapshots"`
	SnapshotLog        []snapshotLogEntry     `json:"snapshot-log"`
	MetadataLog        []metadataLogEntry     `json:"metadata-log"`
}

type icebergSchema struct {
	Type            string        `json:"type"`
	SchemaID        int           `json:"schema-id"`
	Fields          []schemaField `json:"fields"`
	IdentifierField []int         `json:"identifier-field-ids"`
}

type schemaField struct {
	ID             int    `json:"id"`
	Name           string `json:"name"`
	Required       bool   `json:"required"`
	Type           string `json:"type"`
	InitialDefault string `json:"initial-default,omitempty"`
	WriteDefault   string `json:"write-default,omitempty"`
}

type partitionSpec struct {
	SpecID int           `json:"spec-id"`
	Fields []interface{} `json:"fields"`
}

type sortOrder struct {
	OrderID int           `json:"order-id"`
	Fields  []interface{} `json:"fields"`
}

type snapshot struct {
	SnapshotID     int64             `json:"snapshot-id"`
	SequenceNumber int64             `json:"sequence-number"`
	TimestampMS    int64             `json:"timestamp-ms"`
	ManifestList   string            `json:"manifest-list"`
	Summary        map[string]string `json:"summary"`
}

type snapshotLogEntry struct {
	TimestampMS int64 `json:"timestamp-ms"`
	SnapshotID  int64 `json:"snapshot-id"`
}

type metadataLogEntry struct {
	TimestampMS  int64  `json:"timestamp-ms"`
	MetadataFile string `json:"metadata-file"`
}
