package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"time"

	ice "github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/viggy28/streambed/internal/storage"
)

// ColumnDef describes a table column for Iceberg metadata.
type ColumnDef struct {
	Name string
	OID  uint32
}

// DataFile describes a Parquet data file to be committed.
type DataFile struct {
	Path     string // relative path under the table directory (e.g., "data/xxx.parquet")
	RowCount int64
	FileSize int64
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
	storage *storage.S3Client
	bucket  string
	prefix  string
}

func NewCatalog(s3Client *storage.S3Client, bucket, prefix string) *Catalog {
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
func (c *Catalog) CreateTable(ctx context.Context, schema, table string, columns []ColumnDef) error {
	tableLoc := fmt.Sprintf("s3://%s/%s", c.bucket, c.tablePath(schema, table))
	tableUUID := uuid.New().String()
	now := time.Now().UnixMilli()

	fields := make([]schemaField, len(columns))
	for i, col := range columns {
		fields[i] = schemaField{
			ID:       i + 1,
			Name:     col.Name,
			Required: false,
			Type:     PgOIDToIcebergType(col.OID),
		}
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
		return fmt.Errorf("marshal metadata: %w", err)
	}

	basePath := c.tablePath(schema, table)

	// Write v1.metadata.json (DuckDB looks for v<N>.metadata.json)
	metaKey := path.Join(basePath, "metadata", "v1.metadata.json")
	if err := c.storage.PutObject(ctx, metaKey, metadataJSON, "application/json"); err != nil {
		return err
	}

	// Write version-hint.text
	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	return c.storage.PutObject(ctx, hintKey, []byte("1"), "text/plain")
}

// GetDataFilePaths returns the S3 paths of all data files in the current
// snapshot for the given table. Returns nil if the table has no snapshots.
func (c *Catalog) GetDataFilePaths(ctx context.Context, schema, table string) ([]string, error) {
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

	// Find manifest list for the current snapshot.
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

	var paths []string
	for _, mf := range manifests {
		// Only read data manifests, skip delete manifests.
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
			paths = append(paths, entry.DataFile().FilePath())
		}
	}
	return paths, nil
}

// CommitSnapshot appends a new data file via a new Iceberg snapshot.
// This is a thin wrapper around CommitChangeset for append-only callers
// (see resync and initial backfill paths).
func (c *Catalog) CommitSnapshot(ctx context.Context, schema, table string, dataFile DataFile, flushLSN string) error {
	return c.CommitChangeset(ctx, schema, table, &dataFile, nil, false, flushLSN)
}

// CommitChangeset atomically adds one snapshot that may contain a data
// file and/or an equality-delete file.
//
// When replace is false (append mode), previous manifest files are
// carried forward so all historical data files remain visible. When
// replace is true (overwrite / COW mode), previous manifests are NOT
// carried forward — only the files in this commit are visible. This is
// used for copy-on-write deletes where the caller has already merged
// existing data with pending changes.
//
// When replace is true, dataFile may be nil (meaning the table is now
// empty after all rows were deleted).
//
// Steps (in order):
//  1. Read version-hint.text → current version N
//  2. Read v<N>.metadata.json
//  3. Write data-manifest (Avro) if dataFile != nil
//  4. Write eq-delete-manifest (Avro) if eqDel != nil
//  5. Write manifest list (Avro) referencing new manifests (+ prior if !replace)
//  6. Write v<N+1>.metadata.json with new snapshot
//  7. Write version-hint.text with N+1 (THE LAST STEP)
func (c *Catalog) CommitChangeset(
	ctx context.Context,
	schema, table string,
	dataFile *DataFile,
	eqDel *EqDeleteFile,
	replace bool,
	flushLSN string,
) error {
	if dataFile == nil && eqDel == nil && !replace {
		return fmt.Errorf("commit changeset: both data and delete are nil")
	}

	// COW with no data file at all (shouldn't happen now that the writer
	// always writes a Parquet file, but kept as a safety net).
	if replace && dataFile == nil {
		return c.commitEmptyTable(ctx, schema, table, flushLSN)
	}
	basePath := c.tablePath(schema, table)

	// 1. Read current version
	hintKey := path.Join(basePath, "metadata", "version-hint.text")
	hintData, err := c.storage.GetObject(ctx, hintKey)
	if err != nil {
		return fmt.Errorf("read version-hint: %w", err)
	}
	currentVersion, err := strconv.Atoi(string(hintData))
	if err != nil {
		return fmt.Errorf("parse version hint %q: %w", hintData, err)
	}

	// 2. Read current metadata
	currentMetaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", currentVersion))
	metaData, err := c.storage.GetObject(ctx, currentMetaKey)
	if err != nil {
		return fmt.Errorf("read metadata v%d: %w", currentVersion, err)
	}

	var metadata tableMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		return fmt.Errorf("parse metadata: %w", err)
	}

	snapID := time.Now().UnixMilli()
	seqNum := metadata.LastSequenceNumber + 1

	// Build iceberg-go schema from metadata
	iceSchema := schemaFromMetadata(metadata)

	// 3/4. Write one manifest per content type (data vs delete).
	var manifestFiles []ice.ManifestFile

	if dataFile != nil {
		dataFilePath := fmt.Sprintf("s3://%s/%s/%s", c.bucket, basePath, dataFile.Path)
		manifestUUID := uuid.New().String()
		manifestFilename := fmt.Sprintf("%s-m0.avro", manifestUUID)
		manifestKey := path.Join(basePath, "metadata", manifestFilename)
		manifestS3Path := fmt.Sprintf("s3://%s/%s", c.bucket, manifestKey)

		// iceberg-go requires record_count > 0 in manifest entries. When the
		// writer produces a 0-row Parquet file (all rows deleted via COW),
		// advertise 1 row so the manifest passes validation. DuckDB reads the
		// actual row count from the Parquet footer, so this is harmless.
		manifestRowCount := dataFile.RowCount
		if manifestRowCount == 0 {
			manifestRowCount = 1
		}
		manifestBytes, mf, err := writeManifestAvro(
			manifestS3Path, iceSchema, snapID, seqNum,
			dataFilePath, manifestRowCount, dataFile.FileSize,
		)
		if err != nil {
			return fmt.Errorf("write manifest avro: %w", err)
		}
		if err := c.storage.PutObject(ctx, manifestKey, manifestBytes, "application/octet-stream"); err != nil {
			return err
		}
		manifestFiles = append(manifestFiles, mf)
	}

	if eqDel != nil {
		delFilePath := fmt.Sprintf("s3://%s/%s/%s", c.bucket, basePath, eqDel.Path)
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

	// Carry forward previous manifest files (append mode only).
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
	manifestListKey := path.Join(basePath, "metadata",
		fmt.Sprintf("snap-%d-%s.avro", snapID, snapUUID))
	if err := c.storage.PutObject(ctx, manifestListKey, manifestListBytes, "application/octet-stream"); err != nil {
		return err
	}

	// 5. Write new metadata
	newVersion := currentVersion + 1
	metadata.LastSequenceNumber = seqNum
	metadata.LastUpdatedMS = time.Now().UnixMilli()
	metadata.CurrentSnapshotID = snapID

	// Build summary. Operation is "append" for data-only, "delete" for
	// delete-only, and "overwrite" for mixed (the canonical Iceberg
	// operation name for a commit that both adds and removes rows).
	summary := map[string]string{}
	var addedRows int64
	var addedData int64
	if dataFile != nil {
		addedData = 1
		addedRows = dataFile.RowCount
		summary["added-data-files"] = "1"
		summary["added-records"] = strconv.FormatInt(dataFile.RowCount, 10)
	}
	var addedDeletes int64
	var addedDeleteRows int64
	if eqDel != nil {
		addedDeletes = 1
		addedDeleteRows = eqDel.RowCount
		summary["added-delete-files"] = "1"
		summary["added-equality-deletes"] = strconv.FormatInt(eqDel.RowCount, 10)
	}
	switch {
	case replace:
		summary["operation"] = "overwrite"
	case addedData > 0 && addedDeletes > 0:
		summary["operation"] = "overwrite"
	case addedDeletes > 0:
		summary["operation"] = "delete"
	default:
		summary["operation"] = "append"
	}
	if flushLSN != "" {
		summary["streambed.last_flush_lsn"] = flushLSN
	}
	if replace {
		// Overwrite: total-records is just what's in this snapshot.
		summary["total-records"] = strconv.FormatInt(addedRows, 10)
	} else {
		// Append: accumulate over all snapshots.
		summary["total-records"] = strconv.FormatInt(totalRecords(metadata.Snapshots)+addedRows, 10)
	}
	_ = addedDeleteRows

	newSnapshot := snapshot{
		SnapshotID:     snapID,
		SequenceNumber: seqNum,
		TimestampMS:    time.Now().UnixMilli(),
		ManifestList:   fmt.Sprintf("s3://%s/%s", c.bucket, manifestListKey),
		Summary:        summary,
	}
	metadata.Snapshots = append(metadata.Snapshots, newSnapshot)
	metadata.SnapshotLog = append(metadata.SnapshotLog, snapshotLogEntry{
		TimestampMS: newSnapshot.TimestampMS,
		SnapshotID:  snapID,
	})
	metadata.Refs = map[string]interface{}{
		"main": map[string]interface{}{
			"snapshot-id": snapID,
			"type":        "branch",
		},
	}
	metadata.MetadataLog = append(metadata.MetadataLog, metadataLogEntry{
		TimestampMS:  metadata.LastUpdatedMS,
		MetadataFile: fmt.Sprintf("s3://%s/%s", c.bucket, currentMetaKey),
	})

	newMetadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal new metadata: %w", err)
	}

	newMetaKey := path.Join(basePath, "metadata", fmt.Sprintf("v%d.metadata.json", newVersion))
	if err := c.storage.PutObject(ctx, newMetaKey, newMetadataJSON, "application/json"); err != nil {
		return err
	}

	// 6. Update version-hint.text — THE LAST STEP
	return c.storage.PutObject(ctx, hintKey, []byte(strconv.Itoa(newVersion)), "text/plain")
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
	if err := c.storage.PutObject(ctx, newMetaKey, newMetadataJSON, "application/json"); err != nil {
		return err
	}
	return c.storage.PutObject(ctx, hintKey, []byte(strconv.Itoa(newVersion)), "text/plain")
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
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
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
