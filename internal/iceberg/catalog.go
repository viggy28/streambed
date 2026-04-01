package iceberg

import (
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

// CommitSnapshot appends a new data file via a new Iceberg snapshot.
//
// Steps (in order):
//  1. Read version-hint.text → current version N
//  2. Read v<N>.metadata.json
//  3. Write manifest file (Avro) referencing the new data file
//  4. Write manifest list (Avro) referencing the manifest
//  5. Write v<N+1>.metadata.json with new snapshot
//  6. Write version-hint.text with N+1 (THE LAST STEP)
func (c *Catalog) CommitSnapshot(ctx context.Context, schema, table string, dataFile DataFile) error {
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
	manifestUUID := uuid.New().String()

	dataFilePath := fmt.Sprintf("s3://%s/%s/%s", c.bucket, basePath, dataFile.Path)

	// Build iceberg-go schema from metadata
	iceSchema := schemaFromMetadata(metadata)

	// 3. Write manifest file (Avro) using iceberg-go
	manifestFilename := fmt.Sprintf("%s-m0.avro", manifestUUID)
	manifestKey := path.Join(basePath, "metadata", manifestFilename)
	manifestS3Path := fmt.Sprintf("s3://%s/%s", c.bucket, manifestKey)

	manifestBytes, mf, err := writeManifestAvro(
		manifestS3Path, iceSchema, snapID, seqNum,
		dataFilePath, dataFile.RowCount, dataFile.FileSize,
	)
	if err != nil {
		return fmt.Errorf("write manifest avro: %w", err)
	}

	if err := c.storage.PutObject(ctx, manifestKey, manifestBytes, "application/octet-stream"); err != nil {
		return err
	}

	// 4. Build manifest list (new manifest + carry forward previous)
	manifestFiles := []ice.ManifestFile{mf}

	if metadata.CurrentSnapshotID > 0 {
		for _, snap := range metadata.Snapshots {
			if snap.SnapshotID == metadata.CurrentSnapshotID {
				prevFiles, err := c.readManifestList(ctx, snap.ManifestList)
				if err == nil {
					manifestFiles = append(manifestFiles, prevFiles...)
				}
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

	newSnapshot := snapshot{
		SnapshotID:     snapID,
		SequenceNumber: seqNum,
		TimestampMS:    time.Now().UnixMilli(),
		ManifestList:   fmt.Sprintf("s3://%s/%s", c.bucket, manifestListKey),
		Summary: map[string]string{
			"operation":        "append",
			"added-data-files": "1",
			"added-records":    strconv.FormatInt(dataFile.RowCount, 10),
			"total-records":    strconv.FormatInt(totalRecords(metadata.Snapshots)+dataFile.RowCount, 10),
		},
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
