package iceberg

import (
	"bytes"
	"fmt"

	ice "github.com/apache/iceberg-go"
)

// pgOIDToIcebergPrimitive maps a Postgres OID to an iceberg-go PrimitiveType.
func pgOIDToIcebergPrimitive(oid uint32) ice.Type {
	switch oid {
	case 16: // bool
		return ice.PrimitiveTypes.Bool
	case 21, 23: // int2, int4
		return ice.PrimitiveTypes.Int32
	case 20: // int8
		return ice.PrimitiveTypes.Int64
	case 700: // float4
		return ice.PrimitiveTypes.Float32
	case 701: // float8
		return ice.PrimitiveTypes.Float64
	case 1082: // date
		return ice.PrimitiveTypes.Date
	case 1114: // timestamp
		return ice.PrimitiveTypes.Timestamp
	case 1184: // timestamptz
		return ice.PrimitiveTypes.TimestampTz
	case 2950: // uuid
		return ice.PrimitiveTypes.UUID
	case 17: // bytea
		return ice.PrimitiveTypes.Binary
	default:
		// text, varchar, json, jsonb, numeric, etc.
		return ice.PrimitiveTypes.String
	}
}

// buildIcebergSchema creates an iceberg-go Schema from our column definitions.
func buildIcebergSchema(columns []ColumnDef) *ice.Schema {
	fields := make([]ice.NestedField, len(columns))
	for i, col := range columns {
		fields[i] = ice.NestedField{
			ID:       i + 1,
			Name:     col.Name,
			Required: false,
			Type:     pgOIDToIcebergPrimitive(col.OID),
		}
	}
	return ice.NewSchema(0, fields...)
}

// writeManifestAvro writes an Avro manifest file using iceberg-go's WriteManifest.
// Returns the raw Avro bytes and the ManifestFile metadata object.
func writeManifestAvro(
	filename string,
	schema *ice.Schema,
	snapshotID int64,
	seqNum int64,
	dataFilePath string,
	rowCount int64,
	fileSize int64,
) ([]byte, ice.ManifestFile, error) {
	// Build DataFile
	dfBuilder, err := ice.NewDataFileBuilder(
		*ice.UnpartitionedSpec,
		ice.EntryContentData,
		dataFilePath,
		ice.ParquetFile,
		nil, nil, nil,
		rowCount,
		fileSize,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("build data file: %w", err)
	}
	df := dfBuilder.Build()

	// Build ManifestEntry
	entry := ice.NewManifestEntryBuilder(
		ice.EntryStatusADDED,
		&snapshotID,
		df,
	).
		SequenceNum(seqNum).
		FileSequenceNum(seqNum).
		Build()

	// Write manifest
	buf := new(bytes.Buffer)
	mf, err := ice.WriteManifest(filename, buf, 2, *ice.UnpartitionedSpec, schema, snapshotID, []ice.ManifestEntry{entry})
	if err != nil {
		return nil, nil, fmt.Errorf("write manifest: %w", err)
	}

	return buf.Bytes(), mf, nil
}

// writeEqDeleteManifestAvro writes an Avro manifest file that tracks one
// equality-delete file. Iceberg v2 requires data and delete entries to be
// in separate manifest files (content=DELETES at the manifest-file level),
// so this is a sibling of writeManifestAvro rather than a shared path.
//
// equalityFieldIDs lists the Iceberg field IDs of the columns present in
// the delete parquet file — readers match rows whose values in those
// columns equal any row in the delete file and drop them.
func writeEqDeleteManifestAvro(
	filename string,
	schema *ice.Schema,
	snapshotID int64,
	seqNum int64,
	deleteFilePath string,
	rowCount int64,
	fileSize int64,
	equalityFieldIDs []int,
) ([]byte, ice.ManifestFile, error) {
	dfBuilder, err := ice.NewDataFileBuilder(
		*ice.UnpartitionedSpec,
		ice.EntryContentEqDeletes,
		deleteFilePath,
		ice.ParquetFile,
		nil, nil, nil,
		rowCount,
		fileSize,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("build eq-delete file: %w", err)
	}
	df := dfBuilder.EqualityFieldIDs(equalityFieldIDs).Build()

	entry := ice.NewManifestEntryBuilder(
		ice.EntryStatusADDED,
		&snapshotID,
		df,
	).
		SequenceNum(seqNum).
		FileSequenceNum(seqNum).
		Build()

	buf := new(bytes.Buffer)
	mf, err := ice.WriteManifest(filename, buf, 2, *ice.UnpartitionedSpec, schema, snapshotID, []ice.ManifestEntry{entry})
	if err != nil {
		return nil, nil, fmt.Errorf("write eq-delete manifest: %w", err)
	}
	return buf.Bytes(), mf, nil
}

// writeManifestListAvro writes an Avro manifest list using iceberg-go's WriteManifestList.
func writeManifestListAvro(snapshotID int64, seqNum int64, parentSnapshotID *int64, files []ice.ManifestFile) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := ice.WriteManifestList(2, buf, snapshotID, parentSnapshotID, &seqNum, 0, files)
	if err != nil {
		return nil, fmt.Errorf("write manifest list: %w", err)
	}
	return buf.Bytes(), nil
}

// readManifestListAvro reads a manifest list Avro file and returns ManifestFile objects.
func readManifestListAvro(data []byte) ([]ice.ManifestFile, error) {
	return ice.ReadManifestList(bytes.NewReader(data))
}
