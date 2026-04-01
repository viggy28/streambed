package iceberg

import (
	"testing"

	ice "github.com/apache/iceberg-go"
)

func TestWriteManifestAvro(t *testing.T) {
	schema := ice.NewSchema(0,
		ice.NestedField{ID: 1, Name: "id", Type: ice.PrimitiveTypes.Int64, Required: true},
		ice.NestedField{ID: 2, Name: "name", Type: ice.PrimitiveTypes.String, Required: false},
	)

	data, mf, err := writeManifestAvro(
		"s3://bucket/prefix/metadata/test-m0.avro",
		schema,
		1234567890,
		1,
		"s3://bucket/prefix/data/test.parquet",
		100,
		4096,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty avro data")
	}
	if mf == nil {
		t.Fatal("expected non-nil ManifestFile")
	}
	if mf.SnapshotID() != 1234567890 {
		t.Errorf("expected snapshot ID 1234567890, got %d", mf.SnapshotID())
	}
}

func TestWriteManifestListAvro(t *testing.T) {
	// First create a manifest to get a ManifestFile
	schema := ice.NewSchema(0,
		ice.NestedField{ID: 1, Name: "id", Type: ice.PrimitiveTypes.Int64, Required: true},
	)

	_, mf, err := writeManifestAvro(
		"s3://bucket/prefix/metadata/test-m0.avro",
		schema,
		1234567890,
		1,
		"s3://bucket/prefix/data/test.parquet",
		100,
		4096,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Write manifest list
	data, err := writeManifestListAvro(1234567890, 1, nil, []ice.ManifestFile{mf})
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty avro data")
	}

	// Read it back using iceberg-go
	files, err := readManifestListAvro(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 manifest file, got %d", len(files))
	}
	if files[0].SnapshotID() != 1234567890 {
		t.Errorf("expected snapshot ID 1234567890, got %d", files[0].SnapshotID())
	}
}

func TestBuildIcebergSchema(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", OID: 20},        // int8 → Int64
		{Name: "name", OID: 25},      // text → String
		{Name: "active", OID: 16},    // bool → Bool
		{Name: "created", OID: 1184}, // timestamptz → TimestampTz
	}

	schema := buildIcebergSchema(cols)
	fields := schema.Fields()

	if len(fields) != 4 {
		t.Fatalf("expected 4 fields, got %d", len(fields))
	}
	if fields[0].Name != "id" || fields[0].ID != 1 {
		t.Errorf("unexpected first field: %+v", fields[0])
	}
	if fields[1].Name != "name" || fields[1].ID != 2 {
		t.Errorf("unexpected second field: %+v", fields[1])
	}
}
