package main

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/viggy28/streambed/internal/ducklake"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/wal"
)

func TestRunSnapshotsDuckLake(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	catalogPath := filepath.Join(dir, "catalog.ducklake")
	dataPath := filepath.Join(dir, "data") + "/"

	writer, err := ducklake.NewWriter(ctx, ducklake.Config{
		CatalogPath: catalogPath, CatalogStore: "duckdb", DataPath: dataPath,
	}, nil, 100, time.Hour, slog.Default())
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	_, err = writer.HandleEvent(ctx, wal.RowEvent{
		Schema: "public", Table: "orders", Op: wal.OpInsert,
		Columns:    []wal.Column{{Name: "id", OID: 23, IsKey: true}},
		KeyColumns: []int{0},
		Values:     []wal.ColumnValue{{Name: "id", OID: 23, Value: []byte("1")}},
	})
	if err != nil {
		t.Fatalf("handle event: %v", err)
	}
	if err := writer.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	cmd := &cobra.Command{}
	cmd.Flags().String("target-format", "", "")
	cmd.Flags().String("ducklake-catalog", "", "")
	cmd.Flags().String("ducklake-catalog-store", "", "")
	cmd.Flags().String("ducklake-data-path", "", "")
	for name, value := range map[string]string{
		"target-format":          "ducklake",
		"ducklake-catalog":       catalogPath,
		"ducklake-catalog-store": "duckdb",
		"ducklake-data-path":     dataPath,
	} {
		if err := cmd.Flags().Set(name, value); err != nil {
			t.Fatalf("set %s: %v", name, err)
		}
	}
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := runSnapshots(cmd, "public.orders"); err != nil {
		t.Fatalf("runSnapshots: %v", err)
	}
	got := out.String()
	for _, want := range []string{"SNAPSHOT_ID", "TIMESTAMP_UTC", "SCHEMA_VERSION", "flush public.orders"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
}

func TestWriteIcebergSnapshots(t *testing.T) {
	var out bytes.Buffer
	writeIcebergSnapshots(&out, []iceberg.SnapshotInfo{{
		SnapshotID: 42, SequenceNumber: 7,
		Timestamp: time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
		Summary: map[string]string{
			"operation":                "overwrite",
			"streambed.last_flush_lsn": "0/20",
		},
	}})
	got := out.String()
	for _, want := range []string{"SEQUENCE_NUMBER", "42", "2026-08-10T12:00:00Z", "overwrite", "0/20"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRunSnapshotsRequiresQualifiedTable(t *testing.T) {
	cmd := &cobra.Command{}
	if err := runSnapshots(cmd, "orders"); err == nil || err.Error() != "--table schema.table is required" {
		t.Fatalf("got error %v", err)
	}
}
