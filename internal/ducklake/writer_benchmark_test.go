package ducklake

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/viggy28/streambed/internal/wal"
)

func BenchmarkWriterOperationMatrix(b *testing.B) {
	cases := []struct {
		op     string
		rows   int
		tables int
	}{
		{"insert", 100, 1},
		{"insert", 1000, 1},
		{"insert", 5000, 1},
		{"insert", 1000, 4},
		{"update", 100, 1},
		{"update", 1000, 1},
		{"update", 5000, 1},
		{"update", 1000, 4},
		{"delete", 100, 1},
		{"delete", 1000, 1},
		{"delete", 5000, 1},
		{"delete", 1000, 4},
	}
	for _, tc := range cases {
		name := fmt.Sprintf("%s/rows=%d/tables=%d", tc.op, tc.rows, tc.tables)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				benchDuckLakeOperation(b, tc.op, tc.rows, tc.tables)
			}
		})
	}
}

func benchDuckLakeOperation(b *testing.B, op string, rows, tables int) {
	b.Helper()
	ctx := context.Background()
	dir := b.TempDir()
	w, err := NewWriter(ctx, Config{
		CatalogPath: filepath.Join(dir, "catalog.sqlite"),
		DataPath:    filepath.Join(dir, "data") + "/",
	}, nil, rows*tables+1, time.Second, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	if err != nil {
		b.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()
	cols := []wal.Column{
		{Name: "id", OID: 23, IsKey: true},
		{Name: "name", OID: 25},
		{Name: "amount", OID: 20},
	}
	if op == "update" || op == "delete" {
		for table := 0; table < tables; table++ {
			for row := 0; row < rows; row++ {
				if _, err := w.HandleEvent(ctx, insertEvent(table, row, cols, mustBenchLSN(1))); err != nil {
					b.Fatalf("seed insert: %v", err)
				}
			}
		}
		if err := w.FlushAll(ctx); err != nil {
			b.Fatalf("seed flush: %v", err)
		}
	}
	b.ResetTimer()
	switch op {
	case "insert":
		for table := 0; table < tables; table++ {
			for row := 0; row < rows; row++ {
				if _, err := w.HandleEvent(ctx, insertEvent(table, row, cols, mustBenchLSN(2))); err != nil {
					b.Fatalf("insert event: %v", err)
				}
			}
		}
	case "update":
		for table := 0; table < tables; table++ {
			for row := 0; row < rows; row++ {
				if _, err := w.HandleEvent(ctx, updateEvent(table, row, cols, mustBenchLSN(3))); err != nil {
					b.Fatalf("update event: %v", err)
				}
			}
		}
	case "delete":
		for table := 0; table < tables; table++ {
			for row := 0; row < rows; row++ {
				if _, err := w.HandleEvent(ctx, deleteEvent(table, row, cols, mustBenchLSN(4))); err != nil {
					b.Fatalf("delete event: %v", err)
				}
			}
		}
	}
	if err := w.FlushAll(ctx); err != nil {
		b.Fatalf("flush: %v", err)
	}
	b.StopTimer()
	b.ReportMetric(float64(rows*tables), "rows")
	b.ReportMetric(float64(tables), "tables")
}

func insertEvent(table, row int, cols []wal.Column, lsn pglogrepl.LSN) wal.RowEvent {
	return wal.RowEvent{
		Schema:     "public",
		Table:      fmt.Sprintf("bench_%d", table),
		Columns:    cols,
		KeyColumns: []int{0},
		Op:         wal.OpInsert,
		Values: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte(fmt.Sprintf("%d", row+1))},
			{Name: "name", OID: 25, Value: []byte(fmt.Sprintf("name_%d", row))},
			{Name: "amount", OID: 20, Value: []byte(fmt.Sprintf("%d", row*10))},
		},
		WALStartLSN: lsn,
	}
}

func updateEvent(table, row int, cols []wal.Column, lsn pglogrepl.LSN) wal.RowEvent {
	return wal.RowEvent{
		Schema:     "public",
		Table:      fmt.Sprintf("bench_%d", table),
		Columns:    cols,
		KeyColumns: []int{0},
		Op:         wal.OpUpdate,
		OldKey:     []wal.ColumnValue{{Name: "id", OID: 23, Value: []byte(fmt.Sprintf("%d", row+1))}},
		Values: []wal.ColumnValue{
			{Name: "id", OID: 23, Value: []byte(fmt.Sprintf("%d", row+1))},
			{Name: "name", OID: 25, Value: []byte(fmt.Sprintf("updated_%d", row))},
			{Name: "amount", OID: 20, Value: []byte(fmt.Sprintf("%d", row*20))},
		},
		WALStartLSN: lsn,
	}
}

func deleteEvent(table, row int, cols []wal.Column, lsn pglogrepl.LSN) wal.RowEvent {
	return wal.RowEvent{
		Schema:      "public",
		Table:       fmt.Sprintf("bench_%d", table),
		Columns:     cols,
		KeyColumns:  []int{0},
		Op:          wal.OpDelete,
		OldKey:      []wal.ColumnValue{{Name: "id", OID: 23, Value: []byte(fmt.Sprintf("%d", row+1))}},
		WALStartLSN: lsn,
	}
}

func mustBenchLSN(n int) pglogrepl.LSN {
	return pglogrepl.LSN(n)
}
