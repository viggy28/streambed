package pipeline

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/viggy28/streambed/internal/wal"
)

func TestCollectAddColumns(t *testing.T) {
	tests := []struct {
		name    string
		changes []wal.SchemaChange
		want    []string
	}{
		{
			name:    "no changes",
			changes: nil,
			want:    nil,
		},
		{
			name: "only ADD",
			changes: []wal.SchemaChange{
				{Type: wal.SchemaChangeAdd, Column: "email"},
				{Type: wal.SchemaChangeAdd, Column: "phone"},
			},
			want: []string{"email", "phone"},
		},
		{
			name: "mixed types",
			changes: []wal.SchemaChange{
				{Type: wal.SchemaChangeDrop, Column: "old_col"},
				{Type: wal.SchemaChangeAdd, Column: "new_col"},
				{Type: wal.SchemaChangeTypeChange, Column: "changed_col"},
				{Type: wal.SchemaChangeAdd, Column: "another_new"},
			},
			want: []string{"new_col", "another_new"},
		},
		{
			name: "no ADD columns",
			changes: []wal.SchemaChange{
				{Type: wal.SchemaChangeDrop, Column: "removed"},
				{Type: wal.SchemaChangeTypeChange, Column: "widened"},
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collectAddColumns(tt.changes)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d columns, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("column %d: got %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestShouldDropBackfillOverlap(t *testing.T) {
	filter := pglogrepl.LSN(1000)
	tests := []struct {
		name  string
		event pglogrepl.LSN
		want  bool
	}{
		{name: "before snapshot boundary drops", event: 999, want: true},
		{name: "at snapshot boundary replays", event: 1000, want: false},
		{name: "after snapshot boundary replays", event: 1001, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldDropBackfillOverlap(tt.event, filter); got != tt.want {
				t.Fatalf("shouldDropBackfillOverlap(%s, %s) = %v, want %v", tt.event, filter, got, tt.want)
			}
		})
	}
}

func TestComputeAck(t *testing.T) {
	tests := []struct {
		name          string
		receivedLSN   pglogrepl.LSN
		pendingMinLSN pglogrepl.LSN
		want          pglogrepl.LSN
	}{
		{
			name:          "no pending buffers",
			receivedLSN:   pglogrepl.LSN(1000),
			pendingMinLSN: 0,
			want:          pglogrepl.LSN(1000),
		},
		{
			name:          "pending below received",
			receivedLSN:   pglogrepl.LSN(1000),
			pendingMinLSN: pglogrepl.LSN(500),
			want:          pglogrepl.LSN(499), // pendingMinLSN - 1
		},
		{
			name:          "pending above received",
			receivedLSN:   pglogrepl.LSN(100),
			pendingMinLSN: pglogrepl.LSN(500),
			want:          pglogrepl.LSN(100), // receivedLSN is smaller
		},
		{
			name:          "pending equals received",
			receivedLSN:   pglogrepl.LSN(500),
			pendingMinLSN: pglogrepl.LSN(500),
			want:          pglogrepl.LSN(499), // pendingMinLSN - 1
		},
		{
			name:          "pending min LSN is 1",
			receivedLSN:   pglogrepl.LSN(1000),
			pendingMinLSN: pglogrepl.LSN(1),
			want:          pglogrepl.LSN(0), // pendingMinLSN - 1 = 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeAck(tt.receivedLSN, tt.pendingMinLSN)
			if got != tt.want {
				t.Errorf("computeAck(%s, %s) = %s, want %s",
					tt.receivedLSN, tt.pendingMinLSN, got, tt.want)
			}
		})
	}
}
