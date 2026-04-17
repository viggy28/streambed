package iceberg

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

// TestComputePendingMinLSN_MultipleBuffers verifies that ComputePendingMinLSN
// returns the smallest FirstLSN across all non-empty buffers.
func TestComputePendingMinLSN_MultipleBuffers(t *testing.T) {
	w := &Writer{
		buffers: map[string]*tableBuffer{
			"public.orders": {
				FirstLSN: pglogrepl.LSN(1000),
			},
			"public.users": {
				FirstLSN: pglogrepl.LSN(500),
			},
			"public.products": {
				FirstLSN: pglogrepl.LSN(2000),
			},
		},
	}

	got := w.ComputePendingMinLSN()
	want := pglogrepl.LSN(500)
	if got != want {
		t.Errorf("ComputePendingMinLSN = %s, want %s", got, want)
	}
}

// TestComputePendingMinLSN_EmptyBuffers verifies that ComputePendingMinLSN
// returns 0 when all buffers are empty (FirstLSN == 0).
func TestComputePendingMinLSN_EmptyBuffers(t *testing.T) {
	w := &Writer{
		buffers: map[string]*tableBuffer{
			"public.orders":   {FirstLSN: 0},
			"public.users":    {FirstLSN: 0},
			"public.products": {FirstLSN: 0},
		},
	}

	got := w.ComputePendingMinLSN()
	if got != 0 {
		t.Errorf("ComputePendingMinLSN = %s, want 0 (all empty)", got)
	}
}

// TestComputePendingMinLSN_NoBuffers verifies that ComputePendingMinLSN
// returns 0 when there are no buffers at all.
func TestComputePendingMinLSN_NoBuffers(t *testing.T) {
	w := &Writer{
		buffers: map[string]*tableBuffer{},
	}

	got := w.ComputePendingMinLSN()
	if got != 0 {
		t.Errorf("ComputePendingMinLSN = %s, want 0 (no buffers)", got)
	}
}

// TestComputePendingMinLSN_AfterPartialFlush verifies that after flushing one
// table (setting its FirstLSN to 0), the minimum updates correctly to the
// next non-empty buffer.
func TestComputePendingMinLSN_AfterPartialFlush(t *testing.T) {
	w := &Writer{
		buffers: map[string]*tableBuffer{
			"public.orders": {
				FirstLSN: pglogrepl.LSN(100),
			},
			"public.users": {
				FirstLSN: pglogrepl.LSN(200),
			},
			"public.products": {
				FirstLSN: pglogrepl.LSN(300),
			},
		},
	}

	// Before flush: min should be 100.
	got := w.ComputePendingMinLSN()
	if got != pglogrepl.LSN(100) {
		t.Fatalf("before flush: ComputePendingMinLSN = %s, want 0/64", got)
	}

	// Simulate flushing "orders" (the smallest).
	w.buffers["public.orders"].FirstLSN = 0

	// After flush: min should now be 200 (users).
	got = w.ComputePendingMinLSN()
	if got != pglogrepl.LSN(200) {
		t.Errorf("after partial flush: ComputePendingMinLSN = %s, want 0/C8", got)
	}

	// Flush users too.
	w.buffers["public.users"].FirstLSN = 0

	got = w.ComputePendingMinLSN()
	if got != pglogrepl.LSN(300) {
		t.Errorf("after second flush: ComputePendingMinLSN = %s, want 0/12C", got)
	}

	// Flush all.
	w.buffers["public.products"].FirstLSN = 0

	got = w.ComputePendingMinLSN()
	if got != 0 {
		t.Errorf("after all flushed: ComputePendingMinLSN = %s, want 0", got)
	}
}

// TestComputePendingMinLSN_MixedEmptyNonEmpty verifies correct behavior when
// some buffers are empty and some are not.
func TestComputePendingMinLSN_MixedEmptyNonEmpty(t *testing.T) {
	w := &Writer{
		buffers: map[string]*tableBuffer{
			"public.orders":   {FirstLSN: 0},                     // empty
			"public.users":    {FirstLSN: pglogrepl.LSN(999)},    // non-empty
			"public.products": {FirstLSN: 0},                     // empty
			"public.logs":     {FirstLSN: pglogrepl.LSN(888)},    // non-empty
		},
	}

	got := w.ComputePendingMinLSN()
	if got != pglogrepl.LSN(888) {
		t.Errorf("mixed: ComputePendingMinLSN = %s, want 0/378", got)
	}
}
