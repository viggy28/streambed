package failpoint

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDisabledFailpointIsNoop(t *testing.T) {
	DisableAll()
	if err := Check(context.Background(), "missing"); err != nil {
		t.Fatalf("disabled failpoint returned error: %v", err)
	}
}

func TestErrorFailpoint(t *testing.T) {
	DisableAll()
	t.Cleanup(DisableAll)
	want := errors.New("boom")
	EnableError("before_parquet_write", want)
	if err := Check(context.Background(), "before_parquet_write"); !errors.Is(err, want) {
		t.Fatalf("Check error = %v, want %v", err, want)
	}
}

func TestBlockingFailpointCanBeReleased(t *testing.T) {
	DisableAll()
	t.Cleanup(DisableAll)
	h := EnableBlock("before_standby_status_update")
	done := make(chan error, 1)
	go func() { done <- Check(context.Background(), "before_standby_status_update") }()

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.WaitHit(waitCtx); err != nil {
		t.Fatalf("wait hit: %v", err)
	}

	select {
	case err := <-done:
		t.Fatalf("failpoint returned before release: %v", err)
	default:
	}
	h.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Check after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("failpoint did not release")
	}
}
