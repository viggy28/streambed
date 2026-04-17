package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestRetryWithBackoff_TransientThenSuccess verifies that transient errors
// are retried and eventual success is returned.
func TestRetryWithBackoff_TransientThenSuccess(t *testing.T) {
	callCount := 0
	failUntil := 3

	err := retryWithBackoff(context.Background(), func() error {
		callCount++
		if callCount <= failUntil {
			// Return a retryable error (net.Error with Timeout).
			return &testNetError{timeout: true}
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if callCount != failUntil+1 {
		t.Errorf("expected %d calls, got %d", failUntil+1, callCount)
	}
}

// TestRetryWithBackoff_PermanentFailure verifies that after exhausting all
// retries, the last error is returned.
func TestRetryWithBackoff_PermanentFailure(t *testing.T) {
	callCount := 0

	err := retryWithBackoff(context.Background(), func() error {
		callCount++
		return &testNetError{timeout: true}
	})

	if err == nil {
		t.Fatal("expected error after all retries exhausted")
	}
	// maxRetries + 1 (initial attempt + retries)
	expectedCalls := maxRetries + 1
	if callCount != expectedCalls {
		t.Errorf("expected %d calls, got %d", expectedCalls, callCount)
	}
}

// TestRetryWithBackoff_ContextCancellation verifies that context cancellation
// stops retries immediately.
func TestRetryWithBackoff_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	callCount := 0

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := retryWithBackoff(ctx, func() error {
		callCount++
		return &testNetError{timeout: true}
	})

	if err == nil {
		t.Fatal("expected context error")
	}
	if err != context.Canceled {
		t.Logf("got error: %v (acceptable if wrapping context.Canceled)", err)
	}
	// Should have been cancelled quickly, not all retries exhausted.
	if callCount >= maxRetries+1 {
		t.Errorf("expected fewer than %d calls due to cancellation, got %d", maxRetries+1, callCount)
	}
}

// TestRetryWithBackoff_NonRetryableError verifies that non-retryable errors
// are returned immediately without retrying.
func TestRetryWithBackoff_NonRetryableError(t *testing.T) {
	callCount := 0

	err := retryWithBackoff(context.Background(), func() error {
		callCount++
		return fmt.Errorf("permission denied")
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if callCount != 1 {
		t.Errorf("expected 1 call (no retry for non-retryable), got %d", callCount)
	}
}

// TestRetryWithBackoff_ImmediateSuccess verifies that successful ops are not retried.
func TestRetryWithBackoff_ImmediateSuccess(t *testing.T) {
	callCount := 0

	err := retryWithBackoff(context.Background(), func() error {
		callCount++
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}
}

// TestIsRetryable verifies the classification of retryable vs non-retryable errors.
func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"plain error", fmt.Errorf("something"), false},
		{"net timeout", &testNetError{timeout: true}, true},
		{"net temporary", &testNetError{temporary: true}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRetryable(tt.err)
			if got != tt.retryable {
				t.Errorf("isRetryable(%v) = %v, want %v", tt.err, got, tt.retryable)
			}
		})
	}
}

// testNetError implements net.Error for testing retry logic.
type testNetError struct {
	timeout   bool
	temporary bool
}

func (e *testNetError) Error() string   { return "test network error" }
func (e *testNetError) Timeout() bool   { return e.timeout }
func (e *testNetError) Temporary() bool { return e.temporary }
