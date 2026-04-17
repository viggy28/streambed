package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// FaultS3Client wraps an ObjectStorage and injects failures for testing.
// It can fail PutObject calls based on key patterns or call counts.
type FaultS3Client struct {
	Inner ObjectStorage

	mu             sync.Mutex
	failOnKeyMatch string // fail PutObject when key contains this substring
	failAfterN     int    // fail after N successful PutObject calls (-1 = never)
	failErr        error  // the error to return
	putCount       int    // total PutObject calls made
}

// NewFaultS3Client creates a fault-injecting wrapper around the given storage.
func NewFaultS3Client(inner ObjectStorage) *FaultS3Client {
	return &FaultS3Client{
		Inner:      inner,
		failAfterN: -1,
		failErr:    fmt.Errorf("injected fault"),
	}
}

// FailOnKeyContaining configures the client to fail PutObject when the key
// contains the given substring.
func (f *FaultS3Client) FailOnKeyContaining(substr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOnKeyMatch = substr
}

// FailAfterNPuts configures the client to fail PutObject after N successful
// calls. Set to -1 to disable.
func (f *FaultS3Client) FailAfterNPuts(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAfterN = n
}

// SetFailError sets the error returned on injected failures.
func (f *FaultS3Client) SetFailError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failErr = err
}

// PutCount returns the total number of PutObject calls made.
func (f *FaultS3Client) PutCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putCount
}

// Reset clears all fault configuration and counters.
func (f *FaultS3Client) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOnKeyMatch = ""
	f.failAfterN = -1
	f.putCount = 0
}

func (f *FaultS3Client) PutObject(ctx context.Context, key string, data []byte, contentType string) error {
	f.mu.Lock()
	f.putCount++
	currentCount := f.putCount
	keyMatch := f.failOnKeyMatch
	afterN := f.failAfterN
	failErr := f.failErr
	f.mu.Unlock()

	// Check key-based fault.
	if keyMatch != "" && strings.Contains(key, keyMatch) {
		return fmt.Errorf("fault injection on key %q: %w", key, failErr)
	}

	// Check count-based fault.
	if afterN >= 0 && currentCount > afterN {
		return fmt.Errorf("fault injection after %d puts (current: %d): %w", afterN, currentCount, failErr)
	}

	return f.Inner.PutObject(ctx, key, data, contentType)
}

func (f *FaultS3Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	return f.Inner.GetObject(ctx, key)
}

func (f *FaultS3Client) HeadObject(ctx context.Context, key string) (bool, error) {
	return f.Inner.HeadObject(ctx, key)
}

func (f *FaultS3Client) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	return f.Inner.ListPrefix(ctx, prefix)
}

func (f *FaultS3Client) DeleteObjects(ctx context.Context, keys []string) error {
	return f.Inner.DeleteObjects(ctx, keys)
}

func (f *FaultS3Client) Bucket() string {
	return f.Inner.Bucket()
}

// Compile-time check.
var _ ObjectStorage = (*FaultS3Client)(nil)
