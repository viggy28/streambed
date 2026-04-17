package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// MemS3Client is an in-memory ObjectStorage implementation for unit testing.
// It stores objects in a map and requires no external dependencies.
type MemS3Client struct {
	mu      sync.RWMutex
	objects map[string][]byte
	bucket  string
}

// NewMemS3Client creates an in-memory S3 client.
func NewMemS3Client(bucket string) *MemS3Client {
	return &MemS3Client{
		objects: make(map[string][]byte),
		bucket:  bucket,
	}
}

func (m *MemS3Client) PutObject(_ context.Context, key string, data []byte, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.objects[key] = cp
	return nil
}

func (m *MemS3Client) GetObject(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *MemS3Client) HeadObject(_ context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.objects[key]
	return ok, nil
}

func (m *MemS3Client) ListPrefix(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *MemS3Client) DeleteObjects(_ context.Context, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.objects, k)
	}
	return nil
}

func (m *MemS3Client) Bucket() string {
	return m.bucket
}

// Compile-time check.
var _ ObjectStorage = (*MemS3Client)(nil)
