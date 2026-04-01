package config

import (
	"os"
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	cfg := Default()
	if cfg.S3Prefix != "streambed/" {
		t.Errorf("expected S3Prefix 'streambed/', got %q", cfg.S3Prefix)
	}
	if cfg.S3Region != "us-east-1" {
		t.Errorf("expected S3Region 'us-east-1', got %q", cfg.S3Region)
	}
	if cfg.SlotName != "streambed" {
		t.Errorf("expected SlotName 'streambed', got %q", cfg.SlotName)
	}
	if cfg.FlushRows != 10000 {
		t.Errorf("expected FlushRows 10000, got %d", cfg.FlushRows)
	}
	if cfg.FlushInterval != 30*time.Second {
		t.Errorf("expected FlushInterval 30s, got %v", cfg.FlushInterval)
	}
	if cfg.LogLevel != "INFO" {
		t.Errorf("expected LogLevel 'INFO', got %q", cfg.LogLevel)
	}
}

func TestLoadFromEnv(t *testing.T) {
	os.Setenv("STREAMBED_SOURCE_URL", "postgres://localhost/test")
	os.Setenv("STREAMBED_S3_BUCKET", "my-bucket")
	os.Setenv("STREAMBED_S3_PREFIX", "data/")
	os.Setenv("STREAMBED_FLUSH_ROWS", "5000")
	os.Setenv("STREAMBED_FLUSH_INTERVAL_SEC", "10")
	os.Setenv("STREAMBED_INCLUDE_TABLES", "public.orders, public.users")
	os.Setenv("STREAMBED_LOG_LEVEL", "debug")
	defer func() {
		os.Unsetenv("STREAMBED_SOURCE_URL")
		os.Unsetenv("STREAMBED_S3_BUCKET")
		os.Unsetenv("STREAMBED_S3_PREFIX")
		os.Unsetenv("STREAMBED_FLUSH_ROWS")
		os.Unsetenv("STREAMBED_FLUSH_INTERVAL_SEC")
		os.Unsetenv("STREAMBED_INCLUDE_TABLES")
		os.Unsetenv("STREAMBED_LOG_LEVEL")
	}()

	cfg := Load()
	if cfg.SourceURL != "postgres://localhost/test" {
		t.Errorf("expected SourceURL from env, got %q", cfg.SourceURL)
	}
	if cfg.S3Bucket != "my-bucket" {
		t.Errorf("expected S3Bucket from env, got %q", cfg.S3Bucket)
	}
	if cfg.S3Prefix != "data/" {
		t.Errorf("expected S3Prefix 'data/', got %q", cfg.S3Prefix)
	}
	if cfg.FlushRows != 5000 {
		t.Errorf("expected FlushRows 5000, got %d", cfg.FlushRows)
	}
	if cfg.FlushInterval != 10*time.Second {
		t.Errorf("expected FlushInterval 10s, got %v", cfg.FlushInterval)
	}
	if len(cfg.IncludeTables) != 2 || cfg.IncludeTables[0] != "public.orders" || cfg.IncludeTables[1] != "public.users" {
		t.Errorf("expected IncludeTables [public.orders, public.users], got %v", cfg.IncludeTables)
	}
	if cfg.LogLevel != "DEBUG" {
		t.Errorf("expected LogLevel 'DEBUG', got %q", cfg.LogLevel)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{"missing source-url", func(c *Config) { c.SourceURL = "" }, "source-url is required"},
		{"missing s3-bucket", func(c *Config) { c.S3Bucket = "" }, "s3-bucket is required"},
		{"both include and exclude", func(c *Config) {
			c.IncludeTables = []string{"a"}
			c.ExcludeTables = []string{"b"}
		}, "cannot use both"},
		{"valid config", func(c *Config) {}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				SourceURL:     "postgres://localhost/test",
				S3Bucket:      "bucket",
				FlushRows:     10000,
				FlushInterval: 30 * time.Second,
			}
			tt.modify(cfg)
			err := cfg.Validate()
			if tt.wantErr == "" && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if tt.wantErr != "" && (err == nil || !contains(err.Error(), tt.wantErr)) {
				t.Errorf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && searchString(s, sub)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
