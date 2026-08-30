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
	if cfg.FlushInterval != 2*time.Second {
		t.Errorf("expected FlushInterval 2s, got %v", cfg.FlushInterval)
	}
	if cfg.TargetFileSizeMB != 128 {
		t.Errorf("expected TargetFileSizeMB 128, got %d", cfg.TargetFileSizeMB)
	}
	if cfg.LogLevel != "INFO" {
		t.Errorf("expected LogLevel 'INFO', got %q", cfg.LogLevel)
	}
	if cfg.MutationMode != "cow" {
		t.Errorf("expected MutationMode 'cow', got %q", cfg.MutationMode)
	}
	if cfg.TargetFormat != "iceberg" {
		t.Errorf("expected TargetFormat 'iceberg', got %q", cfg.TargetFormat)
	}
	if cfg.DuckLakeCatalog == "" {
		t.Error("expected DuckLakeCatalog default")
	}
	if cfg.DuckLakeCatalogStore != "sqlite" {
		t.Errorf("expected DuckLakeCatalogStore 'sqlite', got %q", cfg.DuckLakeCatalogStore)
	}
}

func TestLoadFromEnv(t *testing.T) {
	os.Setenv("STREAMBED_SOURCE_URL", "postgres://localhost/test")
	os.Setenv("STREAMBED_S3_BUCKET", "my-bucket")
	os.Setenv("STREAMBED_S3_PREFIX", "data/")
	os.Setenv("STREAMBED_FLUSH_ROWS", "5000")
	os.Setenv("STREAMBED_FLUSH_INTERVAL_SEC", "10")
	os.Setenv("STREAMBED_TARGET_FILE_SIZE_MB", "64")
	os.Setenv("STREAMBED_INCLUDE_TABLES", "public.orders, public.users")
	os.Setenv("STREAMBED_LOG_LEVEL", "debug")
	os.Setenv("STREAMBED_MUTATION_MODE", "mor")
	os.Setenv("STREAMBED_TARGET_FORMAT", "ducklake")
	os.Setenv("STREAMBED_DUCKLAKE_CATALOG", "/tmp/streambed-ducklake.ducklake")
	os.Setenv("STREAMBED_DUCKLAKE_CATALOG_STORE", "duckdb")
	os.Setenv("STREAMBED_DUCKLAKE_DATA_PATH", "s3://my-bucket/data/ducklake")
	defer func() {
		os.Unsetenv("STREAMBED_SOURCE_URL")
		os.Unsetenv("STREAMBED_S3_BUCKET")
		os.Unsetenv("STREAMBED_S3_PREFIX")
		os.Unsetenv("STREAMBED_FLUSH_ROWS")
		os.Unsetenv("STREAMBED_FLUSH_INTERVAL_SEC")
		os.Unsetenv("STREAMBED_TARGET_FILE_SIZE_MB")
		os.Unsetenv("STREAMBED_INCLUDE_TABLES")
		os.Unsetenv("STREAMBED_LOG_LEVEL")
		os.Unsetenv("STREAMBED_MUTATION_MODE")
		os.Unsetenv("STREAMBED_TARGET_FORMAT")
		os.Unsetenv("STREAMBED_DUCKLAKE_CATALOG")
		os.Unsetenv("STREAMBED_DUCKLAKE_CATALOG_STORE")
		os.Unsetenv("STREAMBED_DUCKLAKE_DATA_PATH")
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
	if cfg.TargetFileSizeMB != 64 {
		t.Errorf("expected TargetFileSizeMB 64, got %d", cfg.TargetFileSizeMB)
	}
	if len(cfg.IncludeTables) != 2 || cfg.IncludeTables[0] != "public.orders" || cfg.IncludeTables[1] != "public.users" {
		t.Errorf("expected IncludeTables [public.orders, public.users], got %v", cfg.IncludeTables)
	}
	if cfg.LogLevel != "DEBUG" {
		t.Errorf("expected LogLevel 'DEBUG', got %q", cfg.LogLevel)
	}
	if cfg.MutationMode != "mor" {
		t.Errorf("expected MutationMode 'mor', got %q", cfg.MutationMode)
	}
	if cfg.TargetFormat != "ducklake" {
		t.Errorf("expected TargetFormat 'ducklake', got %q", cfg.TargetFormat)
	}
	if cfg.DuckLakeCatalog != "/tmp/streambed-ducklake.ducklake" {
		t.Errorf("expected DuckLakeCatalog from env, got %q", cfg.DuckLakeCatalog)
	}
	if cfg.DuckLakeCatalogStore != "duckdb" {
		t.Errorf("expected DuckLakeCatalogStore from env, got %q", cfg.DuckLakeCatalogStore)
	}
	if got := cfg.EffectiveDuckLakeDataPath(); got != "s3://my-bucket/data/ducklake/" {
		t.Errorf("expected normalized DuckLake data path, got %q", got)
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
		{"invalid mutation mode", func(c *Config) { c.MutationMode = "invalid" }, "mutation-mode must be one of"},
		{"mor mutation mode", func(c *Config) { c.MutationMode = "mor" }, ""},
		{"invalid target format", func(c *Config) { c.TargetFormat = "delta" }, "target-format must be one of"},
		{"ducklake missing catalog", func(c *Config) {
			c.TargetFormat = "ducklake"
			c.DuckLakeCatalog = ""
		}, "ducklake-catalog is required"},
		{"invalid ducklake catalog store", func(c *Config) { c.DuckLakeCatalogStore = "mysql" }, "ducklake-catalog-store must be one of"},
		{"valid duckdb catalog store", func(c *Config) { c.DuckLakeCatalogStore = "duckdb" }, ""},
		{"valid config", func(c *Config) {}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				SourceURL:            "postgres://localhost/test",
				S3Bucket:             "bucket",
				FlushRows:            10000,
				FlushInterval:        30 * time.Second,
				TargetFileSizeMB:     128,
				TargetFormat:         "iceberg",
				DuckLakeCatalog:      "/tmp/ducklake.sqlite",
				DuckLakeCatalogStore: "sqlite",
				MutationMode:         "cow",
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
