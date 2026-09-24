package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	SourceURL            string
	S3Bucket             string
	S3Prefix             string
	S3Endpoint           string
	S3Region             string
	StatePath            string
	TargetFormat         string // lakehouse target: "iceberg" or "ducklake"
	DuckLakeCatalog      string // catalog DB path for DuckLake metadata
	DuckLakeCatalogStore string // DuckLake catalog store: "duckdb" (default) or "sqlite"
	DuckLakeDataPath     string // DuckLake data path (defaults to s3://bucket/prefix/ducklake/)
	SlotName             string
	FlushRows            int
	FlushInterval        time.Duration
	TargetFileSizeMB     int
	IncludeTables        []string
	ExcludeTables        []string
	LogLevel             string
	MutationMode         string // Iceberg row mutation strategy: "cow" or "mor"
	QueryAddr            string // listen address for query server (e.g., ":5433")
}

func Default() *Config {
	return &Config{
		S3Prefix:             "streambed/",
		S3Region:             "us-east-1",
		StatePath:            defaultStatePath(),
		TargetFormat:         "iceberg",
		DuckLakeCatalog:      defaultDuckLakeCatalogPath(),
		DuckLakeCatalogStore: "duckdb",
		SlotName:             "streambed",
		FlushRows:            10000,
		FlushInterval:        2 * time.Second,
		TargetFileSizeMB:     128,
		LogLevel:             "INFO",
		MutationMode:         "cow",
	}
}

func defaultStatePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".streambed/state.db"
	}
	return home + "/.streambed/state.db"
}

func defaultDuckLakeCatalogPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".streambed/ducklake-catalog.duckdb"
	}
	return home + "/.streambed/ducklake-catalog.duckdb"
}

// Load reads configuration from environment variables.
// CLI flags should override the returned config after calling this.
func Load() *Config {
	cfg := Default()

	if v := os.Getenv("STREAMBED_SOURCE_URL"); v != "" {
		cfg.SourceURL = v
	}
	if v := os.Getenv("STREAMBED_S3_BUCKET"); v != "" {
		cfg.S3Bucket = v
	}
	if v := os.Getenv("STREAMBED_S3_PREFIX"); v != "" {
		cfg.S3Prefix = v
	}
	if v := os.Getenv("STREAMBED_S3_ENDPOINT"); v != "" {
		cfg.S3Endpoint = v
	}
	if v := os.Getenv("STREAMBED_S3_REGION"); v != "" {
		cfg.S3Region = v
	}
	if v := os.Getenv("STREAMBED_STATE_PATH"); v != "" {
		cfg.StatePath = v
	}
	if v := os.Getenv("STREAMBED_TARGET_FORMAT"); v != "" {
		cfg.TargetFormat = strings.ToLower(v)
	}
	if v := os.Getenv("STREAMBED_DUCKLAKE_CATALOG"); v != "" {
		cfg.DuckLakeCatalog = v
	}
	if v := os.Getenv("STREAMBED_DUCKLAKE_CATALOG_STORE"); v != "" {
		cfg.DuckLakeCatalogStore = strings.ToLower(v)
	}
	if v := os.Getenv("STREAMBED_DUCKLAKE_DATA_PATH"); v != "" {
		cfg.DuckLakeDataPath = v
	}
	if v := os.Getenv("STREAMBED_SLOT_NAME"); v != "" {
		cfg.SlotName = v
	}
	if v := os.Getenv("STREAMBED_FLUSH_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.FlushRows = n
		}
	}
	if v := os.Getenv("STREAMBED_FLUSH_INTERVAL_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.FlushInterval = time.Duration(n) * time.Second
		}
	}
	if v := os.Getenv("STREAMBED_TARGET_FILE_SIZE_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.TargetFileSizeMB = n
		}
	}
	if v := os.Getenv("STREAMBED_INCLUDE_TABLES"); v != "" {
		cfg.IncludeTables = splitTables(v)
	}
	if v := os.Getenv("STREAMBED_EXCLUDE_TABLES"); v != "" {
		cfg.ExcludeTables = splitTables(v)
	}
	if v := os.Getenv("STREAMBED_LOG_LEVEL"); v != "" {
		cfg.LogLevel = strings.ToUpper(v)
	}
	if v := os.Getenv("STREAMBED_MUTATION_MODE"); v != "" {
		cfg.MutationMode = strings.ToLower(v)
	}
	if v := os.Getenv("STREAMBED_QUERY_ADDR"); v != "" {
		cfg.QueryAddr = v
	}

	return cfg
}

func splitTables(s string) []string {
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

func (c *Config) Validate() error {
	if c.SourceURL == "" {
		return fmt.Errorf("source-url is required")
	}
	if c.S3Bucket == "" {
		return fmt.Errorf("s3-bucket is required")
	}
	if len(c.IncludeTables) > 0 && len(c.ExcludeTables) > 0 {
		return fmt.Errorf("cannot use both --include-tables and --exclude-tables")
	}
	if c.TargetFormat != "iceberg" && c.TargetFormat != "ducklake" {
		return fmt.Errorf("target-format must be one of: iceberg, ducklake")
	}
	if c.TargetFormat == "ducklake" && c.DuckLakeCatalog == "" {
		return fmt.Errorf("ducklake-catalog is required when target-format=ducklake")
	}
	if c.DuckLakeCatalogStore != "sqlite" && c.DuckLakeCatalogStore != "duckdb" {
		return fmt.Errorf("ducklake-catalog-store must be one of: sqlite, duckdb")
	}
	if c.FlushRows <= 0 {
		return fmt.Errorf("flush-rows must be positive")
	}
	if c.FlushInterval <= 0 {
		return fmt.Errorf("flush-interval must be positive")
	}
	if c.TargetFileSizeMB <= 0 {
		return fmt.Errorf("target-file-size-mb must be positive")
	}
	if c.MutationMode != "cow" && c.MutationMode != "mor" {
		return fmt.Errorf("mutation-mode must be one of: cow, mor")
	}
	return nil
}

// ValidateQuery validates config for query-only mode.
// Does not require source-url since no Postgres connection is needed.
func (c *Config) ValidateQuery() error {
	if c.S3Bucket == "" {
		return fmt.Errorf("s3-bucket is required")
	}
	if c.TargetFormat != "iceberg" && c.TargetFormat != "ducklake" {
		return fmt.Errorf("target-format must be one of: iceberg, ducklake")
	}
	if c.TargetFormat == "ducklake" && c.DuckLakeCatalog == "" {
		return fmt.Errorf("ducklake-catalog is required when target-format=ducklake")
	}
	if c.DuckLakeCatalogStore != "sqlite" && c.DuckLakeCatalogStore != "duckdb" {
		return fmt.Errorf("ducklake-catalog-store must be one of: sqlite, duckdb")
	}
	if c.QueryAddr == "" {
		return fmt.Errorf("listen-addr is required")
	}
	return nil
}

func (c *Config) EffectiveDuckLakeDataPath() string {
	if c.DuckLakeDataPath != "" {
		return ensureTrailingSlash(c.DuckLakeDataPath)
	}
	prefix := strings.Trim(c.S3Prefix, "/")
	if prefix == "" {
		return fmt.Sprintf("s3://%s/ducklake/", c.S3Bucket)
	}
	return fmt.Sprintf("s3://%s/%s/ducklake/", c.S3Bucket, prefix)
}

func ensureTrailingSlash(s string) string {
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}
