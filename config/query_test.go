package config

import (
	"strings"
	"testing"
)

func TestValidateQueryAllowsHTTPOnly(t *testing.T) {
	cfg := Default()
	cfg.S3Bucket = "demo"
	cfg.HTTPQueryAddr = ":8080"
	if err := cfg.ValidateQuery(); err != nil {
		t.Fatalf("ValidateQuery: %v", err)
	}
}

func TestValidateQueryRejectsTwoListeners(t *testing.T) {
	cfg := Default()
	cfg.S3Bucket = "demo"
	cfg.QueryAddr = ":5433"
	cfg.HTTPQueryAddr = ":8080"
	if err := cfg.ValidateQuery(); err == nil || !strings.Contains(err.Error(), "cannot be used together") {
		t.Fatalf("ValidateQuery error = %v", err)
	}
}

func TestValidateQueryRejectsTinyMemoryLimit(t *testing.T) {
	cfg := Default()
	cfg.S3Bucket = "demo"
	cfg.HTTPQueryAddr = ":8080"
	cfg.QueryMemoryLimitMB = 32
	if err := cfg.ValidateQuery(); err == nil || !strings.Contains(err.Error(), "at least 64") {
		t.Fatalf("ValidateQuery error = %v", err)
	}
}

func TestLoadHTTPQueryEnvironment(t *testing.T) {
	t.Setenv("STREAMBED_HTTP_QUERY_ADDR", ":8080")
	t.Setenv("STREAMBED_QUERY_MEMORY_LIMIT_MB", "128")
	cfg := Load()
	if cfg.HTTPQueryAddr != ":8080" || cfg.QueryMemoryLimitMB != 128 {
		t.Fatalf("Load() = HTTPQueryAddr %q, memory %d", cfg.HTTPQueryAddr, cfg.QueryMemoryLimitMB)
	}
}
