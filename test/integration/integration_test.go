//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	pqgo "github.com/parquet-go/parquet-go"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/storage"
	"github.com/viggy28/streambed/internal/wal"
)

const (
	pgHost     = "localhost"
	pgPort     = "5433"
	pgUser     = "postgres"
	pgPassword = "test"
	pgDB       = "postgres"

	minioEndpoint = "http://localhost:9002"
	s3Bucket      = "streambed"
	s3Region      = "us-east-1"
	s3Prefix      = "test"

	slotName  = "streambed_integration_test"
	flushRows = 500
)

func pgConnStr() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pgUser, pgPassword, pgHost, pgPort, pgDB)
}

func pgReplConnStr() string {
	return pgConnStr() + "?replication=database"
}

func skipIfNotAvailable(t *testing.T) {
	t.Helper()
	// Check Postgres
	conn, err := net.DialTimeout("tcp", pgHost+":"+pgPort, 2*time.Second)
	if err != nil {
		t.Skipf("Postgres not available at %s:%s: %v", pgHost, pgPort, err)
	}
	conn.Close()

	// Check MinIO
	conn, err = net.DialTimeout("tcp", "localhost:9002", 2*time.Second)
	if err != nil {
		t.Skipf("MinIO not available at localhost:9002: %v", err)
	}
	conn.Close()
}

// cleanup drops the replication slot and publication if they exist.
func cleanup(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgReplConnStr())
	if err != nil {
		t.Logf("cleanup: could not connect: %v", err)
		return
	}
	defer conn.Close(ctx)

	// Drop slot
	result := conn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s') FROM pg_replication_slots WHERE slot_name = '%s'", slotName, slotName))
	result.ReadAll()
	result.Close()

	// Drop publication
	result = conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", slotName))
	result.ReadAll()
	result.Close()
}

func setupTestTable(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer conn.Close(ctx)

	sqls := []string{
		"DROP TABLE IF EXISTS test_events",
		`CREATE TABLE test_events (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value DOUBLE PRECISION,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
	}
	for _, sql := range sqls {
		result := conn.Exec(ctx, sql)
		if _, err := result.ReadAll(); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
}

func insertRows(t *testing.T, count int) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer conn.Close(ctx)

	// Insert in batches of 100
	for i := 0; i < count; i += 100 {
		batchSize := 100
		if i+batchSize > count {
			batchSize = count - i
		}
		var sb strings.Builder
		sb.WriteString("INSERT INTO test_events (name, value) VALUES ")
		for j := 0; j < batchSize; j++ {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("('event_%d', %d.%d)", i+j, i+j, (i+j)%100))
		}
		result := conn.Exec(ctx, sb.String())
		if _, err := result.ReadAll(); err != nil {
			t.Fatalf("insert batch at %d: %v", i, err)
		}
	}
}

// runSync runs the streambed sync pipeline for the given duration.
func runSync(t *testing.T, ctx context.Context, duration time.Duration) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// State store
	stateDir := t.TempDir()
	statePath := stateDir + "/state.db"
	stateStore, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open state store: %v", err)
	}
	defer stateStore.Close()

	// S3 client
	s3Client, err := storage.NewS3Client(ctx, s3Bucket, s3Region, minioEndpoint)
	if err != nil {
		t.Fatalf("create S3 client: %v", err)
	}

	// Postgres replication connection
	pgConn, err := pgconn.Connect(ctx, pgReplConnStr())
	if err != nil {
		t.Fatalf("connect to postgres for replication: %v", err)
	}
	defer pgConn.Close(context.Background())

	// Create publication
	if err := wal.CreatePublication(ctx, pgConn, slotName, nil, logger); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	// Create or reuse replication slot
	slotLSN, err := wal.CreateOrReuseSlot(ctx, pgConn, slotName, logger)
	if err != nil {
		t.Fatalf("setup replication slot: %v", err)
	}

	// Check state store for last flushed LSN
	stateLSN, err := stateStore.GetFlushedLSN(slotName)
	if err != nil {
		t.Fatalf("get flushed LSN: %v", err)
	}

	startLSN := slotLSN
	if stateLSN > startLSN {
		startLSN = stateLSN
	}

	// Initialize Iceberg catalog
	catalog := iceberg.NewCatalog(s3Client, s3Bucket, s3Prefix)

	// Initialize writer
	writer := iceberg.NewWriter(catalog, s3Client, stateStore, slotName,
		flushRows, 5*time.Second, logger)

	// Create consumer
	consumer := wal.NewConsumer(pgConn, slotName, slotName, startLSN, nil, logger)

	// Channels
	events := make(chan wal.RowEvent, 1000)
	ackCh := make(chan pglogrepl.LSN, 10)

	// Run with timeout
	syncCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	// Start writer
	writerErrCh := make(chan error, 1)
	go func() {
		writerErrCh <- writer.Start(syncCtx, events, ackCh)
	}()

	// Start consumer (blocks until ctx cancelled)
	consumerErr := consumer.Start(syncCtx, events, ackCh)
	close(events)

	// Wait for writer
	select {
	case writerErr := <-writerErrCh:
		if writerErr != nil {
			t.Logf("writer error: %v", writerErr)
		}
	case <-time.After(10 * time.Second):
		t.Log("writer shutdown timed out")
	}

	if consumerErr != nil && syncCtx.Err() != nil {
		// Expected: context deadline exceeded
		t.Logf("consumer stopped: %v", consumerErr)
	} else if consumerErr != nil {
		t.Fatalf("unexpected consumer error: %v", consumerErr)
	}
}

// countParquetFilesOnS3 lists objects under the test prefix and counts .parquet files.
func countParquetFilesOnS3(t *testing.T) int {
	t.Helper()
	ctx := context.Background()

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(s3Region))
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint)
		o.UsePathStyle = true
		o.Credentials = credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")
	})

	output, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Bucket),
		Prefix: aws.String(s3Prefix + "/"),
	})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	parquetCount := 0
	for _, obj := range output.Contents {
		key := aws.ToString(obj.Key)
		t.Logf("S3 object: %s (size: %d)", key, *obj.Size)
		if strings.HasSuffix(key, ".parquet") {
			parquetCount++
		}
	}
	return parquetCount
}

// readVersionHint reads version-hint.text for the test table.
func readVersionHint(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(s3Region))
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint)
		o.UsePathStyle = true
		o.Credentials = credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")
	})

	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Prefix + "/public/test_events/metadata/version-hint.text"),
	})
	if err != nil {
		return ""
	}
	defer output.Body.Close()
	data, _ := io.ReadAll(output.Body)
	return string(data)
}

// readMetadataJSON reads the metadata JSON for the given version.
func readMetadataJSON(t *testing.T, version string) []byte {
	t.Helper()
	ctx := context.Background()

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(s3Region))
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint)
		o.UsePathStyle = true
		o.Credentials = credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")
	})

	key := fmt.Sprintf("%s/public/test_events/metadata/v%s.metadata.json", s3Prefix, version)
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("read metadata v%s: %v", version, err)
	}
	defer output.Body.Close()
	data, _ := io.ReadAll(output.Body)
	return data
}

// clearS3Prefix deletes all objects under the test prefix.
func clearS3Prefix(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(s3Region))
	if err != nil {
		return
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint)
		o.UsePathStyle = true
		o.Credentials = credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")
	})

	output, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Bucket),
		Prefix: aws.String(s3Prefix + "/"),
	})
	if err != nil {
		return
	}

	for _, obj := range output.Contents {
		client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s3Bucket),
			Key:    obj.Key,
		})
	}
}

// countTotalParquetRows downloads all Parquet files from S3 and counts total rows.
// Also verifies the schema contains expected columns.
func countTotalParquetRows(t *testing.T) int64 {
	t.Helper()
	ctx := context.Background()

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(s3Region))
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint)
		o.UsePathStyle = true
		o.Credentials = credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")
	})

	output, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Bucket),
		Prefix: aws.String(s3Prefix + "/"),
	})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	var totalRows int64
	for _, obj := range output.Contents {
		key := aws.ToString(obj.Key)
		if !strings.HasSuffix(key, ".parquet") {
			continue
		}

		// Download the parquet file
		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s3Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("get parquet file %s: %v", key, err)
		}
		data, err := io.ReadAll(getOut.Body)
		getOut.Body.Close()
		if err != nil {
			t.Fatalf("read parquet file %s: %v", key, err)
		}

		// Open with parquet-go
		reader := bytes.NewReader(data)
		f, err := pqgo.OpenFile(reader, int64(len(data)))
		if err != nil {
			t.Fatalf("open parquet file %s: %v", key, err)
		}

		numRows := f.NumRows()
		t.Logf("parquet file %s: %d rows, %d columns", key, numRows, len(f.Schema().Columns()))

		// Verify schema has expected columns
		colNames := make([]string, 0)
		for _, col := range f.Schema().Columns() {
			colNames = append(colNames, col[0])
		}
		t.Logf("  columns: %v", colNames)

		// We expect: id, name, value, created_at
		if len(colNames) < 4 {
			t.Errorf("expected at least 4 columns, got %d: %v", len(colNames), colNames)
		}

		// Read a sample row to verify data is not empty/corrupt
		pqReader := pqgo.NewReader(reader)
		rows := make([]map[string]interface{}, 0, 1)
		for i := 0; i < 1; i++ {
			row := make(map[string]interface{})
			if err := pqReader.Read(&row); err != nil {
				break
			}
			rows = append(rows, row)
		}
		if len(rows) > 0 {
			t.Logf("  sample row: %v", rows[0])
		}

		totalRows += numRows
	}

	return totalRows
}

// createSlotAndPublication creates the replication slot and publication
// so that rows inserted afterward will be captured.
func createSlotAndPublication(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	conn, err := pgconn.Connect(ctx, pgReplConnStr())
	if err != nil {
		t.Fatalf("connect for slot setup: %v", err)
	}
	defer conn.Close(ctx)

	if err := wal.CreatePublication(ctx, conn, slotName, nil, logger); err != nil {
		t.Fatalf("create publication: %v", err)
	}
	if _, err := wal.CreateOrReuseSlot(ctx, conn, slotName, logger); err != nil {
		t.Fatalf("create slot: %v", err)
	}
}

func TestEndToEnd(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	// Clean state
	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)

	// Create slot BEFORE inserting rows so WAL captures them
	createSlotAndPublication(t)

	// Insert 1000 rows
	t.Log("inserting 1000 rows...")
	insertRows(t, 1000)

	// Run sync for 15 seconds
	t.Log("running sync for 15 seconds...")
	runSync(t, ctx, 15*time.Second)

	// Verify: Parquet files exist on S3
	parquetCount := countParquetFilesOnS3(t)
	if parquetCount == 0 {
		t.Fatal("expected at least 1 parquet file on S3, got 0")
	}
	t.Logf("found %d parquet file(s) on S3", parquetCount)

	// Verify: actual row count in Parquet files
	totalRows := countTotalParquetRows(t)
	if totalRows != 1000 {
		t.Fatalf("expected 1000 total rows in parquet files, got %d", totalRows)
	}
	t.Logf("verified %d rows across %d parquet file(s)", totalRows, parquetCount)

	// Verify: version-hint.text exists and is > 0
	version := readVersionHint(t)
	if version == "" {
		t.Fatal("version-hint.text not found or empty")
	}
	t.Logf("version-hint.text = %s", version)

	// Verify: metadata JSON exists and has snapshots
	metaJSON := readMetadataJSON(t, version)
	if !bytes.Contains(metaJSON, []byte(`"snapshot-id"`)) {
		t.Fatal("metadata JSON does not contain any snapshots")
	}
	if !bytes.Contains(metaJSON, []byte(`"added-records"`)) {
		t.Fatal("metadata JSON does not contain added-records summary")
	}
	t.Logf("metadata JSON looks valid (%d bytes)", len(metaJSON))

	// Cleanup slot before resume test
	cleanup(t)
}

func TestEndToEndResume(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	// Clean state fully
	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)

	// Create slot BEFORE inserting rows
	createSlotAndPublication(t)

	// Phase 1: Insert 500 rows and sync
	t.Log("phase 1: inserting 500 rows...")
	insertRows(t, 500)

	t.Log("phase 1: running sync for 12 seconds...")
	runSync(t, ctx, 12*time.Second)

	parquetCount1 := countParquetFilesOnS3(t)
	if parquetCount1 == 0 {
		t.Fatal("phase 1: expected at least 1 parquet file")
	}
	version1 := readVersionHint(t)
	t.Logf("phase 1: %d parquet files, version=%s", parquetCount1, version1)

	// Phase 2: Insert 500 more rows and sync again (resume)
	t.Log("phase 2: inserting 500 more rows...")
	insertRows(t, 500)

	t.Log("phase 2: running sync for 12 seconds...")
	runSync(t, ctx, 12*time.Second)

	parquetCount2 := countParquetFilesOnS3(t)
	if parquetCount2 <= parquetCount1 {
		t.Fatalf("phase 2: expected more parquet files than %d, got %d", parquetCount1, parquetCount2)
	}
	version2 := readVersionHint(t)
	t.Logf("phase 2: %d parquet files, version=%s", parquetCount2, version2)

	if version2 <= version1 {
		t.Fatalf("expected version to increase: was %s, now %s", version1, version2)
	}

	t.Logf("resume test passed: %d → %d parquet files, version %s → %s", parquetCount1, parquetCount2, version1, version2)

	// Cleanup
	cleanup(t)
}
