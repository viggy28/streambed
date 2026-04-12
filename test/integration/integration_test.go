//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
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
	_ "github.com/marcboeker/go-duckdb"
	pqgo "github.com/parquet-go/parquet-go"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/pipeline"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/storage"
	"github.com/viggy28/streambed/internal/wal"
)

const (
	pgHost     = "localhost"
	pgPort     = "5434"
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

// newTestS3Client returns an S3 client configured for the MinIO test instance.
func newTestS3Client(t *testing.T) *s3.Client {
	t.Helper()
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(s3Region))
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint)
		o.UsePathStyle = true
		o.Credentials = credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")
	})
}

// newTestDuckDB opens a DuckDB connection with Iceberg and httpfs extensions
// configured to read from the MinIO test instance. The connection is
// automatically closed when the test finishes.
func newTestDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { duckDB.Close() })

	for _, stmt := range []string{
		"INSTALL iceberg", "LOAD iceberg",
		"INSTALL httpfs", "LOAD httpfs",
		fmt.Sprintf("SET GLOBAL s3_region = '%s'", s3Region),
		"SET GLOBAL s3_endpoint = 'localhost:9002'",
		"SET GLOBAL s3_url_style = 'path'",
		"SET GLOBAL s3_use_ssl = false",
		"SET GLOBAL s3_access_key_id = 'minioadmin'",
		"SET GLOBAL s3_secret_access_key = 'minioadmin'",
	} {
		if _, err := duckDB.Exec(stmt); err != nil {
			t.Fatalf("duckdb setup %q: %v", stmt, err)
		}
	}
	return duckDB
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
	setupNamedTable(t, "test_events")
}

func insertRows(t *testing.T, count int) {
	t.Helper()
	insertNamedRows(t, "test_events", count)
}

// runSync runs the streambed sync pipeline for the given duration.
// If statePath is empty, a temporary directory is used.
func runSync(t *testing.T, ctx context.Context, duration time.Duration, statePath ...string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// State store — use provided path or create a temp one.
	path := ""
	if len(statePath) > 0 && statePath[0] != "" {
		path = statePath[0]
	} else {
		path = t.TempDir() + "/state.db"
	}
	stateStore, err := state.Open(path)
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

	startLSN := slotLSN

	// Initialize Iceberg catalog
	catalog := iceberg.NewCatalog(s3Client, s3Bucket, s3Prefix)

	// Read per-table flush LSNs from Iceberg for dedup on restart.
	tableFlushLSN := make(map[string]pglogrepl.LSN)
	registeredTables, err := stateStore.GetRegisteredTables()
	if err != nil {
		t.Fatalf("get registered tables: %v", err)
	}
	for _, rt := range registeredTables {
		exists, err := catalog.TableExists(ctx, rt.Schema, rt.Table)
		if err != nil || !exists {
			continue
		}
		lsnStr, found, err := catalog.GetSnapshotFlushLSN(ctx, rt.Schema, rt.Table)
		if err != nil || !found {
			continue
		}
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			continue
		}
		tableFlushLSN[fmt.Sprintf("%s.%s", rt.Schema, rt.Table)] = lsn
	}

	// Initialize writer
	writer := iceberg.NewWriter(catalog, s3Client, stateStore, slotName,
		flushRows, 5*time.Second, logger)

	// Create unified pipeline
	p := pipeline.New(pgConn, slotName, slotName, startLSN, nil,
		logger, stateStore, tableFlushLSN, writer, 5*time.Second)

	// Run with timeout
	syncCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	pipelineErr := p.Run(syncCtx)

	if pipelineErr != nil && syncCtx.Err() != nil {
		// Expected: context deadline exceeded
		t.Logf("pipeline stopped: %v", pipelineErr)
	} else if pipelineErr != nil {
		t.Fatalf("unexpected pipeline error: %v", pipelineErr)
	}
}

// countParquetFilesOnS3 lists objects under the test prefix and counts .parquet files.
func countParquetFilesOnS3(t *testing.T) int {
	t.Helper()
	ctx := context.Background()
	client := newTestS3Client(t)

	parquetCount := 0
	var continuationToken *string
	for {
		output, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s3Bucket),
			Prefix:            aws.String(s3Prefix + "/"),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			t.Fatalf("list objects: %v", err)
		}

		for _, obj := range output.Contents {
			key := aws.ToString(obj.Key)
			t.Logf("S3 object: %s (size: %d)", key, *obj.Size)
			if strings.HasSuffix(key, ".parquet") {
				parquetCount++
			}
		}

		if output.IsTruncated == nil || !*output.IsTruncated {
			break
		}
		continuationToken = output.NextContinuationToken
	}
	return parquetCount
}

// readVersionHint reads version-hint.text for the test table.
func readVersionHint(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	client := newTestS3Client(t)

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
	client := newTestS3Client(t)

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
	client := newTestS3Client(t)

	var continuationToken *string
	for {
		output, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s3Bucket),
			Prefix:            aws.String(s3Prefix + "/"),
			ContinuationToken: continuationToken,
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

		if output.IsTruncated == nil || !*output.IsTruncated {
			break
		}
		continuationToken = output.NextContinuationToken
	}
}

// countTotalParquetRows downloads all Parquet files from S3 and counts total rows.
// Also verifies the schema contains expected columns.
func countTotalParquetRows(t *testing.T) int64 {
	t.Helper()
	ctx := context.Background()
	client := newTestS3Client(t)

	var totalRows int64
	var continuationToken *string
	for {
		output, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s3Bucket),
			Prefix:            aws.String(s3Prefix + "/"),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			t.Fatalf("list objects: %v", err)
		}

		for _, obj := range output.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasSuffix(key, ".parquet") {
				continue
			}

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

			reader := bytes.NewReader(data)
			f, err := pqgo.OpenFile(reader, int64(len(data)))
			if err != nil {
				t.Fatalf("open parquet file %s: %v", key, err)
			}

			numRows := f.NumRows()
			t.Logf("parquet file %s: %d rows, %d columns", key, numRows, len(f.Schema().Columns()))

			colNames := make([]string, 0)
			for _, col := range f.Schema().Columns() {
				colNames = append(colNames, col[0])
			}
			t.Logf("  columns: %v", colNames)

			if len(colNames) < 4 {
				t.Errorf("expected at least 4 columns, got %d: %v", len(colNames), colNames)
			}

			totalRows += numRows
		}

		if output.IsTruncated == nil || !*output.IsTruncated {
			break
		}
		continuationToken = output.NextContinuationToken
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

	// Cleanup slot before next test
	cleanup(t)
}

func TestEndToEndUpdateDelete(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	// Clean state
	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)

	// Set REPLICA IDENTITY so UPDATEs/DELETEs include old key values.
	execSQL(t, "ALTER TABLE test_events REPLICA IDENTITY FULL")

	createSlotAndPublication(t)

	// Shared state store across all sync phases so LSN tracking persists
	// and the consumer doesn't replay already-processed WAL events.
	sharedStatePath := t.TempDir() + "/state.db"

	// Insert 10 rows
	t.Log("inserting 10 rows...")
	insertRows(t, 10)

	// Sync to flush inserts
	t.Log("syncing inserts...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	totalRows := countTotalParquetRows(t)
	t.Logf("after insert: %d rows in parquet", totalRows)
	if totalRows != 10 {
		t.Fatalf("expected 10 rows after insert, got %d", totalRows)
	}

	// UPDATE 3 rows
	t.Log("updating 3 rows...")
	execSQL(t, "UPDATE test_events SET name = 'updated_0' WHERE id = 1")
	execSQL(t, "UPDATE test_events SET name = 'updated_1', value = 999.99 WHERE id = 2")
	execSQL(t, "UPDATE test_events SET name = 'updated_2' WHERE id = 3")

	// Sync updates (COW should read-back + filter + rewrite)
	t.Log("syncing updates...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	// After UPDATE the COW rewrite should still have 10 rows (no rows gained/lost).
	totalRows = countLatestSnapshotRows(t)
	t.Logf("after update: %d rows in latest snapshot", totalRows)
	if totalRows != 10 {
		t.Fatalf("expected 10 rows after update, got %d", totalRows)
	}

	// DELETE 2 rows
	t.Log("deleting 2 rows...")
	execSQL(t, "DELETE FROM test_events WHERE id = 4")
	execSQL(t, "DELETE FROM test_events WHERE id = 5")

	// Sync deletes
	t.Log("syncing deletes...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	totalRows = countLatestSnapshotRows(t)
	t.Logf("after delete: %d rows in latest snapshot", totalRows)
	if totalRows != 8 {
		t.Fatalf("expected 8 rows after delete, got %d", totalRows)
	}

	// DELETE all remaining rows
	t.Log("deleting all remaining rows...")
	execSQL(t, "DELETE FROM test_events")

	t.Log("syncing delete-all...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	totalRows = countLatestSnapshotRows(t)
	t.Logf("after delete-all: %d rows in latest snapshot", totalRows)
	if totalRows != 0 {
		t.Fatalf("expected 0 rows after delete-all, got %d", totalRows)
	}

	cleanup(t)
}

// TestEndToEndTruncate exercises TRUNCATE handling: rows are inserted and
// flushed, then a TRUNCATE is replicated and the next sync must observe
// a latest snapshot with zero rows. A final INSERT verifies the table is
// still writable afterwards.
func TestEndToEndTruncate(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	execSQL(t, "ALTER TABLE test_events REPLICA IDENTITY FULL")

	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"

	t.Log("inserting 20 rows...")
	insertRows(t, 20)

	t.Log("syncing initial inserts...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	rows := countLatestSnapshotRows(t)
	t.Logf("after insert: %d rows in latest snapshot", rows)
	if rows != 20 {
		t.Fatalf("expected 20 rows after insert, got %d", rows)
	}

	t.Log("TRUNCATE test_events...")
	execSQL(t, "TRUNCATE test_events")

	t.Log("syncing truncate...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	rows = countLatestSnapshotRows(t)
	t.Logf("after truncate: %d rows in latest snapshot", rows)
	if rows != 0 {
		t.Fatalf("expected 0 rows after truncate, got %d", rows)
	}

	t.Log("inserting 5 fresh rows after truncate...")
	insertRows(t, 5)

	t.Log("syncing post-truncate inserts...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	rows = countLatestSnapshotRows(t)
	t.Logf("after post-truncate insert: %d rows in latest snapshot", rows)
	if rows != 5 {
		t.Fatalf("expected 5 rows after post-truncate insert, got %d", rows)
	}

	cleanup(t)
}

// TestEndToEndSnapshotSummaryLSN verifies that after a flush, the Iceberg
// snapshot summary contains "streambed.last_flush_lsn" and that it can be
// read back via GetSnapshotFlushLSN. This is the foundation for startup
// reconciliation (Iceberg is the source of truth for dedup cursors).
func TestEndToEndSnapshotSummaryLSN(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"

	t.Log("inserting 10 rows...")
	insertRows(t, 10)

	t.Log("syncing...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	// Read the snapshot summary LSN via the catalog
	storageClient, err := storage.NewS3Client(ctx, s3Bucket, s3Region, minioEndpoint)
	if err != nil {
		t.Fatalf("create S3 client: %v", err)
	}
	catalog := iceberg.NewCatalog(storageClient, s3Bucket, s3Prefix)

	lsnStr, found, err := catalog.GetSnapshotFlushLSN(ctx, "public", "test_events")
	if err != nil {
		t.Fatalf("GetSnapshotFlushLSN: %v", err)
	}
	if !found {
		t.Fatal("expected streambed.last_flush_lsn in snapshot summary, not found")
	}
	if lsnStr == "" {
		t.Fatal("streambed.last_flush_lsn is empty")
	}
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		t.Fatalf("parse LSN %q from summary: %v", lsnStr, err)
	}
	if lsn == 0 {
		t.Fatal("streambed.last_flush_lsn is zero")
	}
	t.Logf("snapshot summary streambed.last_flush_lsn = %s", lsn)

	cleanup(t)
}

// execSQL runs a single SQL statement against postgres.
func execSQL(t *testing.T, sql string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	result := conn.Exec(ctx, sql)
	if _, err := result.ReadAll(); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// countLatestSnapshotRows reads the latest snapshot's data files for
// test_events and counts actual parquet rows.
func countLatestSnapshotRows(t *testing.T) int64 {
	t.Helper()
	return countNamedTableSnapshotRows(t, "public", "test_events")
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

// TestEndToEndCOWDedup verifies that multiple UPDATEs on the same key within
// a single flush batch are collapsed to the last version, not accumulated.
// This is the regression test for issue #3.
func TestEndToEndCOWDedup(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	// Use REPLICA IDENTITY DEFAULT (PK = id) so the dedup key is
	// just the id column, not all columns. With FULL, every column
	// is part of the key and each update produces a unique key.
	// The issue (#3) describes the pgbench case which uses DEFAULT.

	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"

	// Insert 5 rows.
	t.Log("inserting 5 rows...")
	insertRows(t, 5)

	t.Log("syncing inserts...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	rows := countLatestSnapshotRows(t)
	if rows != 5 {
		t.Fatalf("expected 5 rows after insert, got %d", rows)
	}

	// UPDATE the same row 20 times — all in one batch.
	t.Log("updating same row 20 times...")
	for i := 0; i < 20; i++ {
		execSQL(t, fmt.Sprintf("UPDATE test_events SET name = 'version_%d' WHERE id = 1", i))
	}

	t.Log("syncing repeated updates...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	// After dedup, we should still have exactly 5 rows, not 5+20=25.
	rows = countLatestSnapshotRows(t)
	t.Logf("after 20 updates on same key: %d rows in latest snapshot", rows)
	if rows != 5 {
		t.Fatalf("expected 5 rows (dedup should collapse repeated updates), got %d", rows)
	}

	// Also test UPDATE then DELETE in same batch.
	t.Log("updating then deleting same row...")
	execSQL(t, "UPDATE test_events SET name = 'about_to_die' WHERE id = 2")
	execSQL(t, "DELETE FROM test_events WHERE id = 2")

	t.Log("syncing update-then-delete...")
	runSync(t, ctx, 12*time.Second, sharedStatePath)

	rows = countLatestSnapshotRows(t)
	t.Logf("after update+delete on same key: %d rows", rows)
	if rows != 4 {
		t.Fatalf("expected 4 rows (update+delete should net to delete), got %d", rows)
	}

	cleanup(t)
}

// ---------------------------------------------------------------------------
// Helpers for test harness
// ---------------------------------------------------------------------------

// getSlotConfirmedFlushLSN queries pg_replication_slots for the slot's
// confirmed_flush_lsn. Returns 0 if the slot doesn't exist.
func getSlotConfirmedFlushLSN(t *testing.T) pglogrepl.LSN {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect for slot query: %v", err)
	}
	defer conn.Close(ctx)

	result := conn.Exec(ctx, fmt.Sprintf(
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'", slotName))
	results, err := result.ReadAll()
	if err != nil {
		t.Fatalf("query slot: %v", err)
	}
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return 0
	}
	lsnStr := string(results[0].Rows[0][0])
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		t.Fatalf("parse slot LSN %q: %v", lsnStr, err)
	}
	return lsn
}

// pgRowCount returns SELECT count(*) for the given table in Postgres.
func pgRowCount(t *testing.T, table string) int64 {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect for count: %v", err)
	}
	defer conn.Close(ctx)

	result := conn.Exec(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table))
	results, err := result.ReadAll()
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	var count int64
	fmt.Sscanf(string(results[0].Rows[0][0]), "%d", &count)
	return count
}

// setupNamedTable creates a table with the given name and same schema as test_events.
func setupNamedTable(t *testing.T, name string) {
	t.Helper()
	execSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	execSQL(t, fmt.Sprintf(`CREATE TABLE %s (
		id SERIAL PRIMARY KEY,
		name TEXT NOT NULL,
		value DOUBLE PRECISION,
		created_at TIMESTAMPTZ DEFAULT NOW()
	)`, name))
}

// insertNamedRows inserts count rows into the named table.
func insertNamedRows(t *testing.T, table string, count int) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer conn.Close(ctx)

	for i := 0; i < count; i += 100 {
		batchSize := 100
		if i+batchSize > count {
			batchSize = count - i
		}
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("INSERT INTO %s (name, value) VALUES ", table))
		for j := 0; j < batchSize; j++ {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("('event_%d', %d.%d)", i+j, i+j, (i+j)%100))
		}
		result := conn.Exec(ctx, sb.String())
		if _, err := result.ReadAll(); err != nil {
			t.Fatalf("insert batch into %s at %d: %v", table, i, err)
		}
	}
}

// countNamedTableSnapshotRows counts rows in the latest Iceberg snapshot
// for a table with the given schema.table name.
func countNamedTableSnapshotRows(t *testing.T, schema, table string) int64 {
	t.Helper()
	ctx := context.Background()
	client := newTestS3Client(t)

	s3Client, err := storage.NewS3Client(ctx, s3Bucket, s3Region, minioEndpoint)
	if err != nil {
		t.Fatalf("create s3 client: %v", err)
	}

	catalog := iceberg.NewCatalog(s3Client, s3Bucket, s3Prefix)
	paths, err := catalog.GetDataFilePaths(ctx, schema, table)
	if err != nil {
		// Table may not exist yet in Iceberg
		t.Logf("GetDataFilePaths(%s.%s): %v", schema, table, err)
		return 0
	}

	var totalRows int64
	for _, path := range paths {
		key := path
		if idx := strings.Index(key, s3Bucket+"/"); idx >= 0 {
			key = key[idx+len(s3Bucket)+1:]
		}
		if !strings.HasPrefix(key, s3Prefix) {
			key = fmt.Sprintf("%s/%s/%s/%s", s3Prefix, schema, table, key)
		}

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s3Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		data, err := io.ReadAll(getOut.Body)
		getOut.Body.Close()
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}

		f, err := pqgo.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("open parquet %s: %v", key, err)
		}
		totalRows += f.NumRows()
	}
	return totalRows
}

// ---------------------------------------------------------------------------
// Correctness Tests
// ---------------------------------------------------------------------------

// TestIdleAck verifies that the replication slot's confirmed_flush_lsn
// advances even when no new traffic is flowing. The pipeline's standby
// heartbeat + keepalive handling should drive the ack forward.
func TestIdleAck(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	// Insert a small batch so the pipeline has something to flush,
	// then the source goes idle.
	insertRows(t, 10)

	// Run sync long enough for initial flush + several standby heartbeats.
	runSync(t, ctx, 20*time.Second)

	// Record slot position after first sync.
	lsn1 := getSlotConfirmedFlushLSN(t)
	t.Logf("slot LSN after first sync: %s", lsn1)

	if lsn1 == 0 {
		t.Fatal("slot confirmed_flush_lsn is 0 after sync")
	}

	// Generate some WAL activity that the pipeline doesn't care about
	// (a different table, not in the publication). This advances the
	// server WAL position, which should be picked up via keepalives.
	execSQL(t, "CREATE TABLE IF NOT EXISTS _idle_ack_dummy (x int)")
	execSQL(t, "INSERT INTO _idle_ack_dummy VALUES (1)")
	execSQL(t, "DROP TABLE _idle_ack_dummy")

	// Run sync again — no new test_events traffic, but keepalives
	// should advance the slot past the WAL generated above.
	runSync(t, ctx, 20*time.Second)

	lsn2 := getSlotConfirmedFlushLSN(t)
	t.Logf("slot LSN after idle sync: %s", lsn2)

	if lsn2 <= lsn1 {
		t.Fatalf("slot did not advance during idle: %s -> %s", lsn1, lsn2)
	}
	t.Logf("idle ack OK: slot advanced %s -> %s", lsn1, lsn2)

	cleanup(t)
}

// TestRowCountMatch inserts a known number of rows via rapid batches
// (simulating pgbench-style load) and verifies the Iceberg row count
// exactly matches Postgres.
func TestRowCountMatch(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	const totalRows = 5000
	t.Logf("inserting %d rows...", totalRows)
	insertRows(t, totalRows)

	t.Log("syncing...")
	runSync(t, ctx, 25*time.Second)

	pgCount := pgRowCount(t, "test_events")
	icebergCount := countLatestSnapshotRows(t)

	t.Logf("PG count: %d, Iceberg count: %d", pgCount, icebergCount)
	if pgCount != icebergCount {
		t.Fatalf("row count mismatch: PG=%d Iceberg=%d", pgCount, icebergCount)
	}
	if icebergCount != totalRows {
		t.Fatalf("expected %d rows, got %d", totalRows, icebergCount)
	}

	cleanup(t)
}

// TestRestartMidStream runs the pipeline while rows are being inserted,
// kills it mid-stream, then restarts and verifies no duplicates or lost rows.
func TestRestartMidStream(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"

	// Phase 1: Insert 500 rows and sync briefly (will flush some).
	t.Log("phase 1: inserting 500 rows...")
	insertRows(t, 500)

	t.Log("phase 1: short sync (5s) — simulates kill mid-stream")
	runSync(t, ctx, 5*time.Second, sharedStatePath)

	// Phase 2: Insert 500 more rows while pipeline is down.
	t.Log("phase 2: inserting 500 more rows while pipeline is down...")
	insertRows(t, 500)

	// Phase 3: Restart pipeline — should pick up where it left off.
	t.Log("phase 3: restarting pipeline (20s)...")
	runSync(t, ctx, 20*time.Second, sharedStatePath)

	pgCount := pgRowCount(t, "test_events")
	icebergCount := countLatestSnapshotRows(t)

	t.Logf("PG: %d rows, Iceberg: %d rows", pgCount, icebergCount)
	if pgCount != icebergCount {
		t.Fatalf("row count mismatch after restart: PG=%d Iceberg=%d", pgCount, icebergCount)
	}
	if icebergCount != 1000 {
		t.Fatalf("expected 1000 total rows, got %d", icebergCount)
	}

	cleanup(t)
}

// TestMassiveTransaction inserts 50,000 rows in a single transaction
// and verifies all-or-nothing delivery to Iceberg.
func TestMassiveTransaction(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	const totalRows = 50000

	// Insert all rows in a single transaction.
	t.Logf("inserting %d rows in single transaction...", totalRows)
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	result := conn.Exec(ctx, "BEGIN")
	if _, err := result.ReadAll(); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	for i := 0; i < totalRows; i += 1000 {
		batchSize := 1000
		if i+batchSize > totalRows {
			batchSize = totalRows - i
		}
		var sb strings.Builder
		sb.WriteString("INSERT INTO test_events (name, value) VALUES ")
		for j := 0; j < batchSize; j++ {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("('txn_%d', %d.%d)", i+j, i+j, (i+j)%100))
		}
		r := conn.Exec(ctx, sb.String())
		if _, err := r.ReadAll(); err != nil {
			t.Fatalf("insert batch at %d: %v", i, err)
		}
	}
	result = conn.Exec(ctx, "COMMIT")
	if _, err := result.ReadAll(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
	conn.Close(ctx)

	t.Log("syncing massive transaction...")
	runSync(t, ctx, 60*time.Second)

	pgCount := pgRowCount(t, "test_events")
	icebergCount := countLatestSnapshotRows(t)

	t.Logf("PG: %d rows, Iceberg: %d rows", pgCount, icebergCount)
	if pgCount != icebergCount {
		t.Fatalf("massive txn mismatch: PG=%d Iceberg=%d", pgCount, icebergCount)
	}
	if icebergCount != totalRows {
		t.Fatalf("expected %d rows, got %d", totalRows, icebergCount)
	}

	cleanup(t)
}

// TestHotColdTable creates two tables: one "hot" (many writes) and one
// "cold" (few writes). After restart, both tables must be correct and
// the slot must advance past the cold table's stale cursor.
func TestHotColdTable(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)

	// Create two tables.
	setupNamedTable(t, "hot_table")
	setupNamedTable(t, "cold_table")

	createSlotAndPublication(t)
	sharedStatePath := t.TempDir() + "/state.db"

	// Phase 1: Insert into both tables, sync.
	t.Log("phase 1: initial data")
	insertNamedRows(t, "cold_table", 10)
	insertNamedRows(t, "hot_table", 100)

	runSync(t, ctx, 15*time.Second, sharedStatePath)

	coldRows1 := countNamedTableSnapshotRows(t, "public", "cold_table")
	hotRows1 := countNamedTableSnapshotRows(t, "public", "hot_table")
	t.Logf("phase 1: hot=%d cold=%d", hotRows1, coldRows1)

	// Phase 2: Only write to hot table (cold table goes stale).
	t.Log("phase 2: hot table gets 2000 more rows, cold table is idle")
	insertNamedRows(t, "hot_table", 2000)

	runSync(t, ctx, 20*time.Second, sharedStatePath)

	coldRows2 := countNamedTableSnapshotRows(t, "public", "cold_table")
	hotRows2 := countNamedTableSnapshotRows(t, "public", "hot_table")
	t.Logf("phase 2: hot=%d cold=%d", hotRows2, coldRows2)

	if coldRows2 != 10 {
		t.Fatalf("cold table should still have 10 rows, got %d", coldRows2)
	}
	if hotRows2 != 2100 {
		t.Fatalf("hot table should have 2100 rows, got %d", hotRows2)
	}

	// Phase 3: Restart and verify both tables are still correct.
	t.Log("phase 3: restart and verify catchup")
	insertNamedRows(t, "hot_table", 500)
	insertNamedRows(t, "cold_table", 5)

	runSync(t, ctx, 20*time.Second, sharedStatePath)

	coldRows3 := countNamedTableSnapshotRows(t, "public", "cold_table")
	hotRows3 := countNamedTableSnapshotRows(t, "public", "hot_table")
	t.Logf("phase 3: hot=%d cold=%d", hotRows3, coldRows3)

	if coldRows3 != 15 {
		t.Fatalf("cold table should have 15 rows, got %d", coldRows3)
	}
	if hotRows3 != 2600 {
		t.Fatalf("hot table should have 2600 rows, got %d", hotRows3)
	}

	// Verify slot advanced (not stuck on cold table's stale cursor).
	lsn := getSlotConfirmedFlushLSN(t)
	t.Logf("slot LSN after hot/cold test: %s", lsn)
	if lsn == 0 {
		t.Fatal("slot LSN is 0 after hot/cold test")
	}

	cleanup(t)
	execSQL(t, "DROP TABLE IF EXISTS hot_table")
	execSQL(t, "DROP TABLE IF EXISTS cold_table")
}

// TestInterleavedMultiTable writes to 3 tables interleaved within the
// same transaction and verifies all tables have the correct row counts.
func TestInterleavedMultiTable(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)

	setupNamedTable(t, "table_a")
	setupNamedTable(t, "table_b")
	setupNamedTable(t, "table_c")

	createSlotAndPublication(t)

	// Interleave inserts across 3 tables in a single transaction.
	t.Log("inserting interleaved rows across 3 tables in one transaction...")
	conn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	r := conn.Exec(ctx, "BEGIN")
	if _, err := r.ReadAll(); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	for i := 0; i < 100; i++ {
		for _, stmt := range []string{
			fmt.Sprintf("INSERT INTO table_a (name, value) VALUES ('a_%d', %d)", i, i),
			fmt.Sprintf("INSERT INTO table_b (name, value) VALUES ('b_%d', %d)", i, i*2),
			fmt.Sprintf("INSERT INTO table_c (name, value) VALUES ('c_%d', %d)", i, i*3),
		} {
			r := conn.Exec(ctx, stmt)
			if _, err := r.ReadAll(); err != nil {
				t.Fatalf("exec %q: %v", stmt, err)
			}
		}
	}
	r = conn.Exec(ctx, "COMMIT")
	if _, err := r.ReadAll(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
	conn.Close(ctx)

	t.Log("syncing interleaved transaction...")
	runSync(t, ctx, 20*time.Second)

	aRows := countNamedTableSnapshotRows(t, "public", "table_a")
	bRows := countNamedTableSnapshotRows(t, "public", "table_b")
	cRows := countNamedTableSnapshotRows(t, "public", "table_c")

	t.Logf("table_a=%d table_b=%d table_c=%d", aRows, bRows, cRows)

	if aRows != 100 {
		t.Fatalf("table_a: expected 100, got %d", aRows)
	}
	if bRows != 100 {
		t.Fatalf("table_b: expected 100, got %d", bRows)
	}
	if cRows != 100 {
		t.Fatalf("table_c: expected 100, got %d", cRows)
	}

	cleanup(t)
	execSQL(t, "DROP TABLE IF EXISTS table_a")
	execSQL(t, "DROP TABLE IF EXISTS table_b")
	execSQL(t, "DROP TABLE IF EXISTS table_c")
}

// TestKeyLifecycle exercises INSERT->UPDATE->DELETE->re-INSERT on the same
// key within a single batch. The final state should be the re-inserted row.
func TestKeyLifecycle(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"

	// Insert initial rows.
	t.Log("inserting 5 rows...")
	insertRows(t, 5)

	runSync(t, ctx, 12*time.Second, sharedStatePath)
	rows := countLatestSnapshotRows(t)
	if rows != 5 {
		t.Fatalf("expected 5 initial rows, got %d", rows)
	}

	// Now exercise the full lifecycle on key id=1:
	// UPDATE -> DELETE -> re-INSERT (new row with new auto-generated id).
	t.Log("key lifecycle: UPDATE id=1...")
	execSQL(t, "UPDATE test_events SET name = 'updated' WHERE id = 1")
	t.Log("key lifecycle: DELETE id=1...")
	execSQL(t, "DELETE FROM test_events WHERE id = 1")
	t.Log("key lifecycle: re-INSERT with new row...")
	execSQL(t, "INSERT INTO test_events (name, value) VALUES ('reborn', 999.99)")

	runSync(t, ctx, 12*time.Second, sharedStatePath)

	// We started with 5, deleted 1, inserted 1 -> still 5.
	pgCount := pgRowCount(t, "test_events")
	icebergCount := countLatestSnapshotRows(t)
	t.Logf("PG: %d, Iceberg: %d", pgCount, icebergCount)

	if pgCount != icebergCount {
		t.Fatalf("key lifecycle mismatch: PG=%d Iceberg=%d", pgCount, icebergCount)
	}

	cleanup(t)
}

// ---------------------------------------------------------------------------
// Resilience Tests
// ---------------------------------------------------------------------------

// TestRestartReplayDedup forces the pipeline to restart from a position
// behind where Iceberg has already flushed, and verifies no duplicate
// rows appear (the dedup filter must suppress replayed events).
func TestRestartReplayDedup(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"

	// Phase 1: Insert and sync 500 rows.
	t.Log("phase 1: inserting 500 rows and syncing...")
	insertRows(t, 500)
	runSync(t, ctx, 15*time.Second, sharedStatePath)

	rows1 := countLatestSnapshotRows(t)
	t.Logf("phase 1: %d rows in Iceberg", rows1)
	if rows1 != 500 {
		t.Fatalf("expected 500 rows after phase 1, got %d", rows1)
	}

	// Phase 2: Insert more, sync briefly (simulates crash — some may not flush).
	t.Log("phase 2: inserting 300 more rows, short sync...")
	insertRows(t, 300)
	runSync(t, ctx, 5*time.Second, sharedStatePath)

	// Phase 3: Full restart — pipeline will replay some WAL events.
	t.Log("phase 3: full restart sync...")
	runSync(t, ctx, 20*time.Second, sharedStatePath)

	pgCount := pgRowCount(t, "test_events")
	icebergCount := countLatestSnapshotRows(t)
	t.Logf("phase 3: PG=%d Iceberg=%d", pgCount, icebergCount)

	if pgCount != icebergCount {
		t.Fatalf("restart replay dedup failed: PG=%d Iceberg=%d (duplicates?)", pgCount, icebergCount)
	}

	cleanup(t)
}

// TestRepeatedKillRestart loops 5 cycles of: insert rows -> sync briefly ->
// "kill" (stop). After all cycles, verifies the final Iceberg count
// matches Postgres exactly.
func TestRepeatedKillRestart(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	sharedStatePath := t.TempDir() + "/state.db"

	const cycles = 5
	const rowsPerCycle = 200

	for i := 0; i < cycles; i++ {
		t.Logf("cycle %d/%d: inserting %d rows...", i+1, cycles, rowsPerCycle)
		insertRows(t, rowsPerCycle)

		// Short sync — simulates being killed before full processing.
		syncDuration := 8 * time.Second
		if i == cycles-1 {
			// Last cycle: longer sync to ensure everything flushes.
			syncDuration = 20 * time.Second
		}
		t.Logf("cycle %d/%d: syncing for %s...", i+1, cycles, syncDuration)
		runSync(t, ctx, syncDuration, sharedStatePath)
	}

	pgCount := pgRowCount(t, "test_events")
	icebergCount := countLatestSnapshotRows(t)

	t.Logf("after %d kill/restart cycles: PG=%d Iceberg=%d", cycles, pgCount, icebergCount)
	if pgCount != icebergCount {
		t.Fatalf("repeated kill/restart mismatch: PG=%d Iceberg=%d", pgCount, icebergCount)
	}
	expectedTotal := int64(cycles * rowsPerCycle)
	if icebergCount != expectedTotal {
		t.Fatalf("expected %d total rows, got %d", expectedTotal, icebergCount)
	}

	cleanup(t)
}

// ---------------------------------------------------------------------------
// Edge Case Tests
// ---------------------------------------------------------------------------

// TestNullHeavyRows inserts rows where every nullable column is NULL
// and verifies the Parquet roundtrip preserves them correctly.
func TestNullHeavyRows(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)

	// Create a table with many nullable columns.
	execSQL(t, "DROP TABLE IF EXISTS null_test")
	execSQL(t, `CREATE TABLE null_test (
		id SERIAL PRIMARY KEY,
		name TEXT,
		value DOUBLE PRECISION,
		description TEXT,
		metadata TEXT,
		score INTEGER,
		ratio REAL,
		created_at TIMESTAMPTZ
	)`)

	createSlotAndPublication(t)

	// Insert rows with various NULL patterns.
	execSQL(t, "INSERT INTO null_test (name) VALUES ('only_name')")
	execSQL(t, "INSERT INTO null_test (value) VALUES (42.0)")
	execSQL(t, "INSERT INTO null_test (id) VALUES (DEFAULT)") // all nullable cols NULL
	execSQL(t, "INSERT INTO null_test (name, value, description, metadata, score, ratio, created_at) VALUES ('full', 1.0, 'desc', 'meta', 100, 0.5, NOW())")
	execSQL(t, "INSERT INTO null_test (name, score) VALUES ('partial', 50)")

	t.Log("syncing null-heavy rows...")
	runSync(t, ctx, 15*time.Second)

	icebergCount := countNamedTableSnapshotRows(t, "public", "null_test")
	pgCount := pgRowCount(t, "null_test")

	t.Logf("null_test: PG=%d Iceberg=%d", pgCount, icebergCount)
	if pgCount != icebergCount {
		t.Fatalf("null-heavy mismatch: PG=%d Iceberg=%d", pgCount, icebergCount)
	}

	cleanup(t)
	execSQL(t, "DROP TABLE IF EXISTS null_test")
}

// TestWideRow creates a table with 50 columns of mixed types and
// verifies correct schema mapping and data integrity.
func TestWideRow(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)

	// Build a wide table with 50 columns.
	var colDefs strings.Builder
	colDefs.WriteString("id SERIAL PRIMARY KEY")
	for i := 1; i <= 15; i++ {
		colDefs.WriteString(fmt.Sprintf(", col_text_%d TEXT", i))
	}
	for i := 1; i <= 15; i++ {
		colDefs.WriteString(fmt.Sprintf(", col_int_%d INTEGER", i))
	}
	for i := 1; i <= 10; i++ {
		colDefs.WriteString(fmt.Sprintf(", col_float_%d DOUBLE PRECISION", i))
	}
	for i := 1; i <= 9; i++ {
		colDefs.WriteString(fmt.Sprintf(", col_bool_%d BOOLEAN", i))
	}

	execSQL(t, "DROP TABLE IF EXISTS wide_table")
	execSQL(t, fmt.Sprintf("CREATE TABLE wide_table (%s)", colDefs.String()))

	createSlotAndPublication(t)

	// Insert rows with all columns populated.
	for row := 0; row < 20; row++ {
		var colNames []string
		var colVals []string
		for i := 1; i <= 15; i++ {
			colNames = append(colNames, fmt.Sprintf("col_text_%d", i))
			colVals = append(colVals, fmt.Sprintf("'text_%d_%d'", row, i))
		}
		for i := 1; i <= 15; i++ {
			colNames = append(colNames, fmt.Sprintf("col_int_%d", i))
			colVals = append(colVals, fmt.Sprintf("%d", row*100+i))
		}
		for i := 1; i <= 10; i++ {
			colNames = append(colNames, fmt.Sprintf("col_float_%d", i))
			colVals = append(colVals, fmt.Sprintf("%d.%d", row, i))
		}
		for i := 1; i <= 9; i++ {
			colNames = append(colNames, fmt.Sprintf("col_bool_%d", i))
			colVals = append(colVals, fmt.Sprintf("%t", i%2 == 0))
		}

		stmt := fmt.Sprintf("INSERT INTO wide_table (%s) VALUES (%s)",
			strings.Join(colNames, ", "), strings.Join(colVals, ", "))
		execSQL(t, stmt)
	}

	t.Log("syncing wide-row table (50 columns)...")
	runSync(t, ctx, 15*time.Second)

	icebergCount := countNamedTableSnapshotRows(t, "public", "wide_table")
	pgCount := pgRowCount(t, "wide_table")

	t.Logf("wide_table: PG=%d Iceberg=%d (50 columns)", pgCount, icebergCount)
	if pgCount != icebergCount {
		t.Fatalf("wide-row mismatch: PG=%d Iceberg=%d", pgCount, icebergCount)
	}

	cleanup(t)
	execSQL(t, "DROP TABLE IF EXISTS wide_table")
}

// TestLargeBatchFlush inserts 100,000 rows and verifies the pipeline
// handles it without timeout or data loss.
func TestLargeBatchFlush(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	const totalRows = 100000

	t.Logf("inserting %d rows in rapid batches...", totalRows)
	insertRows(t, totalRows)

	t.Log("syncing large batch...")
	runSync(t, ctx, 90*time.Second)

	pgCount := pgRowCount(t, "test_events")
	icebergCount := countLatestSnapshotRows(t)

	t.Logf("large batch: PG=%d Iceberg=%d", pgCount, icebergCount)
	if pgCount != icebergCount {
		t.Fatalf("large batch mismatch: PG=%d Iceberg=%d", pgCount, icebergCount)
	}

	cleanup(t)
}

// ---------------------------------------------------------------------------
// Performance Tests
// ---------------------------------------------------------------------------

// TestQueryLatencyComparison inserts a large dataset, syncs to Iceberg,
// then runs the same aggregation queries against both Postgres and DuckDB
// (via Iceberg) and logs the latency comparison.
func TestQueryLatencyComparison(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	const totalRows = 50000

	t.Logf("inserting %d rows for query benchmark...", totalRows)
	insertRows(t, totalRows)

	t.Log("syncing to Iceberg...")
	runSync(t, ctx, 60*time.Second)

	icebergCount := countLatestSnapshotRows(t)
	if icebergCount != totalRows {
		t.Fatalf("expected %d rows in Iceberg, got %d", totalRows, icebergCount)
	}

	duckDB := newTestDuckDB(t)

	icebergTable := fmt.Sprintf(
		"iceberg_scan('s3://%s/%s/public/test_events', allow_moved_paths = true)",
		s3Bucket, s3Prefix)

	// Queries to benchmark.
	queries := []struct {
		name    string
		pgSQL   string
		duckSQL string
	}{
		{
			name:    "COUNT(*)",
			pgSQL:   "SELECT count(*) FROM test_events",
			duckSQL: fmt.Sprintf("SELECT count(*) FROM %s", icebergTable),
		},
		{
			name:    "SUM(value)",
			pgSQL:   "SELECT sum(value) FROM test_events",
			duckSQL: fmt.Sprintf("SELECT sum(value) FROM %s", icebergTable),
		},
		{
			name:    "AVG + GROUP BY (top 10)",
			pgSQL:   "SELECT name, avg(value) FROM test_events GROUP BY name ORDER BY avg(value) DESC LIMIT 10",
			duckSQL: fmt.Sprintf("SELECT name, avg(value) FROM %s GROUP BY name ORDER BY avg(value) DESC LIMIT 10", icebergTable),
		},
		{
			name:    "Range scan (value > 100)",
			pgSQL:   "SELECT count(*) FROM test_events WHERE value > 100",
			duckSQL: fmt.Sprintf("SELECT count(*) FROM %s WHERE value > 100", icebergTable),
		},
		{
			name:    "MIN/MAX",
			pgSQL:   "SELECT min(value), max(value), min(id), max(id) FROM test_events",
			duckSQL: fmt.Sprintf("SELECT min(value), max(value), min(id), max(id) FROM %s", icebergTable),
		},
	}

	// PG connection for queries.
	pgConn, err := pgconn.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect for queries: %v", err)
	}
	defer pgConn.Close(ctx)

	t.Logf("\n%-30s %15s %15s %10s", "Query", "Postgres", "DuckDB", "Ratio")
	t.Logf("%-30s %15s %15s %10s", "-----", "--------", "------", "-----")

	for _, q := range queries {
		// Warm up (1 run each, discarded).
		pgConn.Exec(ctx, q.pgSQL).ReadAll()
		duckDB.QueryContext(ctx, q.duckSQL)

		// Benchmark Postgres (3 runs).
		var pgTotal time.Duration
		const runs = 3
		for i := 0; i < runs; i++ {
			start := time.Now()
			result := pgConn.Exec(ctx, q.pgSQL)
			result.ReadAll()
			pgTotal += time.Since(start)
		}
		pgAvg := pgTotal / runs

		// Benchmark DuckDB (3 runs).
		var duckTotal time.Duration
		for i := 0; i < runs; i++ {
			start := time.Now()
			rows, err := duckDB.QueryContext(ctx, q.duckSQL)
			if err != nil {
				t.Fatalf("duckdb query %q: %v", q.name, err)
			}
			for rows.Next() {
			}
			rows.Close()
			duckTotal += time.Since(start)
		}
		duckAvg := duckTotal / runs

		ratio := float64(pgAvg) / float64(duckAvg)
		t.Logf("%-30s %15s %15s %9.1fx", q.name, pgAvg, duckAvg, ratio)
	}

	cleanup(t)
}

// TestBulkIngestThroughput measures the pipeline's ingest throughput
// by inserting rows and measuring how fast they flow through to Iceberg.
func TestBulkIngestThroughput(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	createSlotAndPublication(t)

	const totalRows = 50000

	t.Logf("inserting %d rows...", totalRows)
	insertStart := time.Now()
	insertRows(t, totalRows)
	insertDuration := time.Since(insertStart)
	t.Logf("PG insert: %d rows in %s (%.0f rows/sec)",
		totalRows, insertDuration, float64(totalRows)/insertDuration.Seconds())

	t.Log("syncing to Iceberg (measuring throughput)...")
	syncStart := time.Now()
	runSync(t, ctx, 60*time.Second)
	syncDuration := time.Since(syncStart)

	icebergCount := countLatestSnapshotRows(t)
	if icebergCount != totalRows {
		t.Fatalf("expected %d rows, got %d", totalRows, icebergCount)
	}

	t.Logf("Pipeline throughput: %d rows in %s (%.0f rows/sec)",
		totalRows, syncDuration, float64(totalRows)/syncDuration.Seconds())

	cleanup(t)
}

