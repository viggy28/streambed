//go:build integration

package integration

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/viggy28/streambed/internal/iceberg"
	"github.com/viggy28/streambed/internal/pipeline"
	"github.com/viggy28/streambed/internal/state"
	"github.com/viggy28/streambed/internal/storage"
	"github.com/viggy28/streambed/internal/wal"
)

const (
	// replicaHost and replicaPort are the address of the Postgres streaming
	// replica (hot standby) spun up by the docker-compose in this test.
	// Port 5435 is forwarded from the replica container.
	replicaHost = "localhost"
	replicaPort = "5435"
	replicaUser = "postgres"
	replicaPass = "test"
	replicaDB   = "postgres"
)

func replicaConnStr() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		replicaUser, replicaPass, replicaHost, replicaPort, replicaDB)
}

func replicaReplConnStr() string {
	return replicaConnStr() + "?replication=database"
}

// skipIfReplicaNotAvailable skips the test when the replica container is not
// reachable. This prevents failures when running the normal integration suite
// that only brings up the primary (port 5434).
func skipIfReplicaNotAvailable(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", replicaHost+":"+replicaPort, 2*time.Second)
	if err != nil {
		t.Skipf(
			"Postgres replica not available at %s:%s: %v\n"+
				"Start it with: docker compose -f test/integration/docker-compose-replica.yml up -d --wait",
			replicaHost, replicaPort, err,
		)
	}
	conn.Close()

	// MinIO must also be up.
	conn, err = net.DialTimeout("tcp", "localhost:9002", 2*time.Second)
	if err != nil {
		t.Skipf("MinIO not available at localhost:9002: %v", err)
	}
	conn.Close()
}

// TestReplicaConnect verifies that streambed can establish a logical-replication
// connection to a Postgres hot-standby replica.
//
// The test:
//  1. Connects to the PRIMARY (port 5434) to create the publication and slot,
//     then inserts a handful of rows.
//  2. Connects to the REPLICA (port 5435) with replication=database and starts
//     the streambed pipeline against it.
//  3. Runs the pipeline briefly and asserts that WAL events from the primary
//     are received and flushed to S3 (at least one Parquet file is produced).
//
// Prerequisites – start the replica compose file before running:
//
//	docker compose -f test/integration/docker-compose-replica.yml up -d --wait
//	go test -tags integration -v -run TestReplicaConnect -timeout 120s ./test/integration/...
//	docker compose -f test/integration/docker-compose-replica.yml down -v
func TestReplicaConnect(t *testing.T) {
	skipIfReplicaNotAvailable(t)
	ctx := context.Background()

	// ── 1. Primary setup ──────────────────────────────────────────────────────
	// Clean state from any previous run.
	cleanup(t) // drops slot + publication on the primary (port 5434)
	clearS3Prefix(t)

	execSQL(t, "DROP TABLE IF EXISTS replica_connect_test")
	execSQL(t, `CREATE TABLE IF NOT EXISTS replica_connect_test (
		id   SERIAL PRIMARY KEY,
		data TEXT NOT NULL
	)`)
	t.Cleanup(func() {
		cleanup(t)
		execSQL(t, "DROP TABLE IF EXISTS replica_connect_test")
	})

	// Create the replication slot and publication on the primary.
	createSlotAndPublication(t)

	// Insert rows on the primary so there is WAL to stream.
	for i := 0; i < 20; i++ {
		execSQL(t, fmt.Sprintf("INSERT INTO replica_connect_test (data) VALUES ('row-%d')", i))
	}

	// ── 2. Verify the replica is in hot-standby mode ──────────────────────────
	t.Log("verifying replica is a hot-standby...")
	stdConn, err := pgx.Connect(ctx, replicaConnStr())
	if err != nil {
		t.Fatalf("connect to replica (regular): %v", err)
	}
	var isInRecovery bool
	if err := stdConn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery); err != nil {
		stdConn.Close(ctx)
		t.Fatalf("pg_is_in_recovery() on replica: %v", err)
	}
	stdConn.Close(ctx)
	if !isInRecovery {
		t.Fatalf("expected replica to be in recovery mode (hot standby), but pg_is_in_recovery() = false")
	}
	t.Log("replica confirmed: pg_is_in_recovery() = true ✓")

	// ── 3. Open a logical-replication connection to the REPLICA ───────────────
	t.Log("opening replication connection to replica...")
	replConn, err := pgconn.Connect(ctx, replicaReplConnStr())
	if err != nil {
		t.Fatalf(
			"connect to replica for replication: %v\n\n"+
				"Ensure the replica Postgres is configured with:\n"+
				"  hot_standby = on\n"+
				"  wal_level = logical   (inherited from / set on primary)\n"+
				"  primary_conninfo = '...'",
			err,
		)
	}
	defer replConn.Close(context.Background())
	t.Log("replication connection to replica established ✓")

	// ── 4. Reuse the slot created on the primary ──────────────────────────────
	// Logical replication slots created on the primary are visible to
	// subscribers; we can start streaming from the replica using the same slot.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	slotLSN, err := wal.CreateOrReuseSlot(ctx, replConn, slotName, logger)
	if err != nil {
		t.Fatalf("setup replication slot on replica: %v", err)
	}
	t.Logf("slot LSN from replica: %s", slotLSN)

	// ── 5. Build the pipeline aimed at the replica connection ─────────────────
	s3Client, err := storage.NewS3Client(ctx, s3Bucket, s3Region, minioEndpoint)
	if err != nil {
		t.Fatalf("create S3 client: %v", err)
	}

	statePath := t.TempDir() + "/state.db"
	stateStore, err := state.Open(statePath)
	if err != nil {
		t.Fatalf("open state store: %v", err)
	}
	defer stateStore.Close()

	catalog := iceberg.NewCatalog(s3Client, s3Bucket, s3Prefix)
	writer := iceberg.NewWriter(catalog, s3Client, stateStore, slotName,
		flushRows, 5*time.Second, logger)

	// Metadata connection points at the primary because the replica is
	// read-only and cannot handle the pg_attribute queries for schema changes.
	metaConn, err := pgx.Connect(ctx, pgConnStr())
	if err != nil {
		t.Fatalf("connect metadata (primary): %v", err)
	}
	defer metaConn.Close(context.Background())
	metaQuerier := wal.NewMetadataQuerier(metaConn)

	tableFlushLSN := make(map[string]pglogrepl.LSN) // empty on first run
	p := pipeline.New(replConn, slotName, slotName, slotLSN, nil,
		logger, stateStore, tableFlushLSN, writer, 5*time.Second, metaQuerier)

	// ── 6. Run the pipeline briefly ───────────────────────────────────────────
	t.Log("running pipeline against replica for 20 seconds...")
	syncCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	pipelineErr := p.Run(syncCtx)
	if pipelineErr != nil && syncCtx.Err() == nil {
		t.Fatalf("pipeline returned unexpected error (not a timeout): %v", pipelineErr)
	}
	t.Logf("pipeline finished: %v", pipelineErr)

	// ── 7. Verify at least one Parquet file was flushed ───────────────────────
	parquetCount := countParquetFilesOnS3(t)
	if parquetCount == 0 {
		t.Fatal("expected at least 1 Parquet file on S3 after streaming from replica, got 0")
	}
	t.Logf("✓ %d Parquet file(s) written after streaming WAL from the replica", parquetCount)
}

// TestReplicaConnectRefusedOnReadOnly verifies that streambed surfaces a clear
// error when it tries to CREATE a brand-new replication slot on a hot-standby.
// Postgres does not allow slot creation on a standby; only the primary can do
// this. The test ensures CreateOrReuseSlot propagates the server error correctly.
func TestReplicaConnectRefusedOnReadOnly(t *testing.T) {
	skipIfReplicaNotAvailable(t)
	ctx := context.Background()

	// Drop any pre-existing slot so CreateOrReuseSlot will attempt CREATE.
	cleanup(t)

	t.Log("attempting to create a NEW replication slot directly on the replica (must fail)...")
	replConn, err := pgconn.Connect(ctx, replicaReplConnStr())
	if err != nil {
		t.Fatalf("connect to replica for replication: %v", err)
	}
	defer replConn.Close(context.Background())

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	_, err = wal.CreateOrReuseSlot(ctx, replConn, "new_slot_on_replica_test", logger)
	if err == nil {
		t.Fatal("expected an error when creating a replication slot on a hot-standby replica, but got nil")
	}
	t.Logf("✓ got expected error (slot creation refused on standby): %v", err)
}
