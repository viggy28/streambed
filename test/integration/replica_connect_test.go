//go:build integration

package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

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

	conn, err = net.DialTimeout("tcp", "localhost:9002", 2*time.Second)
	if err != nil {
		t.Skipf("MinIO not available at localhost:9002: %v", err)
	}
	conn.Close()
}

func TestReplicaConnect(t *testing.T) {
	skipIfReplicaNotAvailable(t)
	ctx := context.Background()

	// Clean state
	cleanup(t) // drops slot on primary if left over
	clearS3Prefix(t)
	setupTestTable(t)

	// Verify the replica is in hot-standby mode
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

	// Set up publication on primary and slot on replica
	t.Log("creating publication on primary and slot on replica...")
	createReplicaSlotAndPublication(t)

	// Insert rows on primary
	t.Log("inserting 20 rows on primary...")
	insertNamedRows(t, "test_events", 20)

	// Run pipeline against replica
	t.Log("running pipeline against replica for 15 seconds...")
	runReplicaSync(t, ctx, 15*time.Second)

	// Verify Parquet files were flushed
	parquetCount := countParquetFilesOnS3(t)
	if parquetCount == 0 {
		t.Fatal("expected at least 1 Parquet file on S3 after streaming from replica, got 0")
	}
	t.Logf("✓ %d Parquet file(s) written after streaming WAL from the replica", parquetCount)
}
