//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/viggy28/streambed/internal/server"
	"github.com/viggy28/streambed/internal/storage"
)

func TestIcebergTimeTravelThroughQueryServer(t *testing.T) {
	skipIfNotAvailable(t)
	ctx := context.Background()

	cleanup(t)
	clearS3Prefix(t)
	setupTestTable(t)
	t.Cleanup(func() {
		cleanup(t)
		execSQL(t, "DROP TABLE IF EXISTS test_events")
	})

	createSlotAndPublication(t)
	execSQL(t, `INSERT INTO test_events (name, value) VALUES ('old', 1)`)
	runSync(t, ctx, 8*time.Second)

	execSQL(t, `UPDATE test_events SET name = 'new', value = 2 WHERE id = 1`)
	runSync(t, ctx, 8*time.Second)

	version := readVersionHint(t)
	var metadata struct {
		Snapshots []struct {
			TimestampMS int64 `json:"timestamp-ms"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal(readMetadataJSON(t, version), &metadata); err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	if len(metadata.Snapshots) < 2 {
		t.Fatalf("got %d snapshots, want at least 2", len(metadata.Snapshots))
	}
	first := time.UnixMilli(metadata.Snapshots[len(metadata.Snapshots)-2].TimestampMS).UTC()
	second := time.UnixMilli(metadata.Snapshots[len(metadata.Snapshots)-1].TimestampMS).UTC()
	if !second.After(first) {
		t.Fatalf("latest snapshot %s is not after previous snapshot %s", second, first)
	}
	historical := first.Add(second.Sub(first) / 2).Format("2006-01-02 15:04:05.999999999")

	store, err := storage.NewS3Client(ctx, s3Bucket, s3Region, minioEndpoint)
	if err != nil {
		t.Fatalf("create S3 storage: %v", err)
	}
	port := freeTCPPort(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	queryServer, err := server.NewServer(server.ServerConfig{
		ListenAddr:   "127.0.0.1:" + port,
		S3Bucket:     s3Bucket,
		S3Prefix:     s3Prefix,
		S3Endpoint:   minioEndpoint,
		S3Region:     s3Region,
		TargetFormat: "iceberg",
	}, store, logger)
	if err != nil {
		t.Fatalf("create query server: %v", err)
	}
	defer queryServer.Close()
	serverCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() { serverErr <- queryServer.Start(serverCtx) }()

	queryConn := waitForQueryServer(t, port, serverErr)
	defer queryConn.Close(ctx)

	var name string
	historicalSQL := fmt.Sprintf(
		`SELECT name FROM public.test_events AT (TIMESTAMP => TIMESTAMPTZ '%s') WHERE id = 1`, historical)
	if err := queryConn.QueryRow(ctx, historicalSQL).Scan(&name); err != nil {
		t.Fatalf("historical query: %v", err)
	}
	if name != "old" {
		t.Fatalf("historical name = %q, want old", name)
	}
	if err := queryConn.QueryRow(ctx, `SELECT name FROM test_events WHERE id = 1`).Scan(&name); err != nil {
		t.Fatalf("latest query: %v", err)
	}
	if name != "new" {
		t.Fatalf("latest name = %q, want new", name)
	}
}

func freeTCPPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve query port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return fmt.Sprint(port)
}

func waitForQueryServer(t *testing.T, port string, serverErr <-chan error) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	url := "postgres://postgres@127.0.0.1:" + port + "/postgres"
	var lastErr error
	for ctx.Err() == nil {
		select {
		case err := <-serverErr:
			t.Fatalf("query server exited: %v", err)
		default:
		}
		conn, err := pgx.Connect(ctx, url)
		if err == nil {
			return conn
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("query server did not become ready: %v", lastErr)
	return nil
}
