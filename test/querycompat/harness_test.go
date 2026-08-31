//go:build integration

package querycompat

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	sourceURL     = "postgres://postgres:test@localhost:5434/postgres"
	minioEndpoint = "http://localhost:9002"
	s3Bucket      = "streambed"
	s3Region      = "us-east-1"
)

type targetFormat string

const (
	targetIceberg  targetFormat = "iceberg"
	targetDuckLake targetFormat = "ducklake"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type runningProcess struct {
	cmd     *exec.Cmd
	output  *lockedBuffer
	done    chan struct{}
	waitErr error
}

func startProcess(t *testing.T, binary string, args ...string) *runningProcess {
	t.Helper()
	output := &lockedBuffer{}
	cmd := exec.Command(binary, args...)
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s %v: %v", binary, args, err)
	}
	p := &runningProcess{cmd: cmd, output: output, done: make(chan struct{})}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.done)
	}()
	return p
}

func (p *runningProcess) stop(t *testing.T) {
	t.Helper()
	if p == nil || p.cmd.Process == nil {
		return
	}
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil && !strings.Contains(err.Error(), "process already finished") {
		t.Logf("signal process: %v", err)
	}
	select {
	case <-p.done:
		if p.waitErr != nil {
			t.Fatalf("streambed exited after interrupt: %v\n%s", p.waitErr, p.output.String())
		}
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		t.Fatalf("streambed did not stop within 30s\n%s", p.output.String())
	}
}

func (p *runningProcess) kill() {
	if p == nil || p.cmd.Process == nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
}

func TestQueryCompatibility(t *testing.T) {
	if err := validateCases(); err != nil {
		t.Fatalf("invalid query compatibility cases: %v", err)
	}
	ensureDependencies(t)
	root := repositoryRoot(t)
	binary := filepath.Join(t.TempDir(), "streambed")
	build := exec.Command("go", "build", "-o", binary, "./cmd/streambed")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build streambed: %v\n%s", err, output)
	}

	for _, target := range []targetFormat{targetIceberg, targetDuckLake} {
		t.Run(string(target), func(t *testing.T) {
			runTargetCompatibility(t, binary, target)
		})
	}
}

func runTargetCompatibility(t *testing.T, binary string, target targetFormat) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	source, err := pgx.Connect(ctx, sourceURL)
	if err != nil {
		t.Fatalf("connect source Postgres: %v", err)
	}
	defer source.Close(context.Background())

	suffix := fmt.Sprintf("%s_%d", target, time.Now().UnixNano())
	slot := "querycompat_" + suffix
	prefix := "querycompat/" + suffix
	tempDir := t.TempDir()
	statePath := filepath.Join(tempDir, "state.db")
	catalogPath := filepath.Join(tempDir, "ducklake.sqlite")
	dataPath := fmt.Sprintf("s3://%s/%s/ducklake/", s3Bucket, prefix)

	cleanupSource(t, source, slot)
	createFixtureSchema(t, source)
	defer cleanupSource(t, source, slot)

	syncArgs := []string{
		"sync",
		"--source-url=" + sourceURL,
		"--s3-bucket=" + s3Bucket,
		"--s3-prefix=" + prefix,
		"--s3-endpoint=" + minioEndpoint,
		"--s3-region=" + s3Region,
		"--target-format=" + string(target),
		"--slot-name=" + slot,
		"--state-path=" + statePath,
		"--flush-rows=1000",
		"--flush-interval=1s",
		"--include-tables=public.oracle_groups,public.oracle_values",
		"--log-level=INFO",
	}
	if target == targetDuckLake {
		syncArgs = append(syncArgs,
			"--ducklake-catalog="+catalogPath,
			"--ducklake-data-path="+dataPath,
		)
	}

	syncProcess := startProcess(t, binary, syncArgs...)
	t.Cleanup(syncProcess.kill)
	waitForReplicationReady(t, ctx, source, slot, syncProcess)
	fixtureLSN := insertFixture(t, source)
	waitForSync(t, ctx, source, slot, fixtureLSN, syncProcess)
	syncProcess.stop(t) // graceful shutdown performs the final durable flush

	port := freePort(t)
	queryAddr := fmt.Sprintf("127.0.0.1:%d", port)
	queryArgs := []string{
		"query",
		"--listen-addr=" + queryAddr,
		"--s3-bucket=" + s3Bucket,
		"--s3-prefix=" + prefix,
		"--s3-endpoint=" + minioEndpoint,
		"--s3-region=" + s3Region,
		"--target-format=" + string(target),
		"--log-level=INFO",
	}
	if target == targetDuckLake {
		queryArgs = append(queryArgs,
			"--ducklake-catalog="+catalogPath,
			"--ducklake-data-path="+dataPath,
		)
	}
	queryProcess := startProcess(t, binary, queryArgs...)
	t.Cleanup(queryProcess.kill)

	streambedURL := fmt.Sprintf("postgres://querycompat@%s/postgres?sslmode=disable", queryAddr)
	streambed := waitForQueryReady(t, ctx, streambedURL, queryProcess)
	defer streambed.Close(context.Background())

	for _, tc := range queryCases {
		t.Run(tc.Name, func(t *testing.T) {
			runQueryCase(t, ctx, source, streambed, target, tc)
		})
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func ensureDependencies(t *testing.T) {
	t.Helper()
	for name, address := range map[string]string{
		"Postgres": "localhost:5434",
		"MinIO":    "localhost:9002",
	} {
		conn, err := net.DialTimeout("tcp", address, 2*time.Second)
		if err != nil {
			t.Skipf("%s is not available at %s: %v", name, address, err)
		}
		_ = conn.Close()
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate query port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForReplicationReady(t *testing.T, ctx context.Context, source *pgx.Conn, slot string, process *runningProcess) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			t.Fatalf("streambed sync exited before replication was ready: %v\n%s", process.waitErr, process.output.String())
		default:
		}
		var ready bool
		err := source.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_replication_slots WHERE slot_name = $1
		) AND EXISTS (
			SELECT 1 FROM pg_publication WHERE pubname = $1
		)`, slot).Scan(&ready)
		if err == nil && ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for replication setup: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("replication slot/publication %s was not ready within 20s\n%s", slot, process.output.String())
}

func waitForSync(t *testing.T, ctx context.Context, source *pgx.Conn, slot, fixtureLSN string, process *runningProcess) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var confirmedLSN string
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			t.Fatalf("streambed sync exited before fixture LSN %s was durable: %v\n%s", fixtureLSN, process.waitErr, process.output.String())
		default:
		}

		var reached bool
		err := source.QueryRow(ctx, `SELECT confirmed_flush_lsn::text, confirmed_flush_lsn >= $2::pg_lsn
			FROM pg_replication_slots WHERE slot_name = $1`, slot, fixtureLSN).Scan(&confirmedLSN, &reached)
		if err == nil && reached {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for fixture LSN %s: %v", fixtureLSN, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("streambed did not make fixture LSN %s durable within 30s (confirmed=%s)\n%s",
		fixtureLSN, confirmedLSN, process.output.String())
}

func waitForQueryReady(t *testing.T, ctx context.Context, url string, process *runningProcess) *pgx.Conn {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			t.Fatalf("streambed query server exited before readiness: %v\n%s", process.waitErr, process.output.String())
		default:
		}
		conn, err := pgx.Connect(ctx, url)
		if err == nil {
			var count int
			err = conn.QueryRow(ctx, "SELECT count(*) FROM oracle_values").Scan(&count)
			if err == nil && count == fixtureValueCount {
				return conn
			}
			_ = conn.Close(context.Background())
			if err == nil {
				err = fmt.Errorf("oracle_values count=%d, want %d", count, fixtureValueCount)
			}
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("wait for query server: %v\n%s", ctx.Err(), process.output.String())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("query server was not ready within 30s: %v\n%s", lastErr, process.output.String())
	return nil
}
