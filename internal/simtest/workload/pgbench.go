package workload

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/viggy28/streambed/internal/simtest/oracle"
)

// PGBench drives the standard `pgbench` workload against Postgres. It shells
// out to the `pgbench` CLI because rewriting TPC-B in Go would just be an
// inferior copy — the point of pgbench is industry-standard mixed-DML numbers.
//
// Setup initializes the pgbench schema at the configured scale factor (idempotent
// if the tables already exist with the same scale). Run launches a long-lived
// `pgbench -T 0` that exits only when the supervisor context is cancelled.
//
// The oracle watches pgbench_branches, pgbench_tellers, and pgbench_accounts
// — the three tables with primary keys. pgbench_history has no PK, so it is
// left to the invariants layer (row count monotonicity).
type PGBench struct {
	SourceURL string // must match simtest's --source-url
	Scale     int    // -s flag (10 = 1M accounts)
	Clients   int    // -c flag (concurrent connections)
	Jobs      int    // -j flag (threads); defaults to Clients
}

func (p *PGBench) Name() string { return "pgbench" }

func (p *PGBench) Setup(ctx context.Context, conn *pgx.Conn, logger *slog.Logger) error {
	if p.Scale <= 0 {
		p.Scale = 10
	}
	if p.Clients <= 0 {
		p.Clients = 10
	}
	if p.Jobs <= 0 {
		p.Jobs = p.Clients
	}

	// Check if pgbench_accounts already exists at the right scale. Re-
	// initializing at the same scale is harmless but slow, so skip it when
	// possible.
	var exists bool
	if err := conn.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM pg_tables
		               WHERE schemaname = 'public' AND tablename = 'pgbench_accounts')
	`).Scan(&exists); err != nil {
		return fmt.Errorf("check pgbench tables: %w", err)
	}

	if exists {
		var count int64
		expected := int64(p.Scale) * 100_000
		if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM pgbench_accounts").Scan(&count); err != nil {
			return fmt.Errorf("count pgbench_accounts: %w", err)
		}
		if count == expected {
			logger.Info("pgbench: schema already initialized at expected scale",
				"scale", p.Scale, "accounts", count)
			return nil
		}
		logger.Info("pgbench: re-initializing (existing scale mismatch)",
			"expected_accounts", expected, "actual", count)
	}

	// pgbench -i -s N — populates the four tables.
	cmd := exec.CommandContext(ctx, "pgbench", "-i", "-s", fmt.Sprintf("%d", p.Scale), p.libpqURL())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pgbench -i: %w\n%s", err, string(out))
	}
	logger.Info("pgbench: schema initialized", "scale", p.Scale)
	return nil
}

func (p *PGBench) Run(ctx context.Context, _ *pgx.Conn, counter RowCounter, logger *slog.Logger) error {
	// -T 0 means "no time limit" — we control lifetime via ctx.
	// --no-vacuum skips the slow VACUUM that pgbench does by default before
	// each invocation (we initialized once in Setup; no need to redo it).
	args := []string{
		"-c", fmt.Sprintf("%d", p.Clients),
		"-j", fmt.Sprintf("%d", p.Jobs),
		"-T", "0",
		"--no-vacuum",
		"--progress=5", // emit progress lines every 5s
		p.libpqURL(),
	}
	cmd := exec.CommandContext(ctx, "pgbench", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start pgbench: %w", err)
	}
	logger.Info("pgbench running", "clients", p.Clients, "jobs", p.Jobs)

	// Parse pgbench's progress lines to feed the metrics layer. Each TPC-B
	// transaction writes 4 rows (one each in branches, tellers, accounts,
	// history), so we multiply tps by 4 for the row counter.
	done := make(chan struct{})
	go func() {
		defer close(done)
		parsePgbenchProgress(stdout, counter, p.Name(), logger)
	}()

	// Wait for either ctx cancel or pgbench exit.
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	select {
	case <-ctx.Done():
		// pgbench traps SIGTERM and exits cleanly with a summary.
		_ = cmd.Process.Signal(syscall.Signal(15))
		select {
		case <-waitErr:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-waitErr
		}
	case err := <-waitErr:
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("pgbench exited: %w", err)
		}
	}
	<-done
	return nil
}

func (p *PGBench) Tables() []oracle.TableSpec {
	// Streambed flushes per-table buffers independently: a single-table
	// sentinel only proves THAT table caught up. The pgbench balance
	// invariant reads pgbench_tellers and pgbench_branches, so those must
	// have their own sentinels or the invariant can fire mid-flight and
	// look broken.
	//
	// Each sentinel uses a negative primary key (-1) that pgbench never
	// generates. ON CONFLICT DO UPDATE lets the same row be re-used across
	// oracle ticks.
	return []oracle.TableSpec{
		{
			Schema:     "public",
			Table:      "pgbench_accounts",
			KeyColumns: []string{"aid"},
			SentinelSQL: "INSERT INTO public.pgbench_accounts (aid, bid, abalance, filler) " +
				"VALUES (-1, 1, 0, $SENTINEL) ON CONFLICT (aid) DO UPDATE SET filler = EXCLUDED.filler",
			SentinelCol: "filler",
		},
		{
			Schema:     "public",
			Table:      "pgbench_tellers",
			KeyColumns: []string{"tid"},
			SentinelSQL: "INSERT INTO public.pgbench_tellers (tid, bid, tbalance, filler) " +
				"VALUES (-1, 1, 0, $SENTINEL) ON CONFLICT (tid) DO UPDATE SET filler = EXCLUDED.filler",
			SentinelCol: "filler",
		},
		{
			Schema:     "public",
			Table:      "pgbench_branches",
			KeyColumns: []string{"bid"},
			SentinelSQL: "INSERT INTO public.pgbench_branches (bid, bbalance, filler) " +
				"VALUES (-1, 0, $SENTINEL) ON CONFLICT (bid) DO UPDATE SET filler = EXCLUDED.filler",
			SentinelCol: "filler",
		},
	}
}

func (p *PGBench) libpqURL() string {
	// pgbench accepts libpq-style URLs. Our Options.SourceURL is already
	// in that form, but strip any replication=... parameter.
	u, err := url.Parse(p.SourceURL)
	if err != nil {
		return p.SourceURL
	}
	q := u.Query()
	q.Del("replication")
	u.RawQuery = q.Encode()
	return u.String()
}

// parsePgbenchProgress reads pgbench's stdout/stderr stream looking for
// --progress= lines shaped like:
//   progress: 5.0 s, 1234.5 tps, lat 8.123 ms stddev 2.345
// and increments the row counter by tps * 4 (one row per target table).
// Non-progress lines are forwarded to the logger at Debug level.
func parsePgbenchProgress(r interface {
	Read(p []byte) (int, error)
}, counter RowCounter, name string, logger *slog.Logger) {
	buf := make([]byte, 4096)
	var line strings.Builder
	var lastProgress float64

	for {
		n, err := r.Read(buf)
		if n > 0 {
			for _, b := range buf[:n] {
				if b == '\n' {
					s := line.String()
					line.Reset()
					if strings.HasPrefix(s, "progress:") {
						tps := parseTPS(s)
						if tps > 0 {
							delta := tps - lastProgress
							if delta < 0 {
								delta = tps // counter reset or first line
							}
							// 4 rows per TPC-B txn, 5-second progress window.
							rows := int(delta * 5 * 4)
							if rows > 0 {
								counter.AddRows(name, rows)
							}
							lastProgress = tps
						}
					}
					if s != "" {
						logger.Debug("pgbench", "line", s)
					}
				} else {
					line.WriteByte(b)
				}
			}
		}
		if err != nil {
			return
		}
	}
}

// parseTPS extracts the TPS value from a pgbench progress line like:
//   "progress: 5.0 s, 1234.5 tps, lat 8.123 ms stddev 2.345"
func parseTPS(s string) float64 {
	idx := strings.Index(s, "tps")
	if idx <= 0 {
		return 0
	}
	// Walk backwards from "tps" over the comma to the number.
	end := idx
	for end > 0 && s[end-1] == ' ' {
		end--
	}
	start := end
	for start > 0 && (s[start-1] == '.' || (s[start-1] >= '0' && s[start-1] <= '9')) {
		start--
	}
	if start >= end {
		return 0
	}
	var tps float64
	_, err := fmt.Sscanf(s[start:end], "%f", &tps)
	if err != nil {
		return 0
	}
	return tps
}
