// Package workload provides traffic generators that write to Postgres
// to exercise Streambed's replication pipeline under production-like load.
//
// Every workload writes directly to Postgres; none of them know about
// Streambed. Streambed runs as a separate process and consumes the WAL
// asynchronously — just like in production.
package workload

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/viggy28/streambed/internal/simtest/oracle"
)

// Workload is a long-running traffic generator against Postgres.
// Each workload runs in its own goroutine; Run must return when ctx is done.
type Workload interface {
	// Name is used in logs and metrics labels. Must be stable across restarts.
	Name() string

	// Setup is called once before Run. It should create any tables the
	// workload writes to and prepare initial state (seed rows, etc).
	Setup(ctx context.Context, conn *pgx.Conn, logger *slog.Logger) error

	// Run executes the workload until ctx is cancelled. It must tolerate
	// transient database errors and keep running.
	Run(ctx context.Context, conn *pgx.Conn, counter RowCounter, logger *slog.Logger) error

	// Tables returns the oracle TableSpecs this workload contributes — one
	// per table it writes to, so the validator can plant sentinels and diff.
	Tables() []oracle.TableSpec
}

// RowCounter is a callback the workload invokes after each write so the
// metrics layer can track throughput without the workload depending on
// the metrics package directly.
type RowCounter interface {
	AddRows(workload string, n int)
}
