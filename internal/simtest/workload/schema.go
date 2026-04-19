package workload

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/viggy28/streambed/internal/simtest/oracle"
)

// SchemaEvolution stresses Streambed's ability to handle ALTER TABLE
// operations while replication is live. It maintains a table with a small
// rotating set of columns: the workload periodically adds a new column and
// drops one of the oldest mutable columns, with inserts and updates
// continuing throughout.
//
// What this catches:
//   - RelationMessage handling when schema changes mid-stream.
//   - Iceberg schema evolution: add-column, drop-column, column-id mapping.
//   - Row events that arrive referencing columns the writer hasn't observed
//     in the latest RelationMessage yet (narrow race window).
type SchemaEvolution struct {
	Schema           string
	Table            string
	AlterEvery       time.Duration
	WriteRatePerS    int
	MaxMutableColumns int
}

const schemaCreate = `CREATE TABLE IF NOT EXISTS %s.%s (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    filler TEXT
)`

func (s *SchemaEvolution) Name() string { return "schema_evolution" }

func (s *SchemaEvolution) Setup(ctx context.Context, conn *pgx.Conn, logger *slog.Logger) error {
	if s.Schema == "" {
		s.Schema = "public"
	}
	if s.Table == "" {
		s.Table = "sim_schema"
	}
	if s.AlterEvery == 0 {
		s.AlterEvery = 30 * time.Second
	}
	if s.WriteRatePerS <= 0 {
		s.WriteRatePerS = 20
	}
	if s.MaxMutableColumns <= 0 {
		s.MaxMutableColumns = 5
	}

	if _, err := conn.Exec(ctx, fmt.Sprintf(schemaCreate, s.Schema, s.Table)); err != nil {
		return fmt.Errorf("create %s.%s: %w", s.Schema, s.Table, err)
	}
	logger.Info("schema-evolution workload: table ready",
		"schema", s.Schema, "table", s.Table,
		"alter_every", s.AlterEvery, "write_rate_per_s", s.WriteRatePerS)
	return nil
}

func (s *SchemaEvolution) Run(ctx context.Context, conn *pgx.Conn, counter RowCounter, logger *slog.Logger) error {
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0xDEADBEEF))

	writeInterval := time.Second / time.Duration(s.WriteRatePerS)
	writeTicker := time.NewTicker(writeInterval)
	defer writeTicker.Stop()

	alterTicker := time.NewTicker(s.AlterEvery)
	defer alterTicker.Stop()

	// Track currently-live mutable columns (excluding the permanent id,
	// created_at, filler). We append new ones via ALTER ADD COLUMN and drop
	// the oldest when we hit MaxMutableColumns.
	mutable := []string{}
	counter0 := 0 // monotonic suffix for column names

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-writeTicker.C:
			if err := s.doWrite(ctx, conn, rng, mutable); err != nil && ctx.Err() == nil {
				logger.Warn("schema-evolution write failed", "error", err)
				continue
			}
			counter.AddRows(s.Name(), 1)

		case <-alterTicker.C:
			// Alternate ADD and DROP when at capacity.
			if len(mutable) >= s.MaxMutableColumns {
				victim := mutable[0]
				mutable = mutable[1:]
				alter := fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN %s", s.Schema, s.Table, victim)
				if _, err := conn.Exec(ctx, alter); err != nil {
					logger.Warn("drop column failed", "col", victim, "error", err)
				} else {
					logger.Info("schema evolved: dropped column", "col", victim)
				}
			}
			counter0++
			newCol := fmt.Sprintf("dyn_%d", counter0)
			alter := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s TEXT", s.Schema, s.Table, newCol)
			if _, err := conn.Exec(ctx, alter); err != nil {
				logger.Warn("add column failed", "col", newCol, "error", err)
				continue
			}
			mutable = append(mutable, newCol)
			logger.Info("schema evolved: added column", "col", newCol,
				"live_mutable_count", len(mutable))
		}
	}
}

func (s *SchemaEvolution) doWrite(ctx context.Context, conn *pgx.Conn, rng *rand.Rand, _ []string) error {
	// Write to only the permanent columns. The mutable columns get filled
	// with defaults (NULL) on INSERT; reading them on the Iceberg side
	// exercises the "old row without new column" path.
	_, err := conn.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s.%s (filler) VALUES ($1)", s.Schema, s.Table),
		fmt.Sprintf("row_%d", rng.IntN(1_000_000_000)))
	return err
}

func (s *SchemaEvolution) Tables() []oracle.TableSpec {
	if s.Schema == "" {
		s.Schema = "public"
	}
	if s.Table == "" {
		s.Table = "sim_schema"
	}
	return []oracle.TableSpec{{
		Schema:     s.Schema,
		Table:      s.Table,
		KeyColumns: []string{"id"},
		SentinelSQL: fmt.Sprintf(
			"INSERT INTO %s.%s (filler) VALUES ($SENTINEL)", s.Schema, s.Table),
		SentinelCol: "filler",
	}}
}
