package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/viggy28/streambed/internal/simtest/oracle"
)

// Events is an append-only INSERT workload. It simulates a high-cardinality
// event log: every row is new, nothing is updated or deleted (outside the
// oracle's sentinel rows which are cleaned up between ticks).
//
// The schema mirrors a typical events table: a bigint primary key, a short
// type discriminator, a JSONB payload, and a timestamp.
type Events struct {
	Schema    string
	Table     string
	RatePerS  int
	BatchSize int
}

const eventsCreateTable = `CREATE TABLE IF NOT EXISTS %s.%s (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    filler TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`

func (e *Events) Name() string { return "events" }

func (e *Events) Setup(ctx context.Context, conn *pgx.Conn, logger *slog.Logger) error {
	if e.Schema == "" {
		e.Schema = "public"
	}
	if e.Table == "" {
		e.Table = "sim_events"
	}
	if e.BatchSize <= 0 {
		e.BatchSize = 10
	}
	if e.RatePerS <= 0 {
		e.RatePerS = 200
	}

	ddl := fmt.Sprintf(eventsCreateTable, e.Schema, e.Table)
	if _, err := conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("create %s.%s: %w", e.Schema, e.Table, err)
	}
	logger.Info("events workload: table ready",
		"schema", e.Schema, "table", e.Table,
		"rate_per_s", e.RatePerS, "batch_size", e.BatchSize)
	return nil
}

func (e *Events) Run(ctx context.Context, conn *pgx.Conn, counter RowCounter, logger *slog.Logger) error {
	// A simple rate limiter: one tick every (batch_size / rate) seconds.
	batchesPerSecond := float64(e.RatePerS) / float64(e.BatchSize)
	if batchesPerSecond < 1 {
		batchesPerSecond = 1
	}
	interval := time.Duration(float64(time.Second) / batchesPerSecond)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0xC0FFEE))
	types := []string{"login", "click", "purchase", "logout", "view", "search"}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s.%s (event_type, payload) SELECT unnest($1::text[]), unnest($2::jsonb[])",
		e.Schema, e.Table)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		eventTypes := make([]string, e.BatchSize)
		payloads := make([]string, e.BatchSize)
		for i := 0; i < e.BatchSize; i++ {
			eventTypes[i] = types[rng.IntN(len(types))]
			p, _ := json.Marshal(map[string]any{
				"user_id":    rng.IntN(10_000),
				"session_id": rng.IntN(1_000_000),
				"value":      rng.Float64() * 1000,
			})
			payloads[i] = string(p)
		}

		if _, err := conn.Exec(ctx, insertSQL, eventTypes, payloads); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("events insert failed", "error", err)
			continue
		}
		counter.AddRows(e.Name(), e.BatchSize)
	}
}

func (e *Events) Tables() []oracle.TableSpec {
	if e.Schema == "" {
		e.Schema = "public"
	}
	if e.Table == "" {
		e.Table = "sim_events"
	}
	return []oracle.TableSpec{{
		Schema:     e.Schema,
		Table:      e.Table,
		KeyColumns: []string{"id"},
		SentinelSQL: fmt.Sprintf(
			"INSERT INTO %s.%s (event_type, payload, filler) VALUES ('sentinel', '{}'::jsonb, $SENTINEL)",
			e.Schema, e.Table),
		SentinelCol: "filler",
	}}
}
