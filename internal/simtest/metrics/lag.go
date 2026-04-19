package metrics

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// QueryReplicationLag returns the bytes of WAL not yet confirmed by the given
// replication slot. Positive = slot is behind current WAL; 0 = caught up.
// Returns an error if the slot does not exist yet (first run, before streambed
// has ever connected).
func QueryReplicationLag(ctx context.Context, conn *pgconn.PgConn, slotName string) (int64, error) {
	q := fmt.Sprintf(`
		SELECT pg_current_wal_flush_lsn() - confirmed_flush_lsn AS lag_bytes
		FROM pg_replication_slots WHERE slot_name = '%s'`, slotName)
	r := conn.Exec(ctx, q)
	results, err := r.ReadAll()
	if err != nil {
		return 0, fmt.Errorf("query replication lag: %w", err)
	}
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return 0, fmt.Errorf("replication slot %q not found", slotName)
	}
	var lag int64
	raw := results[0].Rows[0][0]
	if raw == nil {
		return 0, nil
	}
	if _, err := fmt.Sscanf(string(raw), "%d", &lag); err != nil {
		return 0, fmt.Errorf("parse lag value %q: %w", raw, err)
	}
	return lag, nil
}
