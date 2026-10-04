package ingester

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

//go:embed schema.sql
var schemaSQL string

type Sink interface {
	Apply(context.Context, map[string][]int64, []Item, time.Time) error
}

type Store struct {
	conn          *pgx.Conn
	frontPageSize int
}

func OpenStore(ctx context.Context, databaseURL string, frontPageSize int) (*Store, error) {
	return openStore(ctx, databaseURL, frontPageSize, true)
}

func OpenExistingStore(ctx context.Context, databaseURL string, frontPageSize int) (*Store, error) {
	return openStore(ctx, databaseURL, frontPageSize, false)
}

func openStore(ctx context.Context, databaseURL string, frontPageSize int, ensureSchema bool) (*Store, error) {
	if frontPageSize < 1 {
		return nil, fmt.Errorf("front page size must be positive")
	}
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to Postgres: %w", err)
	}
	store := &Store{conn: conn, frontPageSize: frontPageSize}
	if ensureSchema {
		if _, err := conn.Exec(ctx, schemaSQL); err != nil {
			conn.Close(ctx)
			return nil, fmt.Errorf("create demo schema: %w", err)
		}
	}
	return store, nil
}

func (s *Store) Close(ctx context.Context) error {
	return s.conn.Close(ctx)
}

func (s *Store) VerifySupabaseSchema(ctx context.Context) error {
	expectedTables := []string{"stories", "rankings", "front_page", "ingestion_status"}
	for _, table := range expectedTables {
		var rlsEnabled bool
		err := s.conn.QueryRow(ctx, `
			SELECT c.relrowsecurity
			FROM pg_class AS c
			JOIN pg_namespace AS n ON n.oid = c.relnamespace
			WHERE n.nspname = 'public' AND c.relname = $1 AND c.relkind = 'r'`, table).Scan(&rlsEnabled)
		if err != nil {
			return fmt.Errorf("verify public.%s: %w", table, err)
		}
		if !rlsEnabled {
			return fmt.Errorf("verify public.%s: row-level security is not enabled", table)
		}
	}

	rows, err := s.conn.Query(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_publication AS p
		JOIN pg_publication_rel AS pr ON pr.prpubid = p.oid
		JOIN pg_class AS c ON c.oid = pr.prrelid
		JOIN pg_namespace AS n ON n.oid = c.relnamespace
		WHERE p.pubname = 'streambed_hn_demo'`)
	if err != nil {
		return fmt.Errorf("verify streambed_hn_demo publication: %w", err)
	}
	defer rows.Close()
	actual := make(map[string]struct{})
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return fmt.Errorf("scan streambed_hn_demo publication: %w", err)
		}
		actual[schema+"."+table] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read streambed_hn_demo publication: %w", err)
	}
	expectedPublished := []string{"public.stories", "public.rankings", "public.front_page"}
	if len(actual) != len(expectedPublished) {
		return fmt.Errorf("verify streambed_hn_demo publication: got %d tables, want %d", len(actual), len(expectedPublished))
	}
	for _, table := range expectedPublished {
		if _, ok := actual[table]; !ok {
			return fmt.Errorf("verify streambed_hn_demo publication: missing %s", table)
		}
	}
	return nil
}

func (s *Store) DropReplicationSlot(ctx context.Context, slotName string) (bool, error) {
	var active bool
	err := s.conn.QueryRow(ctx,
		`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slotName,
	).Scan(&active)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect replication slot %s: %w", slotName, err)
	}
	if active {
		return false, fmt.Errorf("replication slot %s is active; stop Streambed before dropping it", slotName)
	}
	if _, err := s.conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, slotName); err != nil {
		return false, fmt.Errorf("drop replication slot %s: %w", slotName, err)
	}
	return true, nil
}

func (s *Store) Apply(ctx context.Context, lists map[string][]int64, items []Item, observedAt time.Time) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin reconciliation: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, item := range items {
		var createdAt any
		if item.Time > 0 {
			createdAt = time.Unix(item.Time, 0).UTC()
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO stories (
				id, story_type, title, url, author, score, comment_count,
				dead, deleted, created_at, source_updated_at, ingested_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)
			ON CONFLICT (id) DO UPDATE SET
				story_type = EXCLUDED.story_type,
				title = EXCLUDED.title,
				url = EXCLUDED.url,
				author = EXCLUDED.author,
				score = EXCLUDED.score,
				comment_count = EXCLUDED.comment_count,
				dead = EXCLUDED.dead,
				deleted = EXCLUDED.deleted,
				created_at = EXCLUDED.created_at,
				source_updated_at = EXCLUDED.source_updated_at,
				ingested_at = EXCLUDED.ingested_at
			WHERE (stories.story_type, stories.title, stories.url, stories.author,
			       stories.score, stories.comment_count, stories.dead, stories.deleted,
			       stories.created_at)
			  IS DISTINCT FROM
			      (EXCLUDED.story_type, EXCLUDED.title, EXCLUDED.url, EXCLUDED.author,
			       EXCLUDED.score, EXCLUDED.comment_count, EXCLUDED.dead, EXCLUDED.deleted,
			       EXCLUDED.created_at)`,
			item.ID, item.Type, nullIfEmpty(item.Title), nullIfEmpty(item.URL),
			nullIfEmpty(item.By), item.Score, item.Descendants, item.Dead, item.Deleted,
			createdAt, observedAt,
		)
		if err != nil {
			return fmt.Errorf("upsert story %d: %w", item.ID, err)
		}
	}

	for listName, ids := range lists {
		for rank, storyID := range ids {
			_, err := tx.Exec(ctx, `
				INSERT INTO rankings (list_name, story_id, rank, observed_at)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (list_name, story_id) DO UPDATE SET
					rank = EXCLUDED.rank,
					observed_at = EXCLUDED.observed_at
				WHERE rankings.rank IS DISTINCT FROM EXCLUDED.rank`,
				listName, storyID, rank+1, observedAt,
			)
			if err != nil {
				return fmt.Errorf("upsert %s ranking for story %d: %w", listName, storyID, err)
			}
		}
		if len(ids) == 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM rankings WHERE list_name = $1`, listName); err != nil {
				return fmt.Errorf("clear %s rankings: %w", listName, err)
			}
		} else {
			if _, err := tx.Exec(ctx,
				`DELETE FROM rankings WHERE list_name = $1 AND NOT (story_id = ANY($2::bigint[]))`,
				listName, ids,
			); err != nil {
				return fmt.Errorf("remove stale %s rankings: %w", listName, err)
			}
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO front_page (
			story_id, rank, title, url, author, score, comment_count, dead, observed_at
		)
		SELECT s.id, r.rank, s.title, s.url, s.author, s.score, s.comment_count,
		       s.dead OR s.deleted, $1
		FROM rankings AS r
		JOIN stories AS s ON s.id = r.story_id
		WHERE r.list_name = 'top' AND r.rank <= $2
		ON CONFLICT (story_id) DO UPDATE SET
			rank = EXCLUDED.rank,
			title = EXCLUDED.title,
			url = EXCLUDED.url,
			author = EXCLUDED.author,
			score = EXCLUDED.score,
			comment_count = EXCLUDED.comment_count,
			dead = EXCLUDED.dead,
			observed_at = EXCLUDED.observed_at`, observedAt, s.frontPageSize)
	if err != nil {
		return fmt.Errorf("upsert front page: %w", err)
	}
	_, err = tx.Exec(ctx, `
		DELETE FROM front_page
		WHERE story_id NOT IN (
			SELECT story_id
			FROM rankings
			WHERE list_name = 'top' AND rank <= $1
		)`, s.frontPageSize)
	if err != nil {
		return fmt.Errorf("remove stale front page rows: %w", err)
	}

	trackedItems := uniqueListItemCount(lists)
	_, err = tx.Exec(ctx, `
		INSERT INTO ingestion_status (
			source, last_poll_started, last_poll_succeeded, tracked_items
		) VALUES ('hacker-news', $1, clock_timestamp(), $2)
		ON CONFLICT (source) DO UPDATE SET
			last_poll_started = EXCLUDED.last_poll_started,
			last_poll_succeeded = EXCLUDED.last_poll_succeeded,
			tracked_items = EXCLUDED.tracked_items`, observedAt, trackedItems)
	if err != nil {
		return fmt.Errorf("update ingestion status: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reconciliation: %w", err)
	}
	return nil
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func uniqueListItemCount(lists map[string][]int64) int {
	items := make(map[int64]struct{})
	for _, ids := range lists {
		for _, id := range ids {
			items[id] = struct{}{}
		}
	}
	return len(items)
}
