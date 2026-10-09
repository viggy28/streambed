package ingester

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
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
	expectedTables := []string{"stories", "story_analytics", "story_monthly", "story_leaders", "rankings", "front_page", "ingestion_status", "backfill_status"}
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
	expectedPublished := []string{"public.stories", "public.story_analytics", "public.story_monthly", "public.story_leaders", "public.rankings", "public.front_page"}
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

func writeStoryAnalytics(ctx context.Context, tx pgx.Tx, item Item, createdAt any, update bool) error {
	postgresql := containsASCIIWord(item.Title, "postgres") || containsASCIIWord(item.Title, "postgresql")
	query := `
		INSERT INTO story_analytics (
			story_id, created_at, score, comment_count, mentions_postgresql,
			mentions_mysql, mentions_ai, mentions_rust, mentions_python
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (story_id) DO NOTHING`
	if update {
		query = `
			INSERT INTO story_analytics (
				story_id, created_at, score, comment_count, mentions_postgresql,
				mentions_mysql, mentions_ai, mentions_rust, mentions_python
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (story_id) DO UPDATE SET
				created_at = EXCLUDED.created_at,
				score = EXCLUDED.score,
				comment_count = EXCLUDED.comment_count,
				mentions_postgresql = EXCLUDED.mentions_postgresql,
				mentions_mysql = EXCLUDED.mentions_mysql,
				mentions_ai = EXCLUDED.mentions_ai,
				mentions_rust = EXCLUDED.mentions_rust,
				mentions_python = EXCLUDED.mentions_python
			WHERE (story_analytics.created_at, story_analytics.score,
			       story_analytics.comment_count, story_analytics.mentions_postgresql,
			       story_analytics.mentions_mysql, story_analytics.mentions_ai,
			       story_analytics.mentions_rust, story_analytics.mentions_python)
			  IS DISTINCT FROM
			      (EXCLUDED.created_at, EXCLUDED.score, EXCLUDED.comment_count,
			       EXCLUDED.mentions_postgresql, EXCLUDED.mentions_mysql,
			       EXCLUDED.mentions_ai, EXCLUDED.mentions_rust,
			       EXCLUDED.mentions_python)`
	}
	_, err := tx.Exec(ctx, query,
		item.ID, createdAt, item.Score, item.Descendants, postgresql,
		containsASCIIWord(item.Title, "mysql"), containsASCIIWord(item.Title, "ai"),
		containsASCIIWord(item.Title, "rust"), containsASCIIWord(item.Title, "python"),
	)
	return err
}

const storyLeaderCommentThreshold = 500

func writeStoryLeader(ctx context.Context, tx pgx.Tx, item Item, createdAt any, update bool) error {
	if item.Title == "" || item.Descendants < storyLeaderCommentThreshold || item.Dead || item.Deleted {
		if update {
			if _, err := tx.Exec(ctx, `DELETE FROM story_leaders WHERE story_id = $1`, item.ID); err != nil {
				return err
			}
		}
		return nil
	}
	query := `
		INSERT INTO story_leaders (story_id, created_at, title, score, comment_count)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (story_id) DO NOTHING`
	if update {
		query = `
			INSERT INTO story_leaders (story_id, created_at, title, score, comment_count)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (story_id) DO UPDATE SET
				created_at = EXCLUDED.created_at,
				title = EXCLUDED.title,
				score = EXCLUDED.score,
				comment_count = EXCLUDED.comment_count
			WHERE (story_leaders.created_at, story_leaders.title,
			       story_leaders.score, story_leaders.comment_count)
			  IS DISTINCT FROM
			      (EXCLUDED.created_at, EXCLUDED.title,
			       EXCLUDED.score, EXCLUDED.comment_count)`
	}
	_, err := tx.Exec(ctx, query, item.ID, createdAt, item.Title, item.Score, item.Descendants)
	return err
}

func refreshStoryMonths(ctx context.Context, tx pgx.Tx, items []Item) error {
	months := make(map[time.Time]struct{})
	for _, item := range items {
		if item.Time <= 0 {
			continue
		}
		createdAt := time.Unix(item.Time, 0).UTC()
		month := time.Date(createdAt.Year(), createdAt.Month(), 1, 0, 0, 0, 0, time.UTC)
		months[month] = struct{}{}
	}
	for month := range months {
		nextMonth := month.AddDate(0, 1, 0)
		_, err := tx.Exec(ctx, `
			INSERT INTO story_monthly (
				month, story_count, average_score, mentions_postgresql,
				mentions_mysql, mentions_ai, mentions_rust, mentions_python
			)
			SELECT
				$1::date,
				count(*),
				avg(score)::double precision,
				count(*) FILTER (WHERE mentions_postgresql),
				count(*) FILTER (WHERE mentions_mysql),
				count(*) FILTER (WHERE mentions_ai),
				count(*) FILTER (WHERE mentions_rust),
				count(*) FILTER (WHERE mentions_python)
			FROM story_analytics
			WHERE created_at >= $2 AND created_at < $3
			HAVING count(*) > 0
			ON CONFLICT (month) DO UPDATE SET
				story_count = EXCLUDED.story_count,
				average_score = EXCLUDED.average_score,
				mentions_postgresql = EXCLUDED.mentions_postgresql,
				mentions_mysql = EXCLUDED.mentions_mysql,
				mentions_ai = EXCLUDED.mentions_ai,
				mentions_rust = EXCLUDED.mentions_rust,
				mentions_python = EXCLUDED.mentions_python
			WHERE (story_monthly.story_count, story_monthly.average_score,
			       story_monthly.mentions_postgresql, story_monthly.mentions_mysql,
			       story_monthly.mentions_ai, story_monthly.mentions_rust,
			       story_monthly.mentions_python)
			  IS DISTINCT FROM
			      (EXCLUDED.story_count, EXCLUDED.average_score,
			       EXCLUDED.mentions_postgresql, EXCLUDED.mentions_mysql,
			       EXCLUDED.mentions_ai, EXCLUDED.mentions_rust,
			       EXCLUDED.mentions_python)`,
			month.Format(time.DateOnly), month, nextMonth,
		)
		if err != nil {
			return fmt.Errorf("refresh story analytics for %s: %w", month.Format("2006-01"), err)
		}
	}
	return nil
}

func containsASCIIWord(value, word string) bool {
	value = strings.ToLower(value)
	for offset := 0; offset <= len(value)-len(word); {
		index := strings.Index(value[offset:], word)
		if index < 0 {
			return false
		}
		index += offset
		end := index + len(word)
		leftBoundary := index == 0 || !isASCIIWordByte(value[index-1])
		rightBoundary := end == len(value) || !isASCIIWordByte(value[end])
		if leftBoundary && rightBoundary {
			return true
		}
		offset = index + 1
	}
	return false
}

func isASCIIWordByte(value byte) bool {
	return (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '_'
}

func (s *Store) PrepareBackfill(ctx context.Context, source string, start, end time.Time) (time.Time, error) {
	_, err := s.conn.Exec(ctx, `
		INSERT INTO backfill_status (source, range_start, range_end, next_start)
		VALUES ($1, $2, $3, $2)
		ON CONFLICT (source) DO NOTHING`, source, start, end)
	if err != nil {
		return time.Time{}, fmt.Errorf("initialize backfill checkpoint: %w", err)
	}
	var storedStart, storedEnd, next time.Time
	if err := s.conn.QueryRow(ctx, `
		SELECT range_start, range_end, next_start
		FROM backfill_status
		WHERE source = $1`, source).Scan(&storedStart, &storedEnd, &next); err != nil {
		return time.Time{}, fmt.Errorf("read backfill checkpoint: %w", err)
	}
	if !storedStart.Equal(start) || !storedEnd.Equal(end) {
		return time.Time{}, fmt.Errorf("backfill %q already uses range %s to %s", source, storedStart.UTC().Format(time.RFC3339), storedEnd.UTC().Format(time.RFC3339))
	}
	return next.UTC(), nil
}

func (s *Store) ApplyHistoricalWindow(ctx context.Context, source string, windowStart, windowEnd time.Time, items []Item, observedAt time.Time) (int64, error) {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin historical window: %w", err)
	}
	defer tx.Rollback(ctx)

	var inserted int64
	for _, item := range items {
		var createdAt any
		if item.Time > 0 {
			createdAt = time.Unix(item.Time, 0).UTC()
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO stories (
				id, story_type, title, url, author, score, comment_count,
				dead, deleted, created_at, source_updated_at, ingested_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)
			ON CONFLICT (id) DO NOTHING`,
			item.ID, item.Type, nullIfEmpty(item.Title), nullIfEmpty(item.URL),
			nullIfEmpty(item.By), item.Score, item.Descendants, item.Dead, item.Deleted,
			createdAt, observedAt,
		)
		if err != nil {
			return 0, fmt.Errorf("insert historical story %d: %w", item.ID, err)
		}
		inserted += tag.RowsAffected()
		if err := writeStoryAnalytics(ctx, tx, item, createdAt, false); err != nil {
			return 0, fmt.Errorf("insert historical story analytics %d: %w", item.ID, err)
		}
		if err := writeStoryLeader(ctx, tx, item, createdAt, false); err != nil {
			return 0, fmt.Errorf("insert historical story leader %d: %w", item.ID, err)
		}
	}
	if err := refreshStoryMonths(ctx, tx, items); err != nil {
		return 0, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE backfill_status
		SET next_start = $1,
		    rows_seen = rows_seen + $2,
		    rows_inserted = rows_inserted + $3,
		    updated_at = clock_timestamp()
		WHERE source = $4 AND next_start = $5`,
		windowEnd, len(items), inserted, source, windowStart)
	if err != nil {
		return 0, fmt.Errorf("advance backfill checkpoint: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return 0, fmt.Errorf("backfill checkpoint changed concurrently; expected next window at %s", windowStart.UTC().Format(time.RFC3339))
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit historical window: %w", err)
	}
	return inserted, nil
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
		if err := writeStoryAnalytics(ctx, tx, item, createdAt, true); err != nil {
			return fmt.Errorf("upsert story analytics %d: %w", item.ID, err)
		}
		if err := writeStoryLeader(ctx, tx, item, createdAt, true); err != nil {
			return fmt.Errorf("upsert story leader %d: %w", item.ID, err)
		}
	}
	if err := refreshStoryMonths(ctx, tx, items); err != nil {
		return err
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
