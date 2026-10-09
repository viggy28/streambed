CREATE TABLE public.backfill_status (
    source          TEXT PRIMARY KEY,
    range_start     TIMESTAMPTZ NOT NULL,
    range_end       TIMESTAMPTZ NOT NULL,
    next_start      TIMESTAMPTZ NOT NULL,
    rows_seen       BIGINT NOT NULL DEFAULT 0,
    rows_inserted   BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (range_start <= next_start AND next_start <= range_end)
);

-- Supabase exposes public-schema tables through its Data API. The backfill
-- process uses direct Postgres access; browser/API roles receive no policy.
ALTER TABLE public.backfill_status ENABLE ROW LEVEL SECURITY;

COMMENT ON TABLE public.backfill_status IS
    'Resumable checkpoint for the HN historical seed; intentionally excluded from logical replication.';
