CREATE TABLE public.stories (
    id                BIGINT PRIMARY KEY,
    story_type        TEXT NOT NULL,
    title             TEXT,
    url               TEXT,
    author             TEXT,
    score             BIGINT,
    comment_count     BIGINT,
    dead              BOOLEAN NOT NULL DEFAULT FALSE,
    deleted           BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ,
    source_updated_at TIMESTAMPTZ NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE public.rankings (
    list_name   TEXT NOT NULL,
    story_id    BIGINT NOT NULL,
    rank        INTEGER NOT NULL CHECK (rank > 0),
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (list_name, story_id)
);

CREATE TABLE public.front_page (
    story_id      BIGINT PRIMARY KEY,
    rank          INTEGER NOT NULL CHECK (rank > 0),
    title         TEXT,
    url           TEXT,
    author        TEXT,
    score         BIGINT,
    comment_count BIGINT,
    dead          BOOLEAN NOT NULL DEFAULT FALSE,
    observed_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE public.ingestion_status (
    source              TEXT PRIMARY KEY,
    last_poll_started   TIMESTAMPTZ NOT NULL,
    last_poll_succeeded TIMESTAMPTZ NOT NULL,
    tracked_items       INTEGER NOT NULL CHECK (tracked_items >= 0)
);

-- Supabase exposes public-schema tables through its Data API. No policies are
-- intentional: browser/API roles cannot read or mutate the CDC source. The
-- direct Postgres roles used by the ingester and Streambed bypass RLS.
ALTER TABLE public.stories ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.rankings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.front_page ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ingestion_status ENABLE ROW LEVEL SECURITY;

COMMENT ON COLUMN public.stories.source_updated_at IS
    'Time the demo ingester first observed changed fields from the Hacker News API.';
COMMENT ON TABLE public.front_page IS
    'Physical top-30 Hacker News state used for Streambed snapshot time travel.';

CREATE PUBLICATION streambed_hn_demo
    FOR TABLE public.stories, public.rankings, public.front_page;
