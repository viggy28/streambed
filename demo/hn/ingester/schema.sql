CREATE TABLE IF NOT EXISTS stories (
    id                BIGINT PRIMARY KEY,
    story_type        TEXT NOT NULL,
    title             TEXT,
    url               TEXT,
    author            TEXT,
    score             BIGINT,
    comment_count     BIGINT,
    dead              BOOLEAN NOT NULL DEFAULT FALSE,
    deleted           BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ,
    source_updated_at TIMESTAMPTZ NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS story_analytics (
    story_id             BIGINT PRIMARY KEY,
    created_at           TIMESTAMPTZ,
    score                BIGINT,
    comment_count        BIGINT,
    mentions_postgresql  BOOLEAN NOT NULL,
    mentions_mysql       BOOLEAN NOT NULL,
    mentions_ai          BOOLEAN NOT NULL,
    mentions_rust        BOOLEAN NOT NULL,
    mentions_python      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS rankings (
    list_name   TEXT NOT NULL,
    story_id    BIGINT NOT NULL,
    rank        INTEGER NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (list_name, story_id)
);

-- This is intentionally a table rather than a view. Streambed captures each
-- front-page state through ordinary INSERT, UPDATE, and DELETE WAL records,
-- which makes the table straightforward to query with snapshot time travel.
CREATE TABLE IF NOT EXISTS front_page (
    story_id     BIGINT PRIMARY KEY,
    rank         INTEGER NOT NULL,
    title        TEXT,
    url          TEXT,
    author       TEXT,
    score        BIGINT,
    comment_count BIGINT,
    dead         BOOLEAN NOT NULL DEFAULT FALSE,
    observed_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_status (
    source              TEXT PRIMARY KEY,
    last_poll_started   TIMESTAMPTZ NOT NULL,
    last_poll_succeeded TIMESTAMPTZ NOT NULL,
    tracked_items       INTEGER NOT NULL
);

-- Operational checkpoint only. This table is deliberately excluded from the
-- Streambed publication because it is not part of the public analytical data.
CREATE TABLE IF NOT EXISTS backfill_status (
    source          TEXT PRIMARY KEY,
    range_start     TIMESTAMPTZ NOT NULL,
    range_end       TIMESTAMPTZ NOT NULL,
    next_start      TIMESTAMPTZ NOT NULL,
    rows_seen       BIGINT NOT NULL DEFAULT 0,
    rows_inserted   BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (range_start <= next_start AND next_start <= range_end)
);
