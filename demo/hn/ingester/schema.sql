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
    source             TEXT PRIMARY KEY,
    last_poll_started  TIMESTAMPTZ NOT NULL,
    last_poll_succeeded TIMESTAMPTZ NOT NULL,
    tracked_items      INTEGER NOT NULL
);
