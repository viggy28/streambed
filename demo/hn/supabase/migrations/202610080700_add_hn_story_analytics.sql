CREATE TABLE public.story_analytics (
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

ALTER TABLE public.story_analytics ENABLE ROW LEVEL SECURITY;

COMMENT ON TABLE public.story_analytics IS
    'Narrow, query-optimized story facts derived during HN ingestion for the public analytics demo.';

ALTER PUBLICATION streambed_hn_demo ADD TABLE public.story_analytics;
