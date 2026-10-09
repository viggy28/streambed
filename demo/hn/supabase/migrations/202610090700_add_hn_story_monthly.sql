CREATE INDEX story_analytics_created_at_idx
    ON public.story_analytics (created_at);

CREATE TABLE public.story_monthly (
    month                 DATE PRIMARY KEY,
    story_count           BIGINT NOT NULL,
    average_score         DOUBLE PRECISION,
    mentions_postgresql   BIGINT NOT NULL,
    mentions_mysql        BIGINT NOT NULL,
    mentions_ai           BIGINT NOT NULL,
    mentions_rust         BIGINT NOT NULL,
    mentions_python       BIGINT NOT NULL
);

ALTER TABLE public.story_monthly ENABLE ROW LEVEL SECURITY;

COMMENT ON TABLE public.story_monthly IS
    'CDC-maintained monthly HN analytics rollup for bounded public queries.';

ALTER PUBLICATION streambed_hn_demo ADD TABLE public.story_monthly;
