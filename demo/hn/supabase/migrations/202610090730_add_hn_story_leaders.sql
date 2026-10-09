CREATE TABLE public.story_leaders (
    story_id       BIGINT PRIMARY KEY,
    created_at     TIMESTAMPTZ,
    title          TEXT NOT NULL,
    score          BIGINT,
    comment_count  BIGINT NOT NULL
);

CREATE INDEX story_leaders_created_at_idx
    ON public.story_leaders (created_at);

ALTER TABLE public.story_leaders ENABLE ROW LEVEL SECURITY;

COMMENT ON TABLE public.story_leaders IS
    'CDC-maintained HN stories with at least 500 comments for bounded public leader queries.';

ALTER PUBLICATION streambed_hn_demo ADD TABLE public.story_leaders;
