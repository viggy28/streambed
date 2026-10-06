# Hacker News demo

This is the local-first implementation of [issue #108](https://github.com/viggy28/streambed/issues/108). It polls the public Hacker News API, writes current state to Postgres, and lets the real Streambed process replicate and query that state through Iceberg on MinIO.

```text
Local:  HN API -> Postgres -> WAL -> Streambed -> Iceberg/MinIO -> psql
Public: HTTP -> Cloudflare Worker -> on-demand query container -> Iceberg/R2
```

Hacker News data comes from the [public HN API](https://github.com/HackerNews/API). This demo is not operated or endorsed by Y Combinator.

## Run locally

Requirements: Go, Docker with Compose, and `psql`.

```bash
./demo/hn/scripts/start-local.sh
./demo/hn/scripts/smoke-test.sh
```

The startup order is deliberate: create the tables, start Streambed's replication slot, and only then start ingestion. Streambed does not backfill rows that existed before its slot was created.

Useful endpoints:

| Service | Address |
|---|---|
| Source Postgres | `postgres://postgres:test@localhost:55432/hn` |
| Streambed query server | `postgres://demo@localhost:55433/hn?sslmode=disable` |
| MinIO console | <http://localhost:59001> |

Follow the processes:

```bash
tail -f demo/hn/.local/log/hn-ingester.log demo/hn/.local/log/streambed.log
```

Stop while retaining local data:

```bash
./demo/hn/scripts/stop-local.sh
```

Remove all local demo data:

```bash
./demo/hn/scripts/reset-local.sh
```

## Query it

```bash
psql 'postgres://demo@localhost:55433/hn?sslmode=disable'
```

Current front page:

```sql
SELECT rank, title, score, comment_count
FROM front_page
ORDER BY rank
LIMIT 30;
```

Recently changed stories:

```sql
SELECT title, score, comment_count, source_updated_at
FROM stories
ORDER BY source_updated_at DESC
LIMIT 20;
```

List retained front-page snapshots:

```bash
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
./demo/hn/.local/bin/streambed snapshots \
  --table=public.front_page \
  --s3-bucket=streambed \
  --s3-endpoint=http://localhost:59000 \
  --s3-prefix=hn-demo
```

Use one of the reported timestamps:

```sql
SELECT rank, title, score, comment_count
FROM front_page AS f
AT (TIMESTAMP => TIMESTAMPTZ '2026-10-02T06:18:47.89Z')
ORDER BY rank;
```

## Data model

- `stories`: one mutable row per referenced HN item.
- `rankings`: current membership and position in `top`, `best`, `new`, `ask`, `show`, and `jobs`.
- `front_page`: a physical top-30 table. Rank changes are updates, arrivals are inserts, and departures are deletes, so each state is naturally retained by Streambed.
- `ingestion_status`: source-side poll freshness. It is intentionally excluded from Streambed replication for now.

HN does not publish an item update timestamp. `source_updated_at` therefore means “the time this ingester first observed changed source fields.” Extra API fields are ignored safely.

## Ingester configuration

The ingester defaults to 100 entries per list, 16 concurrent item requests, a top-30 front page, and a 30-second poll interval.

```text
HN_DATABASE_URL
HN_API_BASE_URL
HN_POLL_INTERVAL
HN_LIST_LIMIT
HN_ITEM_CONCURRENCY
HN_FRONT_PAGE_SIZE
```

All settings also have command-line flags. `--once` performs one reconciliation; `--migrate-only` creates the schema without contacting HN.

## Deploy the query-only demo on Cloudflare

The public deployment serves Streambed-produced Iceberg snapshots from R2. A Worker starts the query container on the first HTTP request and lets it sleep after one minute of inactivity. There is no public PostgreSQL port; the API accepts SQL over HTTPS.

Requirements: the Cloudflare Workers Paid plan, Docker, Node.js, `curl`, `jq`, and two R2 API tokens scoped to the `streambed-hn-demo` bucket: object read/write for seeding and object read-only for the public query container.

Create the bucket once:

```bash
npx wrangler r2 bucket create streambed-hn-demo
```

Create the two scoped R2 tokens in the Cloudflare dashboard and save their S3 credentials in macOS Keychain without putting them in shell history or source control:

```bash
# Read/write token used only by the seed/sync job.
security add-generic-password -U \
  -a streambed-hn-demo \
  -s streambed-r2-writer-access-key-id \
  -w
security add-generic-password -U \
  -a streambed-hn-demo \
  -s streambed-r2-writer-secret-access-key \
  -w

# Read-only token passed to the public query container.
security add-generic-password -U \
  -a streambed-hn-demo \
  -s streambed-r2-reader-access-key-id \
  -w
security add-generic-password -U \
  -a streambed-hn-demo \
  -s streambed-r2-reader-secret-access-key \
  -w
```

Seed two snapshots directly into R2 and verify current and historical HTTP queries:

```bash
./demo/hn/scripts/seed-r2.sh
```

Build and deploy the Worker and container, then install the two R2 credentials as encrypted Worker secrets:

```bash
./demo/hn/scripts/deploy-cloudflare.sh
```

Query the deployed URL:

```bash
export STREAMBED_DEMO_QUERY_URL='https://demo.streambed.dev/query'
./demo/hn/scripts/smoke-test-http.sh
```

A request body has the form `{"sql":"SELECT * FROM front_page LIMIT 10"}`. The Worker permits ten queries per client IP per minute. The container is restricted to one read-only statement, 10 seconds, 1,000 rows, 8 MiB of results, and 128 MiB of DuckDB memory. External access is restricted to the demo's Iceberg prefix plus DuckDB's extension cache.

## Supabase source

Streambed must use the project's direct Postgres connection with TLS, not Supavisor transaction pooling, because logical replication requires a persistent connection. The Streambed host also needs IPv6 connectivity unless the Supabase project has IPv4 connectivity enabled.

First apply `supabase/migrations/202610020642_create_streambed_hn_demo_source.sql` through Supabase MCP's `apply_migration` tool or the dashboard SQL editor. The startup script verifies that all four tables have RLS enabled and that the publication contains exactly the three CDC tables; it will not silently create an insecure schema.

On macOS, save the project database password in Keychain without putting it in source code:

```bash
security add-generic-password -U \
  -a streambed-hn-demo \
  -s streambed-supabase-db-password \
  -w
```

Then run Supabase as the source while keeping Streambed and MinIO local:

```bash
# Override this when using a different Supabase project.
export SUPABASE_PROJECT_REF=tvnljlooazyxocdtovvy

./demo/hn/scripts/start-supabase-source.sh
./demo/hn/scripts/smoke-test.sh
```

This local helper uses the project `postgres` role. Its password is inherited by the two daemon processes, so use dedicated least-privilege roles before production deployment.

Stopping the local processes intentionally leaves the remote replication slot available for restart. If the demo is being abandoned, stop Streambed and explicitly remove the slot so Supabase cannot retain WAL indefinitely:

```bash
./demo/hn/scripts/stop-local.sh
./demo/hn/scripts/cleanup-supabase-slot.sh
```

`reset-local.sh` only removes local state; it does not remove Supabase tables or replication slots. Do not reset or move the lake while reusing an advanced slot: Streambed does not backfill rows that predate the target.

Periodic hosted ingestion, the custom domain, and the landing page remain deferred. The next deployment phase is a scheduled Cloudflare sync container that catches up from Supabase, flushes, and exits.
