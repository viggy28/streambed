---
title: Streambed
toc: false
---

<div class="hx-mt-6 hx-mb-6">
{{< hextra/hero-headline >}}
  Postgres-to-Iceberg CDC Engine
{{< /hextra/hero-headline >}}
</div>

<div class="hx-mb-12">
{{< hextra/hero-subtitle >}}
  Offload analytical queries from your production database&nbsp;<br class="sm:hx-block hx-hidden" />without changing your application.
{{< /hextra/hero-subtitle >}}
</div>

<div class="hx-mb-6">
{{< hextra/hero-badge link="https://github.com/viggy28/streambed" >}}
  <span>GitHub</span>
  {{< icon name="arrow-circle-right" attributes="height=14" >}}
{{< /hextra/hero-badge >}}
</div>

<div class="hx-mt-6"></div>

Stream WAL changes via logical replication, write Parquet files to S3, and commit Iceberg metadata. Query the result with any Iceberg-compatible engine -- or use the built-in query server, which speaks the Postgres wire protocol so you can connect with `psql`.

<div class="hx-mt-8"></div>

## Same Query. Same Results. 20x Faster.

pgbench schema -- 1M accounts, 500K history rows. Same correlated subquery, same `psql`:

```
Postgres (port 5432):  ~45s
Streambed (port 5433): ~2s
```

No ETL. No Spark. Just Postgres + S3.

<div class="hx-mt-8"></div>

## How It Works

```
Postgres WAL ──▶ Decode ──▶ Buffer ──▶ Parquet ──▶ S3 ──▶ Iceberg Commit
                                                              │
                                                    DuckDB ◀──┘ (query server)
```

Streambed connects to Postgres as a logical replication subscriber. It decodes WAL messages (inserts, updates, deletes), buffers rows per table, and periodically flushes them as Parquet files to S3 with Iceberg metadata commits. Updates and deletes use copy-on-write merging against existing Parquet data.

A query server exposes Iceberg tables over the Postgres wire protocol using embedded DuckDB, so you can query with `psql` or any Postgres client.

<div class="hx-mt-8"></div>

{{< cards >}}
  {{< card link="docs" title="Documentation" icon="book-open" subtitle="Get started with Streambed" >}}
  {{< card link="blog" title="Blog" icon="newspaper" subtitle="Deep dives and design decisions" >}}
  {{< card link="https://github.com/viggy28/streambed" title="Source Code" icon="code" subtitle="View on GitHub" >}}
{{< /cards >}}
