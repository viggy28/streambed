---
title: Streambed
toc: false
---

<div class="hx-mt-6 hx-mb-6">
{{< hextra/hero-headline >}}
  Your Postgres,&nbsp;<br class="sm:hx-block hx-hidden" />20× faster for analytics.
{{< /hextra/hero-headline >}}
</div>

<div class="hx-mb-12">
{{< hextra/hero-subtitle >}}
  Streambed streams your WAL into Iceberg tables on S3.&nbsp;<br class="sm:hx-block hx-hidden" />Query them with `psql` at DuckDB speed — your data stays in open formats, in your bucket.
{{< /hextra/hero-subtitle >}}
</div>

<div class="hx-mb-6">
{{< hextra/hero-badge link="https://github.com/viggy28/streambed" >}}
  <span>GitHub</span>
  {{< icon name="arrow-circle-right" attributes="height=14" >}}
{{< /hextra/hero-badge >}}
</div>

<div class="hx-mt-10"></div>

![Same analytical query. Postgres on the left, Streambed on the right.](demo.gif)

<div class="hx-mt-10"></div>

## Built on open standards. No vendors.

Every piece of streambed is something you can inspect, swap, or run yourself:

- **Apache Iceberg** — your tables live in an open table format, queryable by any Iceberg-compatible engine.
- **DuckDB** — the columnar engine doing the heavy lifting, embedded directly in the query server.
- **Parquet on your S3** — your data sits in your bucket, in an open columnar format. No warehouse to provision.
- **Postgres wire protocol** — connect with `psql`, `pgcli`, or any tool that already speaks Postgres.

No Snowflake. No Databricks. No BigQuery. The point of streambed is to show how far you can push open source and object storage before you need any of them.

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
