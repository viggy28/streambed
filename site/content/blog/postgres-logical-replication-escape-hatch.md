---
title: "Logical Replication Is Postgres's Most Important Feature"
date: 2026-09-26
authors:
  - name: viggy28
    link: https://x.com/viggy28
---

Logical replication might be one of the most important features in Postgres.

But not because it gives you another copy of your database.

Because it gives you a way out.

<!--more-->

I came to this view after building and leading the Postgres platform at Cloudflare from 2019 to 2023, and co-founding [Omnigres](https://github.com/omnigres/omnigres), which explored Postgres extensions as an application runtime. I spent a lot of time thinking about a related problem: how can you build a sustainable company in the Postgres ecosystem without first persuading customers to replace the database at the center of their architecture?

Your Postgres database may start as a single AWS, Azure, or Google Cloud resource. Over time, the provider's identity system, private network, and operational tooling grow around it.

Five years later, moving the database means moving half your architecture.

A backup proves that you possess the data. It does not prove that you can move a live system built around that data. Restore a backup somewhere else and it begins going stale as soon as production receives its next write.

Logical replication changes that constraint. It lets another system begin with the current state of the database and then continue receiving changes while production remains online.

That is more than replication. It is optionality.

## The production database is almost impossible to replace

In a [conversation with Auren Hoffman](https://www.buzzsprout.com/1783651/episodes/16211813-fivetran-ceo-george-fraser-data-for-the-ai-revolution), Fivetran CEO George Fraser put it bluntly. [I wrote about the line when I first heard it](https://www.linkedin.com/posts/vigneshravichandran28_the-only-way-to-change-your-production-database-activity-7494086027423154176-Y1Mh):

> The only way to change your production database is to start a new company.

The statement is deliberately extreme, but the underlying point is right. A production database is not just software that stores rows. It sits underneath application behavior, operational procedures, compliance controls, and years of assumptions nobody has written down.

This creates an awkward problem for a database startup. A new system can be dramatically better at one workload and still fail because adopting it requires a risky migration. The technical product may be good, but the first step asks the customer to move the most sensitive component in their architecture.

Logical replication offers a different path: do not replace Postgres on day one. Attach to it.

The existing database remains the system of record. The new product reads a consistent snapshot and then follows committed changes from the write-ahead log (WAL). It can prove its value on real production data without asking the application to switch databases or dual-write every transaction.

For an early-stage infrastructure company, this is not a minor implementation detail. It changes the economics of adoption.

## One Postgres feature created an entire product surface

Every durable Postgres change is recorded in WAL. Physical replication interprets that log in terms of Postgres storage. Logical decoding interprets it as changes to data: a row was inserted, updated, or deleted. Native logical replication uses this mechanism between Postgres databases; tools such as Debezium and PeerDB consume the underlying logical change stream.

A replication slot remembers how far a consumer has read. An output plugin such as Postgres's built-in `pgoutput` turns WAL records into a stream the consumer can understand. The consumer can then apply those changes to another Postgres database—or translate them into a completely different system.

```text
                         ┌──▶ another Postgres
                         │
Postgres WAL ──▶ logical decoding ──▶ Kafka / event consumers
                         │
                         ├──▶ warehouse or query engine
                         │
                         └──▶ Parquet + open table format
```

This is the important architectural boundary. The source continues to behave like Postgres. The destination does not need to share its storage format, query engine, release cycle, or cloud provider.

That boundary creates room for products that are not trying to become the next primary database:

- analytical systems can remove expensive queries from production;
- search systems can maintain an index without polling tables;
- event platforms can turn committed changes into streams;
- security products can inspect a durable record of data changes;
- migration products can keep a destination current until cutover;
- developer tools can build derived views without changing application writes.

The startup gets a narrow entry point into an existing architecture. The customer gets to evaluate the product without making an irreversible decision.

## Debezium, PeerDB, and the products built at this seam

[Debezium](https://debezium.io/documentation/reference/stable/connectors/postgresql.html) turns Postgres changes into an event stream. It takes an initial consistent snapshot, continues from the corresponding WAL position, and publishes row-level changes into Kafka-compatible infrastructure. The applications consuming those events do not need to understand Postgres WAL.

[PeerDB](https://github.com/PeerDB-io/peerdb) uses the same opening to replicate Postgres data into analytical systems. Its product value is not merely reading WAL. It is everything required to make that stream useful at the destination: initial copies, type mapping, batching, and applying updates and deletes efficiently.

[StreamBed](https://github.com/viggy28/streambed) uses logical replication to write Postgres changes as Parquet with Iceberg or DuckLake metadata. It is one more example of the pattern: Postgres remains the transactional system, while the same data becomes available in an open analytical representation on object storage.

These projects produce very different things—events, warehouse tables, and lakehouse files—but they share the same adoption model:

1. meet the customer where their data already lives;
2. begin as a downstream system rather than a replacement;
3. prove value using real workloads;
4. avoid requiring an application rewrite.

Logical replication is the distribution channel hidden inside the database.

## An escape hatch from the cloud data stack

Cloud lock-in is rarely one proprietary API. It is the accumulation of small dependencies until leaving requires every component to move at once.

The data layer has the strongest gravity. Compute can often be redeployed elsewhere, but a database is large, stateful, and continuously changing. Logical replication weakens that gravity by allowing the data plane to cross the provider boundary before the rest of the architecture moves.

A company can keep its managed Postgres instance running while it creates a live copy in another provider. It can send changes to an independently operated analytics system instead of adopting the provider's warehouse. It can materialize data into an open format on object storage. It can test self-hosted infrastructure against current production data rather than a stale benchmark dump.

The move becomes gradual:

```text
One provider owns the whole path

Application ──▶ Managed Postgres ──▶ Provider analytics

                         ↓ logical replication

Application ──▶ Managed Postgres ──▶ Independent or open systems
                                            │
                                            └──▶ another cloud, later
```

Logical replication does not migrate IAM policies, networks, extensions, or operational knowledge. What it changes is timing. Those pieces no longer have to move in the same maintenance window as the data.

It also changes negotiating power. An exit does not need to begin with a high-risk cutover. It can begin with a replication slot and a consumer.

## Open source is not enough if the data cannot leave

We often describe Postgres as open because the source is available and the database can run almost anywhere. That is necessary, but it is not the complete test.

The meaningful test arrives years later, after the database contains terabytes of data and sits at the center of the company. Can another system consume that data continuously? Can you run the old and new architectures side by side? Can you leave without stopping the business for the duration of a bulk copy?

Logical replication does not eliminate the hard work of migration. It creates the period of coexistence in which that work becomes possible.

It also gives new infrastructure startups a realistic way to enter the Postgres ecosystem. They can complement the production database before asking to replace any part of the stack. Some may never replace it at all. They can build value downstream while preserving Postgres as the stable transactional core.

Perhaps one of the most important properties of an open database is not simply that you can run it anywhere.

It is that you—and the products built around your data—have a way out.
