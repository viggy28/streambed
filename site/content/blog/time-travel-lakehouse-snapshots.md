---
title: "Why time travel and branching are first-class lakehouse operations"
date: 2026-09-25
authors:
  - name: Vignesh (Viggy) Ravichandran
---

Time travel sounds like an advanced database feature. In an immutable-file system, it is closer to following an old pointer.

A lakehouse normally publishes changes by writing new data files and new metadata rather than modifying already-published files in place. If the old metadata and files are retained, the previous table state is still addressable. Give that state an ID and a query syntax, and you have time travel. Give it a name and allow new commits from it, and you have a branch.

None of this makes history free. It makes history explicit.

<!--more-->

## Why Can't PostgreSQL do that?

Coming from Postgres background, my instinct reaction is MVCC is very similar to this concept. So, why can't Postgres do the same.

An `UPDATE` does not simply overwrite the existing tuple. It creates a new tuple version and marks the old version as superseded by the updating transaction.

Conceptually:

```text
Before UPDATE

(id=42, status='pending')
  xmin=100
  xmax=—

After transaction 120 updates the row

(id=42, status='pending')
  xmin=100
  xmax=120

(id=42, status='shipped')
  xmin=120
  xmax=—
```

A transaction snapshot determines which version is visible. A transaction that began before transaction 120 committed may still see `pending`; a newer transaction sees `shipped`.

So PostgreSQL already retains old physical versions. Why does it not expose a general query such as this?

```sql
SELECT * FROM orders AS OF TIMESTAMP 'yesterday';
```

Because those versions exist for **concurrency**, not as a durable historical product contract.

An MVCC snapshot describes transaction visibility for a running transaction. It is not normally a permanent, named database version. Once no active transaction can see an old tuple, PostgreSQL expects [`VACUUM`](https://www.postgresql.org/docs/current/routine-vacuuming.html) to reclaim its space.

PostgreSQL can export a snapshot so that several live transactions share the same view, but the exporting transaction must remain open. The snapshot is not a token that can be saved and queried next week.

PostgreSQL also supports point-in-time recovery using a base backup and archived WAL. That can reconstruct an earlier database state, but it is a recovery workflow: restore a cluster, replay WAL to a target, start the recovered database, and extract the required data. It is not an ordinary query against the running primary.

## Could PostgreSQL MVCC be turned into time travel?

In principle, yes—but retaining tuples is only the first piece.

A complete temporal query system would also need to provide:

- durable historical snapshot identifiers;
- a mapping from timestamps to transaction visibility;
- retention rules that stop vacuum from removing required tuples;
- historical index behavior;
- transaction-ID lifecycle handling;
- garbage collection after a historical state expires.

Simply disabling vacuum would not provide this. It would mostly provide table and index bloat, increasingly unhappy transaction IDs, and a future operator asking who turned the database into an archaeological site.

This distinction is important:

> PostgreSQL's old tuple versions are implementation state retained while active transactions may need them.

A temporal extension or application-level history table can build durable history on top of PostgreSQL. But that is an additional data model and lifecycle, not a free consequence of MVCC.

## The lakehouse starts from a different unit of change

Parquet itself does not provide time travel, transactions, or branches. It is a file format.

The relevant property is how lakehouse systems use Parquet: once a data file is published, it is normally treated as immutable. Changes produce new files and publish a new metadata state describing which files are active.

That gives the system two layers:

```text
Data layer:      Parquet data files and delete files
Metadata layer:  the file set visible in each table or catalog version
```

Suppose the current snapshot is `S1`:

```text
S1 ──▶ orders-001.parquet
   └─▶ orders-002.parquet
```

An update affects rows stored in `orders-002.parquet`. A copy-on-write implementation can write a replacement and publish `S2`:

```text
S1 ──▶ orders-001.parquet
   └─▶ orders-002.parquet

S2 ──▶ orders-001.parquet
   └─▶ orders-003.parquet
```

The unchanged file is shared. The changed file is replaced in the new snapshot. As long as `S1` and `orders-002.parquet` remain retained, both versions are queryable.

A merge-on-read implementation may instead add a new data file and a delete file that hides the older row. The physical mechanism is different, but the principle is the same: a new metadata state is published without destroying the state required by retained snapshots.

## Time travel is snapshot selection

A historical query does not rebuild the table by replaying every change from the beginning. It selects a retained metadata state and reads the files referenced by it.

For a requested time `T`, the usual timestamp rule is:

```text
select the newest retained snapshot whose commit time is <= T
```

In Streambed, that looks like:

```sql
SELECT id, status, total
FROM orders AT (
  TIMESTAMP => TIMESTAMPTZ '2026-08-10 12:00:00'
)
WHERE id = 42;
```

The timestamp is a lakehouse commit cutoff. It is not a predicate on an `updated_at` column, and it is not necessarily the time of the original PostgreSQL transaction.

Streambed consumes PostgreSQL WAL, buffers changes, and commits them according to its flush thresholds and interval. Two source transactions may appear in one lakehouse snapshot. A large PostgreSQL transaction may also be split across lakehouse snapshots if a table reaches its flush threshold before that transaction finishes.

The historical state is therefore:

> What had Streambed committed to the lakehouse by this time?

It is not a reconstruction of PostgreSQL's exact MVCC snapshot at that wall-clock instant.

## Branching is the next metadata operation

Once snapshots have durable identities, a branch is a natural extension.

Imagine that `main` points to `S2`:

```text
S1 ──▶ S2  ← main
```

Creating an experiment from `S1` does not require copying every Parquet file. A branch can initially point to the same snapshot:

```text
       ┌──▶ S2  ← main
S1 ────┤
       └─────── experiment
```

When the experiment writes data, it creates new files and metadata on a separate lineage:

```text
       ┌──▶ S2 ──▶ S3  ← main
S1 ────┤
       └──▶ E2 ──▶ E3  ← experiment
```

Both lineages can continue sharing unchanged files. The cost grows with the data each branch changes and the history it retains, not necessarily with the full size of the original table.

This is similar to Git: creating a branch is cheap because it starts as another reference to an existing commit. New commits add objects and move that reference. Old objects can be removed only when no retained reference needs them.

Apache Iceberg exposes this model directly through [branches and tags](https://iceberg.apache.org/docs/latest/branching/). Other table formats and catalogs provide different APIs and guarantees, so branching should not be assumed merely because files are immutable. The point is that immutable files plus versioned metadata make branching a natural capability to implement.

Streambed currently exposes timestamp-based historical reads; it does not yet expose user-created lakehouse branches. Branching here describes the architectural consequence, not a current Streambed command.

## First-class does not mean free

Time travel and branching have real costs.

Consider small-file compaction:

```text
Snapshot S1 → files A + B
```

A compaction job combines them into a larger file:

```text
Snapshot S2 → file C
```

Current queries against `S2` can read only `C`, but retaining `S1` means the object store must retain all three files:

```text
S1 needs A + B
S2 needs C
```

Deleting `A` and `B` would break time travel to `S1`. They become reclaimable only after `S1` expires and no branch or tag references them.

Historical retention therefore creates several forms of amplification:

### Storage amplification

Replaced data and delete files remain while retained snapshots reference them. More history and more branches generally mean more stored objects.

### Metadata amplification

Every commit adds snapshot, manifest, or catalog metadata. Long-running streaming workloads can accumulate many versions and require metadata maintenance.

### Read amplification

An older snapshot may reference small files that were later compacted. Current queries benefit from compaction while historical queries still pay the cost of the older layout.

### Write amplification

An update may rewrite a complete file or create both new data and delete files. Immutability moves work away from in-place modification; it does not remove the work.

### Garbage-collection complexity

A file is safe to delete only when it is unreachable from every retained snapshot, branch, and tag. Snapshot expiration and orphan-file cleanup must be conservative, especially while writers are active.

The trade-off resembles PostgreSQL MVCC more than it first appears:

```text
PostgreSQL:
long-lived snapshots → old tuples cannot be vacuumed → heap/index bloat

Lakehouse:
long retention/branches → old files cannot be deleted → object/metadata growth
```

Both systems must decide when an old physical representation is no longer observable. The difference is where that decision appears in the product.

## What makes history first-class?

The defining property is not that old bytes happen to exist. Old PostgreSQL tuples, WAL segments, temporary files, and object-store orphans may all exist without being safely queryable.

History becomes first-class when the system provides:

1. **Identity** — a snapshot ID, timestamp, tag, or branch names a state.
2. **Visibility semantics** — the system defines exactly which rows and files belong to that state.
3. **Queryability** — normal reads can select the state without restoring another database.
4. **Retention policy** — operators control how long the state remains available.
5. **Safe garbage collection** — data is removed only after retained states no longer reference it.
6. **Lineage** — the system records how snapshots and branches relate.

Lakehouse formats do not eliminate the costs of history. They promote these concerns into the table and catalog model.

That is the useful distinction between MVCC history and lakehouse history:

> MVCC keeps old versions so concurrent transactions can finish correctly.
>
> A lakehouse keeps named snapshots so future queries can deliberately return to them.

## History is a storage policy, not magic

Immutable files make time travel and branching natural because new states can share most of their physical data with old states. Metadata chooses which state a reader sees, while retention determines how long that choice remains available.

But every retained possibility has a cost. Old files consume storage. Old layouts may be slower to query. Branches delay garbage collection. Compaction may temporarily increase rather than decrease total storage.

So “first-class” should not be read as “free.” It means something more useful:

> History is explicit, addressable, queryable, and disposable according to policy.

That is the foundation Streambed uses when it turns PostgreSQL changes into lakehouse snapshots—and why a historical query can be a metadata selection instead of a database restore.
