# Maintenance

Streambed includes maintenance commands for Iceberg table metadata and file layout.

## Snapshot expiration

```bash
streambed maintenance \
  --table public.users \
  --retain-last 1 \
  --dry-run
```

Apply snapshot expiration:

```bash
streambed maintenance \
  --table public.users \
  --retain-last 1 \
  --dry-run=false \
  --force
```

Snapshot expiration removes old snapshot references from Iceberg metadata. It does not immediately delete all old data files unless they are part of the expiration plan. Orphan deletion is intentionally dry-run only until safer age/locking support is added.

## Small-file compaction

Frequent CDC flushes can create many small Parquet files. Small-file compaction rewrites clean active data files into fewer target-sized files:

```bash
streambed maintenance compact \
  --table public.users \
  --target-file-size-mb 128 \
  --small-file-threshold-mb 32 \
  --max-input-files 1000 \
  --dry-run
```

Apply compaction:

```bash
streambed maintenance compact \
  --table public.users \
  --target-file-size-mb 128 \
  --small-file-threshold-mb 32 \
  --max-input-files 1000 \
  --dry-run=false \
  --force
```

Compaction currently handles clean active data files only. If a table has active MOR equality delete files, compaction skips the table; use future MOR compaction work to merge delete files into clean data files.

## Safety notes

Online maintenance uses a SQLite-backed table commit lock. This coordinates with `streambed sync` only when both processes use the same `--state-path` and are built from a version that supports the lock.

For online compaction apply mode, make sure these match the running sync daemon:

- `--state-path`
- `--s3-bucket`
- `--s3-prefix`
- `--s3-endpoint` / region configuration

If maintenance is pointed at a different state DB, bucket, or prefix, it may not coordinate with the active sync process or may operate on the wrong table location. A future read-only sync status endpoint will make this validation explicit.

Compaction writes replacement files before publishing new Iceberg metadata. If validation fails before commit, uncommitted outputs may remain as orphan candidates. If a metadata publish result is uncertain, Streambed does not delete newly written files immediately because they may have become referenced by a committed snapshot.
