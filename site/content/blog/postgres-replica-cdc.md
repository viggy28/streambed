---
title: "Stream Postgres changes from a replica"
date: 2026-09-27
authors:
  - name: Vignesh (viggy28)
    link: https://x.com/viggy28
---

Streambed can now read Postgres changes from a hot-standby replica instead of connecting directly to the primary.

Postgres usually has one writable primary. Every application write already passes through that node, so it is the last place you want to add avoidable CPU, I/O, and WAL-decoding work.

<!--more-->

Postgres 16 added support for logical decoding on a standby. Streambed can use that standby as its CDC source while the primary remains responsible for operations that require a writable connection.

```text
Application → Postgres primary → hot standby → Streambed → Parquet
```

This does not remove the cost of CDC. It moves logical decoding and the Streambed replication connection away from the primary's critical path.

## How to use it

Point `--source-url` at the replica and `--primary-url` at the primary:

```bash
./streambed sync \
  --source-url="postgres://user:pass@replica-host:5432/mydb" \
  --primary-url="postgres://user:pass@primary-host:5432/mydb" \
  --s3-bucket=my-bucket \
  --s3-prefix=streambed
```

Streambed splits the work between the two connections:

- `--source-url`: creates or reuses the logical replication slot and streams WAL from the standby.
- `--primary-url`: creates the publication and reads database metadata.

If `--primary-url` is omitted, Streambed continues to use `--source-url` for everything. Existing primary-only deployments do not need to change.

## Requirements

This feature requires **Postgres 16 or newer**. The primary and standby must be configured for logical replication, and the standby should use `hot_standby_feedback` so logical decoding retains the catalog information it needs.

As with any replication setup, choose an appropriate WAL-retention strategy for the physical primary-to-standby connection. Streambed does not create or manage that topology.

See the [replica setup guide](https://github.com/viggy28/streambed/blob/main/docs/replica.md) for configuration and troubleshooting details.

Thanks to [Kartik Prajapati](https://github.com/Kartik1397) for contributing this feature.
