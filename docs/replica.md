# Connecting to a Postgres Replica

Streambed supports streaming WAL from a Postgres **hot-standby replica** (Postgres 16+) instead of the primary. This is recommended for production setups where you want to protect primary resources from the overhead of logical replication.

## Why use a replica?

Logical replication adds CPU and I/O load to whichever server Streambed connects to. More importantly, a replication slot prevents Postgres from discarding WAL segments that haven't been consumed yet. If Streambed slows down or loses its connection, WAL accumulates on that server — consuming disk space and potentially destabilising it under sustained lag.

Pointing `--source-url` at a replica contains this risk to the standby. The logical slot is created and lives directly on the standby (supported since Postgres 16), ensuring that replication pressure and WAL retention costs are borne by the standby.

## Requirements

- **Postgres 16+** on both primary and replica — logical replication from a hot-standby was stabilised in Postgres 16.
- The replica must be configured with:
  - `hot_standby = on`
  - `wal_level = logical` — inherited automatically from the primary when using streaming replication.
  - `max_wal_senders` and `max_replication_slots` ≥ the primary's values (Postgres enforces this on standby startup).
- The primary's `pg_hba.conf` must allow replication connections from the replica's IP.

## How it works

Streambed splits its Postgres connections by responsibility:

| Connection | Target | Used for |
|---|---|---|
| `--source-url` | Replica (hot-standby) | `CREATE_REPLICATION_SLOT` and WAL streaming |
| `--primary-url` | Primary | `CREATE PUBLICATION` and metadata queries |

On startup, Streambed connects to the **primary** to create the publication. It then opens a separate replication connection to the **replica** to create the logical replication slot and start streaming WAL.

## Usage

```bash
./streambed sync \
  --source-url="postgres://user:pass@replica-host:5432/mydb" \
  --primary-url="postgres://user:pass@primary-host:5432/mydb" \
  --s3-bucket=my-bucket \
  --s3-endpoint=http://localhost:9000 \
  --s3-prefix=myprefix \
  --query-addr=:5433
```

Environment variable equivalents:

```bash
export STREAMBED_SOURCE_URL="postgres://user:pass@replica-host:5432/mydb"
export STREAMBED_PRIMARY_URL="postgres://user:pass@primary-host:5432/mydb"
```

When `--primary-url` is omitted, Streambed uses `--source-url` for all operations — the existing behaviour for setups connected directly to a primary.

## Local test setup

A Docker Compose file that spins up a primary + hot-standby replica is provided for testing:

```bash
# Start primary (port 5434), replica (port 5435), and MinIO (port 9002)
docker compose -f test/integration/docker-compose-replica.yml up -d --wait

# Run replica-specific integration tests
go test -tags integration -v -run TestReplica -timeout 120s ./test/integration/...

# Tear down
docker compose -f test/integration/docker-compose-replica.yml down -v
```

## Troubleshooting

**`ERROR: cannot execute CREATE PUBLICATION in a read-only transaction (SQLSTATE 25006)`**

You pointed `--source-url` at a replica without setting `--primary-url`. Publication creation must run on the writable primary. Add `--primary-url` pointing at your primary.

