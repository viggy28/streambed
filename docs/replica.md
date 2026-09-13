# Connecting to a Postgres Replica

Streambed supports streaming WAL from a Postgres **hot-standby replica** (Postgres 16+) instead of the primary. This is recommended for production setups where you want to protect primary resources from the overhead of logical replication.

## Why use a replica?

Logical replication adds CPU and I/O load to whichever server Streambed connects to. More importantly, a replication slot prevents Postgres from discarding WAL segments that haven't been consumed yet. If Streambed slows down or loses its connection, WAL accumulates on that server — consuming disk space and potentially destabilising it under sustained lag.

Pointing `--source-url` at a replica contains this risk to the standby. The slot still lives on the primary (Postgres requires this), but the replication pressure and WAL retention cost are borne by the standby.

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
| `--source-url` | Replica (hot-standby) | WAL streaming only |
| `--primary-url` | Primary | `CREATE PUBLICATION`, `CREATE_REPLICATION_SLOT`, metadata queries |

On startup, Streambed connects to the **primary** to create the publication and replication slot (these are write operations; standbys reject them with `ERROR 25006`). It then opens a separate replication connection to the **replica** and starts streaming WAL from the existing slot.

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

You pointed `--source-url` at a replica without setting `--primary-url`. Publication and slot creation must run on the writable primary. Add `--primary-url` pointing at your primary.

**`FATAL: recovery aborted because of insufficient parameter settings`**

The replica's `max_wal_senders` or `max_replication_slots` is lower than the primary's. Set both to at least the primary's values in the replica's Postgres config.

**`FATAL: no pg_hba.conf entry for replication connection`**

The primary's `pg_hba.conf` does not allow the replica to connect for replication (needed for `pg_basebackup` and WAL shipping). Add a rule:

```
host  replication  all  <replica-ip>/32  scram-sha-256
```

**Slot is not found on the replica**

Replication slots created on the primary are visible to the replica only after the replica has streamed past the LSN at which the slot was created. Wait a few seconds for the replica to catch up, then retry.
