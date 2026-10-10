# Deploy Streambed

The Docker image is the recommended deployment artifact. Pin a version tag in
production; `latest` follows the newest stable release. Images currently target
Linux `amd64`.

## Configuration

Create `/etc/streambed/streambed.env` on the host:

```dotenv
STREAMBED_SOURCE_URL=postgres://streambed:password@postgres.example.com:5432/app
STREAMBED_S3_BUCKET=analytics
STREAMBED_S3_PREFIX=streambed
STREAMBED_S3_REGION=us-east-1
STREAMBED_QUERY_ADDR=:5433
AWS_ACCESS_KEY_ID=replace-me
AWS_SECRET_ACCESS_KEY=replace-me
```

Restrict the file because it contains credentials:

```bash
sudo chmod 600 /etc/streambed/streambed.env
```

Streambed runs as UID `10001`. A named Docker volume preserves the SQLite state
and default DuckLake catalog with the correct ownership:

```bash
docker volume create streambed-state
docker run -d \
  --name streambed \
  --restart unless-stopped \
  --env-file /etc/streambed/streambed.env \
  --volume streambed-state:/var/lib/streambed \
  --publish 5433:5433 \
  ghcr.io/viggy28/streambed:v0.3.0 sync
```

Replace `v0.3.0` with the release being deployed. When using a host directory
instead of a named volume, make it writable by UID `10001` first.

## systemd-managed VM

systemd can supervise the same Docker image. Do not also set Docker's
`--restart` policy in this setup; systemd owns restarts.

```ini
# /etc/systemd/system/streambed.service
[Unit]
Description=Streambed Postgres-to-Iceberg CDC
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
ExecStartPre=-/usr/bin/docker rm streambed
ExecStart=/usr/bin/docker run --rm --name streambed \
  --env-file /etc/streambed/streambed.env \
  --volume streambed-state:/var/lib/streambed \
  --publish 5433:5433 \
  ghcr.io/viggy28/streambed:v0.3.0 sync
ExecStop=/usr/bin/docker stop --time 30 streambed
Restart=always
RestartSec=5
TimeoutStopSec=45

[Install]
WantedBy=multi-user.target
```

After replacing the version with the desired release:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now streambed
sudo systemctl status streambed
```
