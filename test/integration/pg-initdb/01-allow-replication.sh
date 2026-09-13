#!/bin/bash
# Appended to pg_hba.conf after initdb so the replica can connect for streaming.
set -e
echo "host  replication  all  0.0.0.0/0  trust" >> "$PGDATA/pg_hba.conf"
echo "host  replication  all  ::/0       trust" >> "$PGDATA/pg_hba.conf"
echo "pg_hba.conf updated to allow replication from any host."
