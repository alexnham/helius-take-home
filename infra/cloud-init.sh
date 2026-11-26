#!/bin/bash
# First-boot setup, same as the benchmark machine. Progress: /var/log/cloud-init-output.log; done when /root/ready exists.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive HOME=/root

apt-get update -qq
apt-get install -y -qq docker.io build-essential cmake clang pkg-config libssl-dev rsync jq postgresql-client
curl -fsSL https://deb.nodesource.com/setup_24.x | bash -
apt-get install -y -qq nodejs
curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal

# Postgres on localhost only, tuned as in the benchmark. The schema is applied after the code is copied up.
docker run -d --name pg --restart unless-stopped -p 127.0.0.1:55433:5432 -e POSTGRES_PASSWORD=postgres \
  --shm-size=2g postgres:17 -c shared_buffers=4GB -c max_connections=300 -c max_wal_size=16GB

touch /root/ready
