#!/usr/bin/env bash
# Run the 3-shard (NUM_SERVERS=3) distributed ladybug-graph algorithm demo on Rivet.
# All progress logs go to stderr; engine logs live in ~/.rivetkit/var/logs/rivet-engine/.
set -euo pipefail
cd "$(dirname "$0")/.."

db=/tmp/foo.db
url=http://127.0.0.1:8123
export RIVET_ENGINE_BINARY_PATH=$PWD/target/release/rivet-engine
export RUST_LOG="${RUST_LOG:-info}"

# The LadybugDB server is the only process that owns the graph file; workers connect over HTTP.
# Recreate + seed a clean store: remove the file (the server recreates it on open).
rm -f "$db" "$db.wal" "$db.shm"

./target/release/ladybug-server --db "$db" --listen 127.0.0.1:8123 &
LADYBUG_SERVER_PID=$!
trap 'kill "$LADYBUG_SERVER_PID" 2>/dev/null || true' EXIT

# Wait for the server to accept connections before seeding.
for _ in $(seq 1 50); do
  curl -fsS "$url/health" >/dev/null 2>&1 && break
  sleep 0.1
done

# Seed through the columnar protocol (the seed client is remote, not shared-file).
./target/release/server seed "$url" 2
echo "hosting worker + coordinator actors (GRAPH_AUTO_RUN defaults to on)..."

# Host the actors; the server auto-triggers runAlgorithm and prints superstep progress.
LADYBUG_DB=$url NUM_SERVERS=3 GRAPH_K=2 GRAPH_ALGO=1 GRAPH_RUN_ID=1 ./target/release/server
