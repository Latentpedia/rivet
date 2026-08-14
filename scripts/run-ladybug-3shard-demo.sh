#!/usr/bin/env bash
# Run the 3-shard (NUM_SERVERS=3) distributed ladybug-graph algorithm demo on Rivet.
# All progress logs go to stderr; engine logs live in ~/.rivetkit/var/logs/rivet-engine/.
set -euo pipefail
cd /home/ubuntu/src/rivet-graph/rivet

db=/tmp/foo.db
export RIVET_ENGINE_BINARY_PATH=$PWD/target/release/rivet-engine
export RUST_LOG="${RUST_LOG:-info}"

# Recreate + seed the shared LadybugDB graph so every run starts from a clean store.
# Remove the WAL/SHM sidecars too: a stale wal from a previous database id makes open fail.
rm -f "$db" "$db.wal" "$db.shm"
./target/release/server seed "$db" 2
echo "hosting worker + coordinator actors (GRAPH_AUTO_RUN defaults to on)..."

# Host the actors; the server auto-triggers runAlgorithm and prints superstep progress.
LADYBUG_DB=$db NUM_SERVERS=3 GRAPH_K=2 GRAPH_ALGO=1 GRAPH_RUN_ID=1 ./target/release/server