#!/usr/bin/env bash
# Run the 3-shard (NUM_SERVERS=3) distributed ladybug-graph algorithm demo on Rivet.
# Uses the prebuilt release binaries; build them once with
# `./scripts/run-ladybug-demo.sh rivet` or `cargo build --release -p rivet-engine -p example-ladybug-graph`.
# All progress logs go to stderr; engine logs live in ~/.rivetkit/var/logs/rivet-engine/.
set -euo pipefail
cd "$(dirname "$0")/.."

db=/tmp/foo.db
url=http://127.0.0.1:8123
export RIVET_ENGINE_BINARY_PATH="$PWD/target/release/rivet-engine"
export RUST_LOG="${RUST_LOG:-info}"

source "$(dirname "$0")/ladybug-demo-lib.sh"

# Run phase: seed a fresh store, then host the worker + coordinator actors.  The server
# auto-triggers runAlgorithm and prints superstep progress; Ctrl-C to stop.
ladybug_install_cleanup
ladybug_start_store "$db" "$url"
./target/release/server seed "$url" 2
echo "hosting worker + coordinator actors (auto-triggers runAlgorithm and exits when done)..."
LADYBUG_DB=$url NUM_SERVERS=3 GRAPH_K=2 GRAPH_ALGO=1 GRAPH_RUN_ID=1 ./target/release/server &
LADYBUG_HOST_PID=$!
wait "$LADYBUG_HOST_PID"
echo "demo finished; results persisted to the graph store at $db (see the superstep logs above; the store is recreated on the next run)" >&2
