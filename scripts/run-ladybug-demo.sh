#!/usr/bin/env bash
# Ladybug-graph demo: run a distributed message-passing graph algorithm over a shared LadybugDB
# store, with NUM_SERVERS shard workers exchanging messages only through the ADBC (Arrow) channel.
#
# Two modes:
#
#   1. "threads" (default) — no engine needed. Spawns NUM_SERVERS concurrent worker threads, each
#      opening its own ADBC connection to the shared on-disk store, and runs the algorithm to a
#      fixed point. Persists the result back into the graph.
#
#      ./scripts/run-ladybug-demo.sh kcore 2
#      ./scripts/run-ladybug-demo.sh wcc
#
#   2. "rivet" — the same compute wrapped as Rivet actors (see src/actors.rs, src/bin/server.rs).
#      Requires the engine binary and a Rivet client to trigger the coordinator; see README.

set -euo pipefail
cd "$(dirname "$0")/.."

mode="${1:-threads}"

case "$mode" in
  threads)
    algo="${2:-kcore}"
    shift 2 || true
    cargo run --release -p example-ladybug-graph -- "$algo" "$@"
    ;;
  rivet)
    echo "building engine + example (one-time)..." >&2
    cargo build --release -p rivet-engine -p example-ladybug-graph
    db="${RIVET_LADYBUG_DB:-$(mktemp -d)/cluster.lbdb}"
    k="${RIVET_K:-2}"
    echo "seeding graph at $db (k=$k)..." >&2
    ./target/release/server seed "$db" "$k"
    echo "building done. Host worker+coordinator actors in one Rivet host process (one embedded store) and trigger runAlgorithm:" >&2
    echo "  export RIVET_ENGINE_BINARY_PATH=\$PWD/target/release/rivet-engine" >&2
    echo "  LADYBUG_DB=\$db NUM_SERVERS=3 ./target/release/server" >&2
    ;;
  *)
    echo "unknown mode '$mode' (use 'threads' or 'rivet')" >&2
    exit 1
    ;;
esac
