#!/usr/bin/env bash
# Ladybug-graph demo: run a distributed message-passing graph algorithm over a shared LadybugDB
# store, with NUM_SERVERS shard workers exchanging messages only through the ADBC (Arrow) channel.
#
# Usage (the algorithm can be given directly, or after an explicit mode):
#
#   ./scripts/run-ladybug-demo.sh kcore 2     # k-core with k=2 (default algorithm/k)
#   ./scripts/run-ladybug-demo.sh wcc
#   ./scripts/run-ladybug-demo.sh threads wcc # explicit mode + algorithm
#
#   ./scripts/run-ladybug-demo.sh rivet       # Rivet actor deployment (needs the engine)

set -euo pipefail
cd "$(dirname "$0")/.."

# "threads" (default) and "rivet" are modes; anything else is treated as the algorithm
# (kcore|k-core|wcc) in the default "threads" mode.
mode="${1:-threads}"
case "$mode" in
  threads|rivet) ;;
  kcore|k-core|wcc|WCC)
    algo="$1"
    shift 1 || true
    set -- threads "$algo" "$@"
    mode=threads
    ;;
  *)
    echo "unknown mode or algorithm '$mode' (expected: threads, rivet, kcore, or wcc)" >&2
    exit 1
    ;;
esac

case "$mode" in
  threads)
    algo="${2:-kcore}"
    shift 2 || true
    # --bin pins the demo binary (the package also ships the rivet `server` binary).
    cargo run --release -p example-ladybug-graph --bin example-ladybug-graph -- "$algo" "$@"
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
esac
