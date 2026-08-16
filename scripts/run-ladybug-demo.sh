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
    algo="${2:-kcore}"
    case "$algo" in
      kcore|k-core)
        graph_algo=1
        k="${3:-${RIVET_K:-2}}"
        ;;
      wcc|WCC)
        graph_algo=2
        k="${RIVET_K:-0}"
        ;;
      *)
        echo "unknown algorithm '$algo' (expected kcore or wcc)" >&2
        exit 1
        ;;
    esac

    source "$(dirname "$0")/ladybug-demo-lib.sh"
    export RIVET_ENGINE_BINARY_PATH="$PWD/target/release/rivet-engine"

    # Compile phase: build the engine + example binaries once.
    ladybug_build

    # Run phase: seed a fresh store, then host the actors.  The server
    # auto-triggers runAlgorithm and prints superstep progress; Ctrl-C to stop.
    db="${RIVET_LADYBUG_DB:-$(mktemp -d)/cluster.lbdb}"
    url="${RIVET_LADYBUG_URL:-http://127.0.0.1:8123}"
    ladybug_install_cleanup
    ladybug_start_store "$db" "$url"
    echo "seeding graph through the columnar protocol at $url (k=$k)..." >&2
    ./target/release/server seed "$url" "$k"
    echo "hosting worker + coordinator actors (auto-triggers runAlgorithm; Ctrl-C to stop)..." >&2
    LADYBUG_DB="$url" NUM_SERVERS=3 GRAPH_K="$k" GRAPH_ALGO="$graph_algo" GRAPH_RUN_ID=1 \
      ./target/release/server &
    LADYBUG_HOST_PID=$!
    wait "$LADYBUG_HOST_PID"
    ;;
esac
