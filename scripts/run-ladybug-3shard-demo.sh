#!/usr/bin/env bash
# Run the 3-shard (NUM_SERVERS=3) distributed ladybug-graph algorithm demo on Rivet.
# All progress logs go to stderr; engine logs live in ~/.rivetkit/var/logs/rivet-engine/.
set -euo pipefail
cd /home/ubuntu/src/rivet-graph/rivet

db=/tmp/foo.db
url=http://127.0.0.1:8123
port="${url##*:}"
engine_port=6420
# The engine state dir.  The demo resets it each run (like the ladybug store) because a fresh
# engine inherits actors owned by the previous run's envoy, and `get_or_create` then hangs.
engine_db="${RIVETKIT_STORAGE_PATH:-$HOME}/.rivetkit/var/engine/db"
export RIVET_ENGINE_BINARY_PATH=$PWD/target/release/rivet-engine
export RUST_LOG="${RUST_LOG:-info}"

# ---------------------------------------------------------------------------
# Cleanup helpers
# ---------------------------------------------------------------------------

# PIDs bound to a TCP port (the demo's ladybug server and rivet engine are the
# only processes expected on their ports).
pids_on_port() {
  ss -ltnpH "sport = :$1" 2>/dev/null | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p' | sort -u
}

# Kills PIDs, waits briefly for graceful shutdown, escalates to SIGKILL.
stop_pids() {
  local desc="$1"
  shift
  [ $# -eq 0 ] && return 0
  echo "stopping $desc: $*" >&2
  kill "$@" 2>/dev/null || true
  for _ in $(seq 1 20); do
    local all_gone=1
    for pid in "$@"; do
      if kill -0 "$pid" 2>/dev/null; then
        all_gone=0
        break
      fi
    done
    [ "$all_gone" -eq 1 ] && return 0
    sleep 0.1
  done
  kill -9 "$@" 2>/dev/null || true
}

# Tear down everything this demo started so the next run is clean: the ladybug
# server, the host `server` process, and the rivet engine (rivetkit spawns it
# intentionally orphaned, so it would otherwise linger on the demo endpoints).
# The engine's state is reset too: a fresh engine reusing it inherits actors
# owned by the previous run's envoy, which hangs `get_or_create` for 30s.
cleanup() {
  if [ -n "${LADYBUG_SERVER_PID:-}" ]; then
    stop_pids "ladybug server" "$LADYBUG_SERVER_PID"
    LADYBUG_SERVER_PID=
  fi
  if [ -n "${SERVER_PID:-}" ]; then
    stop_pids "host server" "$SERVER_PID"
    SERVER_PID=
  fi
  local engine_pids
  engine_pids="$(pids_on_port "$engine_port")"
  [ -n "$engine_pids" ] && stop_pids "demo rivet engine on port $engine_port" $engine_pids
  rm -rf "$engine_db"
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# The LadybugDB server is the only process that owns the graph file; workers
# connect over HTTP.  Recreate + seed a clean store: remove the file (the
# server recreates it on open).
rm -f "$db" "$db.wal" "$db.shm"

# A stale ladybug-server from an interrupted earlier run would still hold the
# port and serve its *old* store, so `seed` would hit stale data (for example
# a duplicate primary key in the Run table).  Free the port before starting a
# fresh server so the health check can only answer for the new store.
if pids="$(pids_on_port "$port")" && [ -n "${pids:-}" ]; then
  echo "freeing stale server(s) already bound to port $port: $pids" >&2
  echo "$pids" | xargs -r kill 2>/dev/null || true
  sleep 0.2
fi

./target/release/ladybug-server --db "$db" --listen 127.0.0.1:$port &
LADYBUG_SERVER_PID=$!

# Wait for the server to accept connections before seeding.  If it dies during
# startup (for example the port is still taken), fail loudly instead of seeding
# a stale server.
for _ in $(seq 1 50); do
  if ! kill -0 "$LADYBUG_SERVER_PID" 2>/dev/null; then
    echo "ladybug server exited during startup (port $port busy?); see output above" >&2
    exit 1
  fi
  curl -fsS "$url/health" >/dev/null 2>&1 && break
  sleep 0.1
done

# Seed through the columnar protocol (the seed client is remote, not shared-file).
./target/release/server seed "$url" 2
echo "hosting worker + coordinator actors (GRAPH_AUTO_RUN defaults to on)..."

# Host the actors in the background so the trap can reach it.  The server
# auto-triggers runAlgorithm and prints superstep progress; Ctrl-C to stop.
LADYBUG_DB=$url NUM_SERVERS=3 GRAPH_K=2 GRAPH_ALGO=1 GRAPH_RUN_ID=1 ./target/release/server &
SERVER_PID=$!
wait "$SERVER_PID"
