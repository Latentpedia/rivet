#!/usr/bin/env bash
# Shared compile + run helpers for the ladybug-graph demos:
#
#   scripts/run-ladybug-demo.sh         (threads and rivet modes)
#   scripts/run-ladybug-3shard-demo.sh  (rivet only)
#
# Source this file from a script that has already cd'd to the repo root and
# set `set -euo pipefail` (both demo scripts do).

# The compile phase: build the rivet engine and the ladybug-graph example
# binaries (the `ladybug-server`, the `server` actor host, and the standalone
# demo binary) in release mode.  Incremental, so it is cheap once built.
ladybug_build() {
  echo "building engine + example (one-time)..." >&2
  cargo build --release -p rivet-engine -p example-ladybug-graph
}

# The rivet engine rivetkit spawns for the actor host, and its state dir.  The
# demos reset the state each run (like the ladybug store) because a fresh
# engine inherits actors owned by the previous run's envoy, and `get_or_create`
# then hangs for the full action timeout.
RIVET_ENGINE_PORT=6420
LADYBUG_ENGINE_DB="${RIVETKIT_STORAGE_PATH:-$HOME}/.rivetkit/var/engine/db"

# PIDs bound to a TCP port (the demo's ladybug server and rivet engine are the
# only processes expected on their ports).
ladybug_pids_on_port() {
  ss -ltnpH "sport = :$1" 2>/dev/null | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p' | sort -u
}

# Kills PIDs, waits briefly for graceful shutdown, escalates to SIGKILL.
ladybug_stop_pids() {
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

# Tear down everything a run started so the next run is clean: the ladybug
# server, the host `server` process, and the rivet engine (rivetkit spawns it
# intentionally orphaned, so it would otherwise linger on the demo endpoints).
# The engine's state is reset too: a fresh engine reusing it inherits actors
# owned by the previous run's envoy, which hangs `get_or_create`.
#
# Run scripts set these before starting their processes:
#   LADYBUG_SERVER_PID  PID of the ladybug-server (set by ladybug_start_store)
#   LADYBUG_HOST_PID    PID of the rivet `server` actor host
ladybug_cleanup() {
  if [ -n "${LADYBUG_SERVER_PID:-}" ]; then
    ladybug_stop_pids "ladybug server" "$LADYBUG_SERVER_PID"
    LADYBUG_SERVER_PID=
  fi
  if [ -n "${LADYBUG_HOST_PID:-}" ]; then
    ladybug_stop_pids "host server" "$LADYBUG_HOST_PID"
    LADYBUG_HOST_PID=
  fi
  local engine_pids
  engine_pids="$(ladybug_pids_on_port "$RIVET_ENGINE_PORT")"
  [ -n "$engine_pids" ] && ladybug_stop_pids "demo rivet engine on port $RIVET_ENGINE_PORT" $engine_pids
  rm -rf "$LADYBUG_ENGINE_DB"
  wait 2>/dev/null || true
}

# Installs the cleanup trap.  Call once at the start of the run phase.
ladybug_install_cleanup() {
  trap ladybug_cleanup EXIT INT TERM
}

# Frees `port` of any stale process so the run's fresh server can bind it.  A
# stale ladybug-server from an interrupted earlier run would otherwise keep
# serving its *old* store, and `seed` would hit stale data (for example a
# duplicate primary key in the Run table).
ladybug_free_port() {
  local port="$1"
  local pids
  pids="$(ladybug_pids_on_port "$port")"
  if [ -n "${pids:-}" ]; then
    echo "freeing stale server(s) already bound to port $port: $pids" >&2
    echo "$pids" | xargs -r kill 2>/dev/null || true
    sleep 0.2
  fi
}

# Starts a ladybug-server owning `db` on `url` (for example
# http://127.0.0.1:8123) and waits for it to accept connections.  Sets
# LADYBUG_SERVER_PID for cleanup.  Fails loudly if the server dies during
# startup (for example the port is still taken) instead of silently talking to
# a stale server.
ladybug_start_store() {
  local db="$1"
  local url="${2%/}"
  local port="${url##*:}"
  rm -f "$db" "$db.wal" "$db.shm"
  ladybug_free_port "$port"
  ./target/release/ladybug-server --db "$db" --listen "127.0.0.1:$port" &
  LADYBUG_SERVER_PID=$!
  for _ in $(seq 1 50); do
    if ! kill -0 "$LADYBUG_SERVER_PID" 2>/dev/null; then
      echo "ladybug server exited during startup (port $port busy?); see output above" >&2
      exit 1
    fi
    curl -fsS "$url/health" >/dev/null 2>&1 && break
    sleep 0.1
  done
}
