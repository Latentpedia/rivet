# ladybug-graph

A platform for running **distributed message-passing graph algorithms** (k-core, weakly connected
components) on Rivet, **persisting typed application data** into the **LadybugDB** property-graph
store over the **`adbc_core`** (ADBC / Arrow Database Connectivity) interface.

```
                 ┌────────────────────────────────────────────────────┐
                 │            ladybug-server (single writer)          │
                 │   Owner of the graph file, Arrow IPC over HTTP     │
                 │   Vertex / Edge / Msg / Run node+rel tables        │
                 └──────▲──────────────▲──────────────▲────────────────┘
                        │  ADBC        │  ADBC        │  ADBC
              (Arrow)   │              │              │   (Arrow)
                 ┌───────┴─────┐ ┌──────┴─────┐ ┌──────┴─────┐
                 │ Server 0    │ │ Server 1   │ │ Server 2   │
                 │ VertexWorker│ │ VertexWorker│ │ VertexWorker│
                 └─────────────┘ └────────────┘ └────────────┘
```

- **Rivet** provides the platform: actor lifecycle and the cross-worker control plane (the
  `Coordinator` invoking each `VertexWorker`'s superstep action).
- **`ladybug-server`** owns the graph file. LadybugDB is an embedded, single-writer-per-file
  store, so exactly one process opens it; that process exposes it to the world.
- **`adbc_core`** provides the inter-instance data plane: workers never talk directly; their only
  channel is the shared graph store served by `ladybug-server`. Every read/write through that
  channel goes through the ADBC `Statement` interface (Arrow result sets) over the wire.

## Why these pieces

- **Real remote access over a columnar protocol.** The old design was entirely local: every worker
  opened the same embedded file in-process, which both prevented cross-machine distribution and
  forced a shared-file/global-lock model. Now `ladybug-server` is a standalone process that owns
  the store, and the ADBC driver (`src/adbc.rs`) is a client of it. The wire format is a tiny
  typed-JSON request envelope (`POST /rpc`, query text + named scalar bindings) and an **Arrow IPC
  stream** response (`application/vnd.apache.arrow.stream`) for reads, so result sets stay columnar
  across the network instead of paying row-by-row JSON overhead. Any number of worker/coordinator
  processes on any number of machines can share the one server-owned store.
- **Strongly typed application loading** (`src/props.rs`, `src/graph.rs`). Hand-rolling
  `Vec<(String, lbug::Value)>` property vectors is error prone (stringly-typed keys, manual
  `Value::Int64(...)` wrapping, no cross-check with the schema). Applications instead declare their
  node tables once as [`props::Table`](src/props.rs) schemas and build every row from a typed Rust
  object through the [`props!`](src/props.rs) macro / [`props::TypedProps`](src/props.rs), which
  reject unknown keys and wrong value types before anything reaches the store.
- **Superstep message passing** (`src/algorithm.rs`). Pregel-style barrier rounds over the shared
  `Msg` table, with a `round` column keeping the barrier clean across shards.
  - `KCore { k }` — peeling: a vertex whose effective degree drops below `k` leaves the core and
    sends *decrement* messages to its neighbors; survivors are the k-core (persisted
    `active = true, core = k`).
  - `Wcc` — components: a vertex adopts the smallest component label it hears and propagates the
    improvement (persisted in `value`).
- **Persistence** — every algorithm ends by persisting its result back into the graph (`Vertex`
  rows), verified by re-opening the store.

## How the columnar RPC works

`ladybug-server` (binary `ladybug-server`, module `src/ladybug_server.rs`) serves two endpoints:

- `GET /health` — liveness probe.
- `POST /rpc` — one endpoint for reads and writes. The request is `{ kind, cypher, params }` where
  `params` are named, type-tagged scalars (so the server rebinds `$name` placeholders losslessly on
  a prepared statement). A `kind: "query"` response is an Arrow IPC stream of the result batches; a
  `kind: "update"` returns a small JSON ack. Server-side query errors come back as a JSON
  `{ "error": ... }` body with a 4xx/5xx status.

The ADBC driver (`src/adbc.rs`) implements the four ADBC traits (`Driver`, `Database`,
`Connection`, `Statement`) as a client of this protocol:

- `Statement::execute` POSTs a `query` and streams the `RecordBatch`es straight off the Arrow IPC
  body, so the columnar data never degrades to rows on the wire.
- `Statement::execute_update` POSTs an `update`.
- The client keeps one pooled HTTP connection per database, which the many per-round superstep
  queries reuse.

The server runs reads concurrently (the engine synchronizes connections internally) and serializes
writes behind one process-wide lock, which is exactly the single-writer guarantee the embedded
engine requires. `src/protocol.rs` holds the wire contract shared by both sides.

## Run

The demos are split into a shared **compile phase** and a **run phase** (`scripts/ladybug-demo-lib.sh`
holds both):

- **Compile phase** (`ladybug_build`) — `cargo build --release -p rivet-engine -p example-ladybug-graph`,
  producing the `rivet-engine`, `ladybug-server`, `server`, and standalone `example-ladybug-graph`
  binaries.  `run-ladybug-demo.sh rivet` runs it automatically; `run-ladybug-3shard-demo.sh` skips it
  and uses the prebuilt release binaries.
- **Run phase** — frees the demo port of any stale server, starts a fresh `ladybug-server` on a
  clean store, seeds it, and hosts the worker + coordinator actors.  The host exits automatically
  once the algorithm run completes (Ctrl-C still works to stop early), and on exit it tears down
  the ladybug-server, the host `server`, and the rivet engine, and resets the engine state
  (`~/.rivetkit/var/engine/db`), so the next run starts clean.

Results and progress are visible on stderr: per-vertex `vertex result` lines show what each shard
computed (`server` is the shard, `tag` is `IN`/`OUT` of the k-core).  The persisted graph lives in
the store file passed to the demo (`/tmp/foo.db` for the 3-shard demo; recreated on the next run),
and engine logs live in `~/.rivetkit/var/logs/rivet-engine/`.

```sh
# Standalone demo (no engine): spins up an in-process ladybug-server on an ephemeral port and
# runs the algorithm over the remote columnar protocol.
scripts/run-ladybug-demo.sh kcore 2      # k-core with k=2 (default)
scripts/run-ladybug-demo.sh wcc

# Rivet actor deployment: compile phase + seed a fresh store + host the actors.
scripts/run-ladybug-demo.sh rivet kcore  # k-core with k=2 (default), or: rivet wcc

# The 3-shard rivet demo: same run phase, no build (expects the prebuilt release binaries).
scripts/run-ladybug-3shard-demo.sh

# Tests (release profile keeps the debug dir small).
cargo test -p example-ladybug-graph --release
```

Example k-core output (demo "house + tail" graph, k = 2):

```text
converged after 4 supersteps (8 vertices)
  vertex  0  server 0  degree 2  core 2  [IN]
  vertex  7  server 1  degree 0  core 2  [OUT]   <- pendant tail peeled off by message passing
```

## The single-writer-served-store constraint

LadybugDB is an embedded store: exactly one `lbug` instance may hold a database file open at a
time, and a second instance opening the same path corrupts the write-ahead log. The `ladybug-server`
process is that one instance. Every other component is a remote client that holds no file handle,
so the constraint is contained to a single process while the rest of the platform is free to
distribute. This is why `LADYBUG_DB` is a URL, not a path: actors, the seed command, and the
standalone demo all point at the server and talk to it through `adbc_core`.

## Modules

| File | Role |
|------|------|
| `src/protocol.rs` | the wire contract shared by server and client (`POST /rpc`, typed params, Arrow IPC responses) |
| `src/ladybug_server.rs` | the server process: owns the lbug file, serializes writers, encodes Arrow IPC |
| `src/adbc.rs` | the ADBC client driver over the remote columnar protocol |
| `src/props.rs` | strongly typed property builders (`props!`, `TypedProps`, `Table`) |
| `src/graph.rs` | typed application layer: table schemas, `Vertex`/`Msg`/`Run` rows, `GraphDb` facade |
| `src/algorithm.rs` | superstep engine + `KCore` / `Wcc` + coordinator |
| `src/actors.rs` | Rivet `VertexWorker` / `Coordinator` actors (control plane over Rivet) |
| `src/bin/ladybug-server.rs` | the LadybugDB server binary |
| `src/bin/server.rs` | one Rivet host process serving the workers + coordinator |
