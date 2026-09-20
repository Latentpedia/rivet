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
    improvement. The label is the live `cluster` partition key, so every improvement
    physically migrates the row with delete + insert (`GraphDb::move_vertex_to_cluster`);
    the label is also mirrored in `value`.
- **Persistence** — every algorithm ends by persisting its result back into the graph (`Vertex`
  rows), verified by re-opening the store.
- **Partitioned storage** (`src/partitioning.rs`, `src/graph.rs`). `Vertex` is
  `PARTITION BY LIST(cluster)`: one partition subgraph per live community, created on demand
  at first sight of a new cluster value. Each vertex starts as its own community
  (`cluster = id`); WCC merges them until connected components share one partition. `Msg`
  stays `PARTITION BY HASH(server)` — messages route to fixed compute shards, not communities.
  Point writes go through the parent and the engine routes them; cluster-colocated reads
  address their partition subgraph directly (pruned, then filtered).
- **Distribution-hook routing** (`src/ladybug_server.rs`, `tests/routing.rs`). The server
  process installs the real engine hooks (`lbug::RoutingGuard`, over
  `PartitionRoutingHooks` from PR `LadybugDB/ladybug#829`) at startup: every partition
  stays local, with lifecycle transitions logged. `tests/routing.rs` proves the full
  remote loop through the engine — claiming a dedicated table's partitions, serving
  point and `COPY` writes from the bundled row store, and reading them back via parent
  scans. `PartitionRouter` (`src/partitioning.rs`) remains as the client-side complement
  for placement discovery and direct-partition reads of local tables.

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

## Partitioning notes

Four engine boundaries shape how the example uses partitioned tables:

- **LIST partitions are born on demand.** `PARTITION BY LIST(cluster)` takes no partition
  count: the DDL-time `Vertex_p0` stays empty and each new cluster value mints its own
  subgraph inside the writing transaction. The router discovers the engine's value map from
  the catalog (`CALL show_tables()` + one `LIMIT 1` sample per partition) and merges
  discoveries forever — a partition keeps its key even after its last row migrates away.
  Covered by `list_partitions_map_each_initial_cluster`.
- **The partition key moves by delete + insert, never in place.** `SET cluster` is refused
  with a delete-and-reinsert hint, so `GraphDb::move_vertex_to_cluster` reads the row and its
  adjacency, `DETACH DELETE`s it, re-creates it with the new cluster, and rewires its
  incident edges onto the concrete partition pairs. `value`, `degree`, `core`, and `active`
  ride along verbatim. Covered by `set_partition_key_is_refused` and
  `move_vertex_migrates_row_and_rewires_edges`.
- **Rel coverage freezes at rel-table creation.** A partition born after `Edge` is declared
  has no rel pairs, so seeding writes every vertex (one cluster each) *before* declaring the
  rel table, and WCC only ever moves vertices onto already-seeded values — the domain stays
  complete. Rel *writes* name concrete `Vertex_p<i>` pairs; rel *reads* cross partitions
  through the parent union. Covered by `edges_span_partitions`.
- **Compute sharding is orthogonal to storage partitioning.** Workers still own fixed
  `server` shards (read through the parent union), while rows physically cluster by live
  community. A connected WCC run ends with all rows in one partition. Covered by
  `wcc_collapses_partitions`.

Primary-key uniqueness is enforced per partition, and `Run` stays a plain table (one row per
run, not per shard). The example depends on `lbug` by path (`../../../../ladybug-rust`)
so it builds against the routing-hooks bindings; switch back to a version requirement
once a release containing them is cut. Building also needs engine headers carrying the
hooks — provided here by `LBUG_LIBRARY_DIR`/`LBUG_INCLUDE_DIR` pointing at a ladybug
build tree newer than PR `LadybugDB/ladybug#1005` (plus `LBUG_SHARED=1` and the lib dir
on the loader path at test time).

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
| `src/graph.rs` | typed application layer: partitioned table schemas, `Vertex`/`Msg`/`Run` rows, `GraphDb` facade |
| `src/partitioning.rs` | partition router mirroring the distributed routing hooks (cluster placement, partition scans, lifecycle) |
| `src/algorithm.rs` | superstep engine + `KCore` / `Wcc` + coordinator |
| `src/actors.rs` | Rivet `VertexWorker` / `Coordinator` actors (control plane over Rivet) |
| `src/bin/ladybug-server.rs` | the LadybugDB server binary |
| `src/bin/server.rs` | one Rivet host process serving the workers + coordinator |
