# ladybug-graph

A platform for running **distributed message-passing graph algorithms** (k-core, weakly connected
components) on Rivet, **persisting typed application data** into the **LadybugDB** property-graph
store over the **`adbc_core`** (ADBC / Arrow Database Connectivity) interface.

```
                 ┌────────────────────────────────────────────────────┐
                 │               LadybugDB graph store                 │
                 │   Vertex / Edge / Msg / Run node+rel tables         │
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
- **`adbc_core`** provides the inter-instance data plane: workers never talk directly; their only
  channel is the shared graph store, and every read/write through that channel goes through the
  ADBC `Statement` interface (Arrow result sets).

## Why these pieces

- **Strongly typed application loading** (`src/props.rs`, `src/graph.rs`). Hand-rolling
  `Vec<(String, lbug::Value)>` property vectors is error prone (stringly-typed keys, manual
  `Value::Int64(...)` wrapping, no cross-check with the schema). Applications instead declare their
  node tables once as [`props::Table`](src/props.rs) schemas and build every row from a typed Rust
  object through the [`props!`](src/props.rs) macro / [`props::TypedProps`](src/props.rs), which
  reject unknown keys and wrong value types before anything reaches the store.
- **`adbc_core` for inter-instance communication** (`src/adbc.rs`). The four ADBC traits
  (`Driver`, `Database`, `Connection`, `Statement`) are implemented over the LadybugDB graph store.
  A worker "sends a message" via `execute_update` (a `CREATE` into the shared `Msg` table) and
  "receives" via `execute` (a `MATCH` returning Arrow), so Arrow is the encoding that crosses the
  boundary on both sides of every worker.
- **Superstep message passing** (`src/algorithm.rs`). Pregel-style barrier rounds over the shared
  `Msg` table, with a `round` column keeping the barrier clean across shards.
  - `KCore { k }` — peeling: a vertex whose effective degree drops below `k` leaves the core and
    sends *decrement* messages to its neighbors; survivors are the k-core (persisted
    `active = true, core = k`).
  - `Wcc` — components: a vertex adopts the smallest component label it hears and propagates the
    improvement (persisted in `value`).
- **Persistence** — every algorithm ends by persisting its result back into the graph (`Vertex`
  rows), verified by re-opening the on-disk store.

## Run

```sh
# Fully automated: NUM_SERVERS shard workers over a shared on-disk store (no engine needed).
scripts/run-ladybug-demo.sh kcore 2
scripts/run-ladybug-demo.sh wcc

# Tests (release profile keeps the debug dir small).
cargo test -p example-ladybug-graph --release
```

Example k-core output (demo "house + tail" graph, k = 2):

```text
converged after 4 supersteps (8 vertices)
  vertex  0  server 0  degree 2  core 2  [IN]
  vertex  7  server 1  degree 0  core 2  [OUT]   <- pendant tail peeled off by message passing
```

## The embedded-store constraint (important)

LadybugDB is an **embedded, single-instance-per-file** store: exactly one `lbug` instance may hold
a database file open at a time, and a second instance opening the same path corrupts the
write-ahead log. So each process opens the store **once** and shares the handle across all of its
workers (see the process-wide `shared_db()` in `src/actors.rs`); the superstep model serializes
writers through the single store while the ADBC surface fans many connections out on top of it.
True multi-process / multi-node sharing of the *same embedded file* is not supported. For that, run
LadybugDB as a **remote server** and connect each Rivet worker over its URL (`lbug` supports
`Database::new("http://host:port")`, per the ladybug skill) — then the workers can live in separate
processes while the single remote store still mediates their ADBC message passing.

## Modules

| File | Role |
|------|------|
| `src/props.rs` | strongly typed property builders (`props!`, `TypedProps`, `Table`) |
| `src/adbc.rs`  | `adbc_core` `Driver`/`Database`/`Connection`/`Statement` over LadybugDB + Arrow encode/decode |
| `src/graph.rs` | typed application layer: table schemas, `Vertex`/`Msg`/`Run` rows, `GraphDb` facade |
| `src/algorithm.rs` | superstep engine + `KCore` / `Wcc` + coordinator |
| `src/actors.rs` | Rivet `VertexWorker` / `Coordinator` actors (control plane over Rivet) |
| `src/bin/server.rs` | one Rivet host process serving the workers + coordinator |
