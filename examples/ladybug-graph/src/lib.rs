//! A platform for running distributed message-passing graph algorithms on Rivet, persisting typed
//! application data into the LadybugDB graph store over the ADBC (Arrow) interface.
//!
//! # Layout
//!
//! - [`props`] — strongly typed node/relationship property builders (the [`props!`] macro and a
//!   schema-checked [`props::TypedProps`]), so persisting application objects is not error-prone
//!   hand-rolled `Vec<(String, Value)>`.
//! - [`adbc`] — the ADBC driver over the remote LadybugDB server (a client of [`ladybug_server`]).
//!   It is the inter-instance data plane: every message passing and result persistence read/write
//!   between Rivet servers flows through [`adbc_core`] and returns Arrow result sets.
//! - [`ladbug_server`](ladybug_server) — the server process that owns the graph file and serves the
//!   columnar ADBC protocol (Arrow IPC over HTTP) to remote clients.
//! - [`protocol`] — the wire contract shared by the server and the ADBC client.
//! - [`graph`] — the strongly typed application layer over ADBC: declares node/rel table schemas,
//!   persists typed rows, and reads them back as typed objects. `Vertex` is LIST-partitioned
//!   by its live computed `cluster` column (one partition per community; moves go through
//!   delete + insert); `Msg` is LIST-partitioned by target `cluster` (one inbox slice per
//!   community, owned by that community's actor).
//! - [`partitioning`] — the client-side partition router mirroring the distributed
//!   partition-routing hooks: catalog-discovered cluster placement per parent table,
//!   per-partition cluster scans, lifecycle.
//! - [`algorithm`] — distributed message-passing graph algorithms (k-core, weak connectivity)
//!   run as supersteps over the partitioned store via ADBC. The coordinator drives the cluster
//!   barrier; the per-cluster compute lives in [`clusters`].
//! - [`clusters`] — the cluster-owned superstep behind model `B`: one superstep function per
//!   community slice, run entirely against the store (pruned partition reads, owner-only
//!   writes), plus the placement policy Rivet uses to route invocations and balance load.
//!   The Rivet [`actors`] invoke this protocol over action calls; tests and the standalone
//!   demo run it inline via [`algorithm::Coordinator`](crate::algorithm::Coordinator).
//! - [`actors`] — Rivet actors that own one cluster slice each: workers run their community's
//!   superstep against the store, and the coordinator routes invocations by cluster, balances
//!   by cost, and stamps run bookkeeping. No graph data lives in Rivet storage.
//!
//! # Demo
//!
//! `cargo test -p example-ladybug-graph` runs the k-core demo end-to-end against an on-disk
//! LadybugDB store with one worker owner per live community and asserts the persisted results.
//! The [`actors`] module and `bin/server.rs` run the same per-cluster compute as a multi-process
//! Rivet deployment (see `scripts/run-ladybug-demo.sh`), with Rivet routing invocations to the
//! owning actor and balancing placement while all state stays in the partitioned tables.

pub mod actors;
pub mod adbc;
pub mod algorithm;
pub mod clusters;
pub mod graph;
pub mod ladybug_server;
pub mod partitioning;
pub mod props;
mod protocol;
