//! A platform for running distributed message-passing graph algorithms on Rivet, persisting typed
//! application data into the LadybugDB graph store over the ADBC (Arrow) interface.
//!
//! # Layout
//!
//! - [`props`] — strongly typed node/relationship property builders (the [`props!`] macro and a
//!   schema-checked [`props::TypedProps`]), so persisting application objects is not error-prone
//!   hand-rolled `Vec<(String, Value)>`.
//! - [`adbc`] — the ADBC bridge over the universalDB `LadybugDatabaseDriver`. It is the
//!   inter-instance data plane: every message passing and result persistence read/write between
//!   Rivet servers flows through [`adbc_core`] and returns Arrow result sets.
//! - [`graph`] — the strongly typed application layer over ADBC: declares node/rel table schemas,
//!   persists typed rows, and reads them back as typed objects.
//! - [`algorithm`] — distributed message-passing graph algorithms (k-core, weak connectivity) run
//!   as supersteps over the shared graph via ADBC, with results persisted back into the store.
//! - [`actors`] — Rivet actors that host the algorithm workers and coordinator across N local
//!   server processes.
//!
//! # Demo
//!
//! `cargo test -p example-ladybug-graph` runs the k-core demo end-to-end against an on-disk
//! LadybugDB store with 3 sharded worker instances and asserts the persisted results. The
//! [`actors`] module and `bin/server.rs` build the same algorithm as a multi-process Rivet
//! deployment (see `scripts/run-ladybug-demo.sh`).

pub mod adbc;
pub mod actors;
pub mod algorithm;
pub mod graph;
pub mod props;
