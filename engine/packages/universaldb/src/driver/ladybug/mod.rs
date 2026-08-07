//! Ladybug graph database driver.
//!
//! This driver is the embedded property-graph (Cypher) backend for UniversalDB, parallel to the
//! [`rocksdb`](super::rocksdb) and [`postgres`](super::postgres) drivers. Unlike those two, which
//! model data as opaque byte key/value pairs, the ladybug driver stores a property graph (nodes and
//! relationships with typed properties) and queries it with Cypher.
//!
//! # Relationship to the key/value `DatabaseDriver` trait
//!
//! A graph store is not a key/value store. The [`DatabaseDriver`]/[`TransactionDriver`]
//! traits are byte-oriented, so this driver implements them for lifecycle parity (so a
//! [`DatabaseDriverHandle`] can point at either a rocksdb or a ladybug store) but every
//! key/value operation on a [`LadybugTransactionDriver`] fails by default with an explicit,
//! actionable error. Graph transactions are the supported surface and are obtained from
//! [`LadybugDatabaseDriver::graph_txn`] / [`LadybugDatabaseDriver::run_graph`] instead.
//!
//! # How actors interact with the graph
//!
//! The recommended way for an actor to touch the graph is a strongly typed application-object
//! layer that compiles down to *parameterized* Cypher via prepared statements, not raw Cypher
//! string interpolation. Parameterization keeps untrusted actor input out of the query AST
//! (Cypher-injection safe), lets the engine plan a query once and reuse it, and gives the driver a
//! typed handle to the result instead of a stringly-typed contract. Raw ad-hoc traversal strings
//! remain available through [`LadybugTransaction::query`] for exploratory queries.
//!
//! See the module docs in [`LadybugTransaction`] and `docs-internal/engine/ladybug-graph.md` for
//! the full proposal.

mod database;
mod transaction;

pub(crate) use database::SharedInternal;

pub use database::{LadybugConfig, LadybugDatabaseDriver};
pub use transaction::{
	LadybugNodeSpec, LadybugRelSpec, LadybugRow, LadybugTransaction, LadybugTransactionDriver,
};
