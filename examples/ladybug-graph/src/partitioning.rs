//! Client-side partition router mirroring LadybugDB's distributed partition-routing hooks.
//!
//! `Vertex` is a `PARTITION BY LIST(cluster)` parent: one partition subgraph per distinct
//! cluster id, created on demand at first sight of a new value (`Vertex_p0` is the unkeyed
//! partition made at DDL time and stays empty; `Vertex_p1`, `Vertex_p2`, ... hold the values
//! in first-sight order). The engine owns the value-to-partition map (it persists on the
//! parent entry) and the catalog metadata; a distributed wrapper only owns *where* each
//! partition lives. That wrapper contract is LadybugDB PR #829,
//! `common/partition_routing_hook.h` (`PartitionRoutingHooks`):
//!
//! | Hook | What it decides | This module's equivalent |
//! |------|---------------|--------------------------|
//! | `locate()` | local vs remote placement per partition | [`PartitionRouter::locate`] |
//! | `onPartitionCreate` / `onPartitionDrop` | lifecycle notifications | [`PartitionRouter::note_created`] / [`note_dropped`](PartitionRouter::note_dropped) |
//! | `bindScan()` | which scan serves a claimed partition | [`PartitionRouter::partition_for_cluster`] (direct `<parent>_p<i>` scan) |
//! | `insertRow()` / `insertChunk()` | where a routed write lands | writes go through the parent so the engine routes them; see below |
//! | `lookupRow()` | MERGE-match materialization | not needed: the algorithm never MERGEs across partitions |
//!
//! The `lbug` Rust crate does not yet bind `setPartitionRoutingHooks`, so the router implements
//! the same decisions at the client layer, over the ADBC bridge. The semantics are identical to
//! what an in-engine wrapper would enforce:
//!
//! - **The engine owns the placement map.** [`PartitionRouter::partition_for_cluster`] never
//!   computes placement locally; it reads the engine's own catalog (`CALL show_tables()` plus
//!   one `LIMIT 1` sample per partition) and caches the cluster-to-table map. Placement is
//!   therefore always consistent with the engine, including partitions created on demand by a
//!   concurrent writer: a cache miss triggers a re-discovery before failing.
//! - **No mixed scans.** The engine rejects a parent scan that mixes local and remote partitions
//!   at bind time, so every cluster-colocated read targets exactly one partition subgraph
//!   (`MATCH (v:Vertex_p<i>) WHERE v.cluster = $c`), never the parent union plus a filter.
//! - **Writes route by key.** Point writes (`CREATE (:Vertex ...)`, `CREATE (:Msg ...)`) go
//!   through the parent and the engine routes each row to its partition — creating the
//!   partition on first sight of a new value — which is the local equivalent of the `insertRow`
//!   hook. Rel writes are the one place the client must name the partition: the engine refuses
//!   parent-bound rel creation on partitioned tables, and rel coverage is frozen when the rel
//!   table is created, so edge creation resolves both endpoints' partitions first and creates
//!   the rel against the concrete `<parent>_p<i>` pair (the client-side form of `insertChunk`
//!   target selection).
//!
//! In a multi-host deployment each host would install real hooks claiming its partitions in
//! `locate()` and serving them in `bindScan()`; here every partition is local, so the router
//! claims nothing and the engine handles all storage. Moving these decisions into
//! `setPartitionRoutingHooks` later changes no query shape: cluster reads already address one
//! partition, and writes already carry the key the engine routes on.
//!
//! `Msg` is partitioned differently — `PARTITION BY HASH(server)` over the fixed compute
//! shards — because messages target shards, not clusters. Its placement is still the engine's
//! `hash(server) % n` function, probed once per shard and cached ([`PartitionRouter::msg_placement`]).

use std::collections::HashMap;

use anyhow::{Result, bail};

use crate::adbc::LadybugDb;
use lbug::Value;

/// Where a partition lives, mirroring what the `locate()` hook reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Location {
	/// Served from the local store (the only case in this deployment).
	Local,
	/// Owned by another host (models a multi-host deployment; unused while local).
	Remote,
}

/// Placement + lifecycle decisions for one deployment's partitioned tables.
#[derive(Debug, Default)]
pub struct PartitionRouter {
	/// Fixed partition count of the HASH-partitioned `Msg` table.
	msg_partitions: i64,
	/// Engine-computed `hash(server) % n` per shard, for `Msg` writes and reads.
	msg_placement_cache: HashMap<i64, usize>,
	/// Discovered `cluster value -> partition table` map for the LIST-partitioned `Vertex`
	/// table (e.g. `{0: "Vertex_p1", 1: "Vertex_p2"}`).
	cluster_map: HashMap<i64, String>,
	/// Lifecycle notifications observed (`created Vertex LIST(cluster)`, ...).
	lifecycle: Vec<String>,
}

impl PartitionRouter {
	pub fn new(msg_partitions: i64) -> Self {
		PartitionRouter {
			msg_partitions,
			msg_placement_cache: HashMap::new(),
			cluster_map: HashMap::new(),
			lifecycle: Vec::new(),
		}
	}

	/// Mirrors `locate()`: reports whether a partition is owned locally. This deployment keeps
	/// every partition local, so the answer is always [`Location::Local`]; a multi-host
	/// deployment would claim partitions per host here and serve the rest remotely.
	pub fn locate(&self, _table: &str) -> Location {
		Location::Local
	}

	/// Mirrors `onPartitionCreate`: records that a partitioned parent was (re)provisioned.
	/// Called by schema setup right after the parent DDL runs.
	pub fn note_created(&mut self, parent: &str, method: &str) {
		self.lifecycle.push(format!("created {parent} {method}"));
	}

	/// Mirrors `onPartitionDrop`: records that a partitioned parent was dropped.
	pub fn note_dropped(&mut self, parent: &str, method: &str) {
		self.lifecycle.push(format!("dropped {parent} {method}"));
	}

	/// Observed lifecycle notifications, in order (for tests and diagnostics).
	pub fn lifecycle(&self) -> &[String] {
		&self.lifecycle
	}

	/// The subgraph table name for partition `index` of a HASH-partitioned `parent`
	/// (`Msg_p<i>`). LIST-partitioned tables resolve names through the discovered
	/// [`PartitionRouter::partition_for_cluster`] map instead, since their indexes carry no
	/// key meaning.
	pub fn table_for(&self, parent: &str, index: usize) -> String {
		format!("{parent}_p{index}")
	}

	/// Which HASH partition the engine assigns to `Msg` rows with shard key `server`. Probed
	/// from the engine (`hash($server) % n`) once per distinct shard and cached; the wrapper
	/// never decides *which* partition a row belongs to, only *where* it lives.
	pub fn msg_placement(&mut self, db: &mut LadybugDb, server: i64) -> Result<usize> {
		if let Some(cached) = self.msg_placement_cache.get(&server) {
			return Ok(*cached);
		}
		let rows = db
			.query(
				"RETURN hash($k) % $n",
				&[
					("k", Value::Int64(server)),
					("n", Value::Int64(self.msg_partitions)),
				],
			)
			.map_err(|e| anyhow::anyhow!("engine placement probe failed: {e}"))?;
		let index = match rows.first().and_then(|r| r.first().cloned().flatten()) {
			Some(Value::Int64(v)) => v,
			Some(Value::Int128(v)) => i64::try_from(v).unwrap_or(-1),
			other => bail!("unexpected placement probe result: {other:?}"),
		};
		if index < 0 || index >= self.msg_partitions {
			bail!("engine placed shard {server} in out-of-range partition {index}");
		}
		let index = index as usize;
		self.msg_placement_cache.insert(server, index);
		Ok(index)
	}

	/// Re-discovers the engine's cluster-to-partition map from the catalog. Lists the
	/// `Vertex_p<i>` tables and samples one `cluster` value from each non-empty one; the
	/// unkeyed DDL-time partition (`Vertex_p0`) stays empty and maps nothing. Discoveries
	/// merge into the cached map and are never removed: a partition keeps its key forever,
	/// so a cluster whose rows all migrated away still resolves to its (now empty) table
	/// instead of looking unseeded. Called automatically on cache misses and after writes
	/// that may have created partitions.
	pub fn refresh_clusters(&mut self, db: &mut LadybugDb) -> Result<()> {
		let tables = db
			.query("CALL show_tables() RETURN *", &[])
			.map_err(|e| anyhow::anyhow!("list partition tables failed: {e}"))?;
		let mut partitions: Vec<String> = tables
			.iter()
			.filter_map(|row| match row.get(1).cloned().flatten() {
				Some(Value::String(name)) if name.starts_with("Vertex_p") => Some(name),
				_ => None,
			})
			.collect();
		partitions.sort();
		for table in &partitions {
			let rows = db
				.query(&format!("MATCH (v:{table}) RETURN v.cluster LIMIT 1"), &[])
				.map_err(|e| anyhow::anyhow!("sample {table} failed: {e}"))?;
			if let Some(Value::Int64(cluster)) =
				rows.first().and_then(|r| r.first().cloned().flatten())
			{
				self.cluster_map.insert(cluster, table.clone());
			}
		}
		Ok(())
	}

	/// The partition subgraph holding `cluster` (the `bindScan` equivalent: a cluster-colocated
	/// read scans this table directly instead of the parent union). Reads the cached map and
	/// re-discovers it on a miss, so partitions created on demand by a concurrent writer are
	/// picked up. Fails for values the engine has never seen: writing the first row with a new
	/// cluster creates its partition, but rel coverage is frozen at rel-table creation, so the
	/// cluster domain must be pre-seeded (see [`crate::graph::seed_edges`]).
	pub fn partition_for_cluster(&mut self, db: &mut LadybugDb, cluster: i64) -> Result<String> {
		if let Some(table) = self.cluster_map.get(&cluster).cloned() {
			return Ok(table);
		}
		self.refresh_clusters(db)?;
		self.cluster_map.get(&cluster).cloned().ok_or_else(|| {
			anyhow::anyhow!(
				"cluster {cluster} has no partition yet: seed a row with that cluster before \
				 creating edges against it (rel coverage is frozen at rel-table creation)"
			)
		})
	}

	/// The discovered cluster-to-partition map, for tests and diagnostics.
	pub fn cluster_map(&self) -> &HashMap<i64, String> {
		&self.cluster_map
	}
}
