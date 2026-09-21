//! Client-side partition router mirroring LadybugDB's distributed partition-routing hooks.
//!
//! Both `Vertex` and `Msg` are `PARTITION BY LIST(cluster)` parents: one partition subgraph per
//! distinct cluster id, created on demand at first sight of a new value (`<parent>_p0` is the
//! unkeyed partition made at DDL time and stays empty; `<parent>_p1`, `<parent>_p2`, ... hold
//! the values in first-sight order). `Vertex` partitions hold community rows; `Msg` partitions
//! hold the matching inbox slices, so each community's actor exclusively reads and writes two
//! colocated slices. The engine owns the value-to-partition map (it persists on each parent
//! entry) and the catalog metadata; a distributed wrapper only owns *where* each partition
//! lives. That wrapper contract is LadybugDB PR #829,
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
//! The `lbug` Rust crate binds `setPartitionRoutingHooks` (`lbug::RoutingGuard`), and the
//! server process installs real hooks at startup (see
//! [`crate::ladybug_server::install_local_hooks`]): all partitions stay local there, with
//! lifecycle transitions logged. This router remains as the client-side complement — it
//! discovers the engine's placement for direct-partition reads and mirrors lifecycle —
//! while true remote placement lives in engine hooks (exercised by `tests/routing.rs`,
//! which claims a dedicated table through a real guard). The semantics are identical to
//! what an in-engine wrapper enforces:
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
//! claims nothing and the engine handles all storage. Rivet adds the compute-routing half:
//! the coordinator's directory maps each cluster to its owning actor, which is the same
//! mapping a host's `locate()` claims would carry — LadybugDB routes *rows* to partitions,
//! Rivet routes *invocations* to the partition's owner. Moving these decisions into
//! `setPartitionRoutingHooks` later changes no query shape: cluster reads already address one
//! partition, and writes already carry the key the engine routes on.
//!
//! Ownership discipline (enforced by convention, checked by
//! [`check_store_ownership`](crate::clusters::check_store_ownership)): only the actor owning
//! cluster `C` writes `Vertex` rows with `cluster = C`, `Msg` rows with `cluster = C`, or
//! moves rows out of `C`. Every other actor may read across partitions (neighbor lookups,
//! directory scans) but never write outside its slice.

use std::collections::HashMap;
use std::sync::LazyLock;

use anyhow::Result;

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

/// Empty map shared by the table accessors before their first discovery.
static EMPTY_MAP: LazyLock<HashMap<i64, String>> = LazyLock::new(HashMap::new);

/// Placement + lifecycle decisions for one deployment's partitioned tables.
#[derive(Debug, Default)]
pub struct PartitionRouter {
	/// Discovered `cluster value -> partition table` map per LIST-partitioned parent
	/// (e.g. `{"Vertex": {0: "Vertex_p1"}, "Msg": {0: "Msg_p1"}}`).
	list_maps: HashMap<String, HashMap<i64, String>>,
	/// Lifecycle notifications observed (`created Vertex LIST(cluster)`, ...).
	lifecycle: Vec<String>,
}

impl PartitionRouter {
	pub fn new() -> Self {
		PartitionRouter {
			list_maps: HashMap::new(),
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

	/// Re-discovers one parent's cluster-to-partition map from the catalog. Lists the
	/// `<parent>_p<i>` tables and samples one `cluster` value from each non-empty one; the
	/// unkeyed DDL-time partition (`<parent>_p0`) stays empty and maps nothing. Discoveries
	/// merge into the cached map and are never removed: a partition keeps its key forever,
	/// so a cluster whose rows all migrated away still resolves to its (now empty) table
	/// instead of looking unseeded. Called automatically on cache misses and after writes
	/// that may have created partitions.
	pub fn refresh_list_map(&mut self, db: &mut LadybugDb, parent: &str) -> Result<()> {
		let tables = db
			.query("CALL show_tables() RETURN *", &[])
			.map_err(|e| anyhow::anyhow!("list partition tables failed: {e}"))?;
		let prefix = format!("{parent}_p");
		let mut partitions: Vec<String> = tables
			.iter()
			.filter_map(|row| match row.get(1).cloned().flatten() {
				Some(Value::String(name)) if name.starts_with(&prefix) => Some(name),
				_ => None,
			})
			.collect();
		partitions.sort();
		let map = self.list_maps.entry(parent.to_string()).or_default();
		for table in &partitions {
			let rows = db
				.query(&format!("MATCH (v:{table}) RETURN v.cluster LIMIT 1"), &[])
				.map_err(|e| anyhow::anyhow!("sample {table} failed: {e}"))?;
			if let Some(Value::Int64(cluster)) =
				rows.first().and_then(|r| r.first().cloned().flatten())
			{
				map.insert(cluster, table.clone());
			}
		}
		Ok(())
	}

	/// The partition subgraph holding `cluster` under `parent` (the `bindScan` equivalent:
	/// a cluster-colocated read scans this table directly instead of the parent union).
	/// Reads the cached map and re-discovers it on a miss, so partitions created on demand
	/// by a concurrent writer are picked up. Fails for values the engine has never seen:
	/// writing the first row with a new cluster creates its partition, but rel coverage is
	/// frozen at rel-table creation, so the cluster domain must be pre-seeded (see
	/// [`crate::graph::seed_edges`]).
	pub fn partition_for(
		&mut self,
		db: &mut LadybugDb,
		parent: &str,
		cluster: i64,
	) -> Result<String> {
		if let Some(table) = self
			.list_maps
			.get(parent)
			.and_then(|map| map.get(&cluster))
			.cloned()
		{
			return Ok(table);
		}
		self.refresh_list_map(db, parent)?;
		self.list_maps
			.get(parent)
			.and_then(|map| map.get(&cluster))
			.cloned()
			.ok_or_else(|| {
				anyhow::anyhow!(
					"cluster {cluster} has no {parent} partition yet: seed a row with that cluster before \
					 creating edges against it (rel coverage is frozen at rel-table creation)"
				)
			})
	}

	/// Like [`partition_for`](PartitionRouter::partition_for), but a value the engine has
	/// never seen resolves to `None` instead of failing. For `Msg` inbox reads that means an
	/// empty inbox: a community that has never been sent a message simply has no partition.
	pub fn partition_for_opt(
		&mut self,
		db: &mut LadybugDb,
		parent: &str,
		cluster: i64,
	) -> Result<Option<String>> {
		if let Some(table) = self
			.list_maps
			.get(parent)
			.and_then(|map| map.get(&cluster))
			.cloned()
		{
			return Ok(Some(table));
		}
		self.refresh_list_map(db, parent)?;
		Ok(self
			.list_maps
			.get(parent)
			.and_then(|map| map.get(&cluster))
			.cloned())
	}

	/// The `Vertex` partition subgraph holding `cluster` (e.g. `Vertex_p2`).
	pub fn partition_for_cluster(&mut self, db: &mut LadybugDb, cluster: i64) -> Result<String> {
		self.partition_for(db, "Vertex", cluster)
	}

	/// Re-discovers the engine's `Vertex` cluster-to-partition map (see
	/// [`refresh_list_map`](PartitionRouter::refresh_list_map)). Kept under its historic
	/// name for the edge-rewiring path in
	/// [`move_vertex_to_cluster`](crate::graph::GraphDb::move_vertex_to_cluster).
	pub fn refresh_clusters(&mut self, db: &mut LadybugDb) -> Result<()> {
		self.refresh_list_map(db, "Vertex")
	}

	/// The discovered `Vertex` cluster-to-partition map, for tests and diagnostics.
	pub fn cluster_map(&self) -> &HashMap<i64, String> {
		self.list_maps.get("Vertex").unwrap_or(&EMPTY_MAP)
	}

	/// The discovered `Msg` cluster-to-partition map, for tests and diagnostics.
	pub fn msg_cluster_map(&self) -> &HashMap<i64, String> {
		self.list_maps.get("Msg").unwrap_or(&EMPTY_MAP)
	}
}
