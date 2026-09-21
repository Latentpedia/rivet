//! The strongly typed application layer over the ADBC graph bridge.
//!
//! This is the "platform loads arbitrary application code with strong types" half of the design.
//! An application is declared once as a set of typed [`props::Table`] schemas, and every row that
//! flows between Rivet servers is produced from a typed Rust object (e.g. [`Vertex`]) through the
//! [`props!`](crate::props) / [`props::TypedProps`] builders and validated against those schemas
//! before it reaches LadybugDB. Reads come back through the ADBC/Arrow boundary as typed rows.
//!
//! [`GraphDb`] is a thin facade over an ADBC database handle. It does not know anything about
//! graph algorithms; it only knows the table schemas and gives the [`crate::algorithm`] layer
//! typed operations over them, all backed by [`adbc_core`]. The handle is a **remote client**:
//! every operation is a parameterized Cypher statement sent to the server-owned store over the
//! columnar protocol in [`crate::ladybug_server`], so a [`GraphDb`] can live in a different
//! machine than the store it reads and writes.

use anyhow::{Result, bail};
use lbug::Value;
use serde::{Deserialize, Serialize};

use adbc_core::sync::Driver;

use crate::{
	adbc::{LadybugDb, LadybugDriver},
	partitioning::{Location, PartitionRouter},
	props::{ColType, Properties, Table, TypedProps},
};

/// The number of shards ("servers") the vertex set is partitioned across.
pub const NUM_SERVERS: i64 = 3;

/// Declared table schemas. These are the strong types of the platform: an algorithm declares its
/// node/rel tables here and then only ever creates/reads rows whose shape matches.
pub fn vertex_table() -> Table {
	Table::new(
		"Vertex",
		vec![
			("id", ColType::Int64),
			("server", ColType::Int64),
			("cluster", ColType::Int64),
			("value", ColType::Int64),
			("degree", ColType::Int64),
			("core", ColType::Int64),
			("active", ColType::Bool),
		],
	)
}

/// A typed vertex row. `to_props` builds the graph row through the schema-checked builder so a
/// wrong key or value type is caught before it reaches the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vertex {
	pub id: i64,
	pub server: i64,
	/// Live computed community id. This is the LIST partition key: the row physically lives
	/// in the partition subgraph holding this value.
	pub cluster: i64,
	pub value: i64,
	pub degree: i64,
	pub core: i64,
	pub active: bool,
}

impl Vertex {
	pub fn to_props(&self) -> Result<Properties> {
		Ok(crate::props! {
			"id" => self.id,
			"server" => self.server,
			"cluster" => self.cluster,
			"value" => self.value,
			"degree" => self.degree,
			"core" => self.core,
			"active" => self.active,
		})
	}

	/// Parses a row of `[id, server, cluster, value, degree, core, active]` (the projection
	/// order used by [`GraphDb::read_vertices`] and friends).
	pub fn parse(cells: &[Option<Value>]) -> Result<Vertex> {
		let mut it = cells.iter();
		fn i64_cell(it: &mut std::slice::Iter<Option<Value>>) -> Result<i64> {
			match it.next().and_then(|c| c.clone()) {
				Some(Value::Int64(v)) => Ok(v),
				other => bail!("expected Int64 vertex cell, got {other:?}"),
			}
		}
		let id = i64_cell(&mut it)?;
		let server = i64_cell(&mut it)?;
		let cluster = i64_cell(&mut it)?;
		let value = i64_cell(&mut it)?;
		let degree = i64_cell(&mut it)?;
		let core = i64_cell(&mut it)?;
		let active = match it.next().and_then(|c| c.clone()) {
			Some(Value::Bool(b)) => b,
			other => bail!("expected Bool vertex cell, got {other:?}"),
		};
		Ok(Vertex {
			id,
			server,
			cluster,
			value,
			degree,
			core,
			active,
		})
	}
}

/// A directed segment of an undirected edge, persisted as an `Edge` relationship.
#[derive(Debug, Clone)]
pub struct Edge {
	pub from: i64,
	pub to: i64,
}

/// An ADBC-backed graph database facade. Owns the ADBC database handle; every operation is a
/// parameterized Cypher statement executed through [`adbc_core`].
///
/// `Vertex` is LIST-partitioned by its live computed `cluster` column (one partition subgraph
/// per community) while `Msg` is LIST-partitioned by target `cluster` (one inbox slice per
/// community, owned by that community's actor), and [`PartitionRouter`] tracks the wrapper side
/// of that contract: which partition subgraph serves each cluster value, plus the lifecycle of
/// the partitioned tables.
pub struct GraphDb {
	db: LadybugDb,
	router: PartitionRouter,
}

impl GraphDb {
	/// Opens a facade addressed to the LadybugDB server at `url` (for example
	/// `http://127.0.0.1:8123`). The store itself is owned by that server process; every read and
	/// write flows to it over the columnar ADBC protocol.
	pub fn open(url: impl Into<String>) -> Result<Self> {
		let mut driver = LadybugDriver::new(url.into());
		let db = driver
			.new_database()
			.map_err(|e| anyhow::anyhow!("adbc open failed: {e}"))?;
		Ok(GraphDb {
			db,
			router: PartitionRouter::new(),
		})
	}

	/// The partition router modelling the distributed wrapper contract (placement, lifecycle).
	pub fn router(&mut self) -> &mut PartitionRouter {
		&mut self.router
	}

	/// Re-discovers both LIST partition maps (`Vertex`, `Msg`) from the catalog. Called
	/// automatically on cache misses; call explicitly when a fresh handle must observe
	/// partitions it never wrote through (tests, diagnostics).
	pub fn refresh_partitions(&mut self) -> Result<()> {
		self.router.refresh_list_map(&mut self.db, "Vertex")?;
		self.router.refresh_list_map(&mut self.db, "Msg")?;
		Ok(())
	}

	/// The partition subgraph holding `cluster` (e.g. `Vertex_p2`). Cluster-colocated reads
	/// address this table directly instead of scanning the parent union, which is the
	/// client-side form of the `bindScan` hook.
	pub fn vertex_partition_for_cluster(&mut self, cluster: i64) -> Result<String> {
		let table = self.router.partition_for_cluster(&mut self.db, cluster)?;
		assert_eq!(self.router.locate(&table), Location::Local);
		Ok(table)
	}

	/// The `Msg` partition subgraph holding `cluster`'s inbox slice, if the engine has created
	/// it yet. A community that has never been sent a message has no partition; that reads as
	/// an empty inbox, not an error.
	pub fn msg_table_for_cluster(&mut self, cluster: i64) -> Result<Option<String>> {
		let table = self.router.partition_for_opt(&mut self.db, "Msg", cluster)?;
		if let Some(ref table) = table {
			assert_eq!(self.router.locate(table), Location::Local);
		}
		Ok(table)
	}

	// -- low-level ADBC passthrough -------------------------------------------

	pub fn query(&mut self, cypher: &str) -> Result<Vec<Vec<Option<Value>>>> {
		self.db
			.query(cypher, &[])
			.map_err(|e| anyhow::anyhow!("adbc query failed: {e}"))
	}

	pub fn query_params(
		&mut self,
		cypher: &str,
		params: &[(&str, Value)],
	) -> Result<Vec<Vec<Option<Value>>>> {
		self.db
			.query(cypher, params)
			.map_err(|e| anyhow::anyhow!("adbc query failed: {e}"))
	}

	pub fn update(&mut self, cypher: &str) -> Result<()> {
		self.db
			.update(cypher)
			.map_err(|e| anyhow::anyhow!("adbc update failed: {e}"))?;
		Ok(())
	}

	pub fn update_params(&mut self, cypher: &str, params: &[(&str, Value)]) -> Result<()> {
		self.db
			.update_params(cypher, params)
			.map_err(|e| anyhow::anyhow!("adbc update failed: {e}"))?;
		Ok(())
	}

	pub fn scalar_i64(&mut self, cypher: &str) -> Result<Option<i64>> {
		self.db
			.scalar_i64(cypher)
			.map_err(|e| anyhow::anyhow!("adbc scalar failed: {e}"))
	}

	// -- schema + seeding (all through typed properties) ----------------------

	/// Creates the node/message/run table schemas (the `Edge` rel table is created by
	/// [`seed_edges`], after the first rows exist — see below).
	///
	/// `Vertex` is LIST-partitioned by its live computed `cluster` column: each community
	/// physically lives in its own partition subgraph, created on demand at first sight of a
	/// new value. `Msg` is LIST-partitioned by target `cluster`: each community's inbox slice
	/// lives in its own partition subgraph, owned exclusively by that community's actor.
	/// `Run` stays a plain table: one row per run.
	///
	/// The rel table must come after the first vertex writes: rel coverage over a
	/// LIST-partitioned parent is frozen when the rel table is created, so a partition born
	/// later would have no rel pairs. Seeding creates every initial cluster up front (each
	/// vertex starts as its own community, `cluster = id`), and algorithms only ever move
	/// vertices onto values that already exist, so the domain is complete before `Edge` is
	/// declared.
	pub fn create_schema(&mut self) -> Result<()> {
		self.update(
			"CREATE NODE TABLE IF NOT EXISTS Vertex(id INT64, server INT64, cluster INT64, \
			 value INT64, degree INT64, core INT64, active BOOLEAN, PRIMARY KEY(id)) \
			 PARTITION BY LIST(cluster)",
		)?;
		self.router.note_created("Vertex", "LIST(cluster)");
		self.update(
			"CREATE NODE TABLE IF NOT EXISTS Msg(msg_id SERIAL, to_id INT64, cluster INT64, \
			 kind INT64, payload INT64, round INT64, PRIMARY KEY(msg_id)) \
			 PARTITION BY LIST(cluster)",
		)?;
		self.router.note_created("Msg", "LIST(cluster)");
		self.update(
			"CREATE NODE TABLE IF NOT EXISTS Run(run_id INT64, k INT64, rounds INT64, \
			 done BOOLEAN, PRIMARY KEY(run_id))",
		)?;
		Ok(())
	}

	/// Inserts a vertex row through the typed schema-checked builder. The write goes through
	/// the parent, so the engine routes it to the `cluster` partition, creating that
	/// partition on first sight of a new value.
	pub fn create_vertex(&mut self, v: &Vertex) -> Result<()> {
		let table = vertex_table();
		let typed = TypedProps::new(table);
		let kv = typed
			.try_set("id", v.id)?
			.try_set("server", v.server)?
			.try_set("cluster", v.cluster)?
			.try_set("value", v.value)?
			.try_set("degree", v.degree)?
			.try_set("core", v.core)?
			.try_set("active", v.active)?
			.build()?;
		let mut parts = String::new();
		for (k, val) in &kv {
			if !parts.is_empty() {
				parts.push_str(", ");
			}
			parts.push_str(&format!("{k}: {}", literal(val)));
		}
		self.update(&format!("CREATE (:Vertex {{{parts}}})"))
	}

	/// Reads the vertices owned by `server` (that server's compute shard), in `id` order.
	///
	/// Compute sharding (`server`) is orthogonal to storage partitioning (`cluster`): a
	/// shard's vertices scatter across community partitions as labels evolve, so this reads
	/// the parent union filtered by shard. Cluster-colocated reads use [`GraphDb::read_cluster`].
	pub fn read_vertices(&mut self, server: i64) -> Result<Vec<Vertex>> {
		let rows = self.query_params(
			"MATCH (v:Vertex) WHERE v.server = $s \
			 RETURN v.id, v.server, v.cluster, v.value, v.degree, v.core, v.active ORDER BY v.id",
			&[("s", Value::Int64(server))],
		)?;
		rows.iter().map(|r| Vertex::parse(r)).collect()
	}

	/// Reads one vertex by id, or `None` when absent.
	pub fn read_vertex(&mut self, id: i64) -> Result<Option<Vertex>> {
		let rows = self.query_params(
			"MATCH (v:Vertex {id: $id}) \
			 RETURN v.id, v.server, v.cluster, v.value, v.degree, v.core, v.active",
			&[("id", Value::Int64(id))],
		)?;
		rows.first().map(|r| Vertex::parse(r)).transpose()
	}

	/// Reads every vertex currently in `cluster`, in `id` order, straight from that community's
	/// partition subgraph (pruned, then filtered) — the client-side form of the `bindScan` hook.
	pub fn read_cluster(&mut self, cluster: i64) -> Result<Vec<Vertex>> {
		let table = self.vertex_partition_for_cluster(cluster)?;
		let rows = self.query_params(
			&format!(
				"MATCH (v:{table}) WHERE v.cluster = $c \
			 RETURN v.id, v.server, v.cluster, v.value, v.degree, v.core, v.active ORDER BY v.id"
			),
			&[("c", Value::Int64(cluster))],
		)?;
		rows.iter().map(|r| Vertex::parse(r)).collect()
	}

	/// Moves a vertex to another cluster with delete + insert: the engine refuses in-place
	/// updates of the partition column, so the row is `DETACH DELETE`d and re-created with the
	/// new `cluster`, and its incident edges are rewired onto the concrete partition pairs.
	/// Every other field is preserved verbatim from `v` (pass the row with any computed fields
	/// such as `value` already updated). Returns the re-created row.
	pub fn move_vertex_to_cluster(&mut self, v: &Vertex, new_cluster: i64) -> Result<Vertex> {
		if v.cluster == new_cluster {
			return Ok(v.clone());
		}
		// Adjacency first: the delete below removes every edge incident to this vertex.
		let adjacent = self.neighbors(v.id)?;
		// Fresh clusters for the rewired endpoints (neighbors may have moved already).
		let mut neighbor_cluster = std::collections::HashMap::new();
		for n in &adjacent {
			let row = self
				.read_vertex(*n)?
				.ok_or_else(|| anyhow::anyhow!("edge neighbor {n} has no vertex row"))?;
			neighbor_cluster.insert(*n, row.cluster);
		}
		self.update_params(
			"MATCH (x:Vertex {id: $id}) DETACH DELETE x",
			&[("id", Value::Int64(v.id))],
		)?;
		let moved = Vertex {
			cluster: new_cluster,
			..v.clone()
		};
		self.create_vertex(&moved)?;
		// The insert may have created a brand-new partition; pick up the engine's map.
		self.router.refresh_clusters(&mut self.db)?;
		for n in &adjacent {
			self.create_edge_directed(v.id, new_cluster, *n, neighbor_cluster[n])?;
			self.create_edge_directed(*n, neighbor_cluster[n], v.id, new_cluster)?;
		}
		Ok(moved)
	}

	/// Creates one directed edge against the concrete partition pair. The engine refuses
	/// parent-bound rel writes on partitioned tables, so both endpoints resolve to their
	/// `<parent>_p<i>` tables first.
	pub fn create_edge_directed(
		&mut self,
		from_id: i64,
		from_cluster: i64,
		to_id: i64,
		to_cluster: i64,
	) -> Result<()> {
		let from_table = self.vertex_partition_for_cluster(from_cluster)?;
		let to_table = self.vertex_partition_for_cluster(to_cluster)?;
		self.update_params(
			&format!(
				"MATCH (x:{from_table} {{id: $a}}), (y:{to_table} {{id: $b}}) \
				 CREATE (x)-[:Edge]->(y)"
			),
			&[("a", Value::Int64(from_id)), ("b", Value::Int64(to_id))],
		)
	}

	/// Returns the outgoing neighbors of `v` (undirected edges are stored both directions).
	pub fn neighbors(&mut self, id: i64) -> Result<Vec<i64>> {
		let rows = self.query_params(
			"MATCH (a:Vertex {id: $id}) -[e:Edge]-> (b:Vertex) RETURN b.id",
			&[("id", Value::Int64(id))],
		)?;
		Ok(rows
			.iter()
			.filter_map(|r| match r.first().cloned().flatten() {
				Some(Value::Int64(id)) => Some(id),
				_ => None,
			})
			.collect())
	}

	pub fn neighbor_count(&mut self, id: i64) -> Result<i64> {
		Ok(self
			.scalar_i64(&format!(
				"MATCH (a:Vertex {{id: {id}}}) -[e:Edge]-> (:Vertex) RETURN count(e)"
			))?
			.unwrap_or(0))
	}

	pub fn count_vertex(&mut self) -> Result<i64> {
		Ok(self
			.scalar_i64("MATCH (v:Vertex) RETURN count(v)")?
			.unwrap_or(0))
	}

	// -- message passing (partitioned inbox slices) -----------------------------

	/// Writes a message row addressed to `to_id`, owned by `cluster` (the target's home
	/// community at send time). The write goes through the parent so the engine routes it
	/// into that community's `Msg` partition, creating the partition on first sight of the
	/// value. The `round` column keeps superstep barriers clean: a message written in round
	/// `r` is only ever consumed by the owning actor's round-`r+1` pass.
	pub fn write_cluster_msg(
		&mut self,
		to_id: i64,
		cluster: i64,
		kind: i64,
		payload: i64,
		round: i64,
	) -> Result<()> {
		self.update(&format!(
			"CREATE (:Msg {{to_id: {to_id}, cluster: {cluster}, kind: {kind}, payload: {payload}, round: {round}}})"
		))
	}

	/// Reads one community's inbox slice for one round, returning `(to_id, kind, payload)`
	/// straight from its `Msg` partition (pruned, then filtered) — the store-side half of
	/// routing a message to its owning actor. A community with no `Msg` partition yet reads
	/// as an empty inbox.
	pub fn read_cluster_msgs(&mut self, cluster: i64, round: i64) -> Result<Vec<(i64, i64, i64)>> {
		let Some(table) = self.msg_table_for_cluster(cluster)? else {
			return Ok(Vec::new());
		};
		let rows = self.query_params(
			&format!(
				"MATCH (m:{table}) WHERE m.cluster = $c AND m.round = $r RETURN m.to_id, m.kind, m.payload"
			),
			&[("c", Value::Int64(cluster)), ("r", Value::Int64(round))],
		)?;
		Ok(rows
			.into_iter()
			.filter_map(|r| {
				let to_id = match r.get(0).cloned().flatten() {
					Some(Value::Int64(v)) => v,
					_ => return None,
				};
				let kind = match r.get(1).cloned().flatten() {
					Some(Value::Int64(v)) => v,
					_ => return None,
				};
				let payload = match r.get(2).cloned().flatten() {
					Some(Value::Int64(v)) => v,
					_ => return None,
				};
				Some((to_id, kind, payload))
			})
			.collect())
	}

	/// Re-addresses one vertex's already-written round messages from its old home to its new
	/// one (delete + reinsert: the engine refuses in-place partition-key updates). Called by
	/// the migrating owner immediately after the move, so offers written earlier in the same
	/// round follow the vertex instead of orphaning in the old partition. Returns how many
	/// rows moved.
	pub fn forward_msgs(
		&mut self,
		to_id: i64,
		from_cluster: i64,
		to_cluster: i64,
		round: i64,
	) -> Result<i64> {
		if from_cluster == to_cluster {
			return Ok(0);
		}
		let Some(table) = self.msg_table_for_cluster(from_cluster)? else {
			return Ok(0);
		};
		let rows = self.query_params(
			&format!(
				"MATCH (m:{table}) WHERE m.to_id = $t AND m.round = $r RETURN m.kind, m.payload"
				),
			&[("t", Value::Int64(to_id)), ("r", Value::Int64(round))],
		)?;
		let mut stragglers = Vec::new();
		for r in &rows {
			let kind = match r.get(0).cloned().flatten() {
				Some(Value::Int64(v)) => v,
				_ => continue,
			};
			let payload = match r.get(1).cloned().flatten() {
				Some(Value::Int64(v)) => v,
				_ => continue,
			};
			stragglers.push((kind, payload));
		}
		if stragglers.is_empty() {
			return Ok(0);
		}
		self.update_params(
			&format!("MATCH (m:{table}) WHERE m.to_id = $t AND m.round = $r DELETE m"),
			&[("t", Value::Int64(to_id)), ("r", Value::Int64(round))],
		)?;
		for (kind, payload) in &stragglers {
			self.write_cluster_msg(to_id, to_cluster, *kind, *payload, round)?;
		}
		Ok(stragglers.len() as i64)
	}

	/// Every vertex row in the store, in `id` order, via the parent union. Used to build the
	/// coordinator's directory snapshot, live-cluster set, and final reports — all bulk reads
	/// that intentionally span partitions instead of addressing one owner's slice.
	pub fn read_all_vertices(&mut self) -> Result<Vec<Vertex>> {
		let rows = self.query(
			"MATCH (v:Vertex) \
			 RETURN v.id, v.server, v.cluster, v.value, v.degree, v.core, v.active ORDER BY v.id",
		)?;
		rows.iter().map(|r| Vertex::parse(r)).collect()
	}

	pub fn count_msgs(&mut self) -> Result<i64> {
		Ok(self
			.scalar_i64("MATCH (m:Msg) RETURN count(m)")?
			.unwrap_or(0))
	}

	pub fn count_msgs_round(&mut self, round: i64) -> Result<i64> {
		Ok(self
			.scalar_i64(&format!(
				"MATCH (m:Msg) WHERE m.round = {round} RETURN count(m)"
			))?
			.unwrap_or(0))
	}

	pub fn clear_msgs(&mut self) -> Result<()> {
		self.update("MATCH (m:Msg) DELETE m")
	}

	// -- run bookkeeping -------------------------------------------------------

	pub fn start_run(&mut self, run_id: i64, k: i64) -> Result<()> {
		self.update(&format!(
			"CREATE (:Run {{run_id: {run_id}, k: {k}, rounds: 0, done: false}})"
		))
	}

	pub fn mark_done(&mut self, run_id: i64, rounds: i64) -> Result<()> {
		self.update(&format!(
			"MATCH (r:Run {{run_id: {run_id}}}) SET r.rounds = {rounds}, r.done = true"
		))
	}

	pub fn run_status(&mut self, run_id: i64) -> Result<(i64, i64, bool)> {
		let rows = self.query_params(
			"MATCH (r:Run {run_id: $id}) RETURN r.k, r.rounds, r.done",
			&[("id", Value::Int64(run_id))],
		)?;
		let row = rows
			.first()
			.ok_or_else(|| anyhow::anyhow!("run {run_id} not found"))?;
		let k = match row.get(0).cloned().flatten() {
			Some(Value::Int64(v)) => v,
			_ => bail!("run {run_id} missing k"),
		};
		let rounds = match row.get(1).cloned().flatten() {
			Some(Value::Int64(v)) => v,
			_ => 0,
		};
		let done = match row.get(2).cloned().flatten() {
			Some(Value::Bool(b)) => b,
			_ => false,
		};
		Ok((k, rounds, done))
	}

	/// Persists a vertex's computed result back into the graph (the final step of every algorithm).
	/// Only non-partition columns are set: `cluster` is deliberately excluded, since the engine
	/// refuses partition-column updates — community changes go through
	/// [`GraphDb::move_vertex_to_cluster`] instead.
	pub fn persist_vertex(&mut self, v: &Vertex) -> Result<()> {
		self.update(&format!(
			"MATCH (x:Vertex {{id: {}}}) SET x.value = {}, x.core = {}, x.active = {}, x.degree = {}",
			v.id,
			v.value,
			v.core,
			bool_literal(v.active),
			v.degree
		))
	}
}

fn literal(value: &Value) -> String {
	match value {
		Value::Int64(v) => v.to_string(),
		Value::Int32(v) => v.to_string(),
		Value::Bool(b) => bool_literal(*b),
		Value::String(s) => format!("'{}'", s.replace('\'', "\\'")),
		_ => format!("{value}"),
	}
}

fn bool_literal(b: bool) -> String {
	if b { "true".into() } else { "false".into() }
}

/// Seeds a small, interesting graph used by the demo and tests. Returns the seeded vertices.
///
/// The graph is a "house" (a 2-core triangle/square ring) plus a pendant vertex on a tail, so a
/// k=2 core keeps the house but the pendant drops out, demonstrating message-passing peeling.
pub fn seed_demo_graph(db: &mut GraphDb) -> Result<Vec<Vertex>> {
	// undirected edges as (from, to) pairs (we store both directions).
	let edges: &[(i64, i64)] = &[
		(0, 1),
		(1, 2),
		(2, 0), // triangle -> 2-core
		(2, 3),
		(3, 4),
		(4, 2), // square ring sharing vertex 2 -> reinforced 2-core
		(4, 5), // pendant tail
		(5, 6), // further tail
		(6, 7), // tail end vertex 7 has degree 1
	];
	seed_edges(db, edges)
}

/// Seeds the given undirected edges: creates the `Vertex` rows (per-shard compute routing,
/// each vertex its own initial community `cluster = id`, initial degree, value = own id) and
/// both directed `Edge` relationships.
///
/// Vertices are created through the parent so the engine routes each row to its cluster
/// partition, creating it on demand. The `Edge` rel table is declared *after* those writes so
/// its rel pairs cover every initial cluster: rel coverage over a LIST parent is frozen at
/// rel-table creation, and a later-born partition would have no pairs. Algorithms only move
/// vertices onto already-seeded cluster values, so the domain stays complete. Edges name
/// concrete partitions (the engine refuses parent-bound rel writes on partitioned tables).
pub fn seed_edges(db: &mut GraphDb, edges: &[(i64, i64)]) -> Result<Vec<Vertex>> {
	let mut deg: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
	for (a, b) in edges {
		*deg.entry(*a).or_insert(0) += 1;
		*deg.entry(*b).or_insert(0) += 1;
	}
	let mut ids: Vec<i64> = deg.keys().copied().collect();
	ids.sort_unstable();

	let mut vertices = Vec::with_capacity(ids.len());
	for id in ids {
		let server = id % NUM_SERVERS;
		let degree = deg[&id];
		let v = Vertex {
			id,
			server,
			cluster: id,
			value: id,
			degree,
			core: 0,
			active: true,
		};
		db.create_vertex(&v)?;
		vertices.push(v);
	}

	// Declare rels once every initial cluster partition exists, so all pairs are covered.
	db.update("CREATE REL TABLE IF NOT EXISTS Edge(FROM Vertex TO Vertex)")?;
	db.router.refresh_clusters(&mut db.db)?;

	let cluster_of: std::collections::HashMap<i64, i64> =
		vertices.iter().map(|v| (v.id, v.cluster)).collect();
	let cluster = |id: &i64| {
		cluster_of
			.get(id)
			.copied()
			.ok_or_else(|| anyhow::anyhow!("edge endpoint {id} has no seeded vertex"))
	};
	for (a, b) in edges {
		let (ca, cb) = (cluster(a)?, cluster(b)?);
		db.create_edge_directed(*a, ca, *b, cb)?;
		db.create_edge_directed(*b, cb, *a, ca)?;
	}
	Ok(vertices)
}
