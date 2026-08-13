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
//! typed operations over them, all backed by [`adbc_core`].

use anyhow::{Result, bail};
use lbug::Value;

use adbc_core::sync::Driver;

use crate::{
	adbc::{LadybugDb, LadybugDriver},
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
			("value", ColType::Int64),
			("degree", ColType::Int64),
			("core", ColType::Int64),
			("active", ColType::Bool),
		],
	)
}

/// A typed vertex row. `to_props` builds the graph row through the schema-checked builder so a
/// wrong key or value type is caught before it reaches the store.
#[derive(Debug, Clone)]
pub struct Vertex {
	pub id: i64,
	pub server: i64,
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
			"value" => self.value,
			"degree" => self.degree,
			"core" => self.core,
			"active" => self.active,
		})
	}

	/// Parses a row of `[id, server, value, degree, core, active]` (the projection order used by
	/// [`GraphDb::read_vertices`]).
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
pub struct GraphDb {
	db: LadybugDb,
}

impl GraphDb {
	pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self> {
		let mut driver = LadybugDriver::new(path.into());
		let db = driver
			.new_database()
			.map_err(|e| anyhow::anyhow!("adbc open failed: {e}"))?;
		Ok(GraphDb { db })
	}

	pub fn in_memory() -> Result<Self> {
		let mut driver = LadybugDriver::in_memory();
		let db = driver
			.new_database()
			.map_err(|e| anyhow::anyhow!("adbc open failed: {e}"))?;
		Ok(GraphDb { db })
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

	pub fn scalar_i64(&mut self, cypher: &str) -> Result<Option<i64>> {
		self.db
			.scalar_i64(cypher)
			.map_err(|e| anyhow::anyhow!("adbc scalar failed: {e}"))
	}

	// -- schema + seeding (all through typed properties) ----------------------

	/// Creates the node/rel/message/run table schemas and seeds the graph.
	pub fn create_schema(&mut self) -> Result<()> {
		self.update(
			"CREATE NODE TABLE IF NOT EXISTS Vertex(id INT64, server INT64, value INT64, \
			 degree INT64, core INT64, active BOOLEAN, PRIMARY KEY(id))",
		)?;
		self.update("CREATE REL TABLE IF NOT EXISTS Edge(FROM Vertex TO Vertex)")?;
		self.update(
			"CREATE NODE TABLE IF NOT EXISTS Msg(msg_id SERIAL, to_id INT64, server INT64, \
			 kind INT64, payload INT64, round INT64, PRIMARY KEY(msg_id))",
		)?;
		self.update(
			"CREATE NODE TABLE IF NOT EXISTS Run(run_id INT64, k INT64, rounds INT64, \
			 done BOOLEAN, PRIMARY KEY(run_id))",
		)?;
		Ok(())
	}

	/// Inserts a vertex row through the typed schema-checked builder.
	pub fn create_vertex(&mut self, v: &Vertex) -> Result<()> {
		let table = vertex_table();
		let typed = TypedProps::new(table);
		let kv = typed
			.try_set("id", v.id)?
			.try_set("server", v.server)?
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

	/// Reads the vertices owned by `server` (that server's shard), in `id` order.
	pub fn read_vertices(&mut self, server: i64) -> Result<Vec<Vertex>> {
		let rows = self.query_params(
			"MATCH (v:Vertex) WHERE v.server = $s \
			 RETURN v.id, v.server, v.value, v.degree, v.core, v.active ORDER BY v.id",
			&[("s", Value::Int64(server))],
		)?;
		rows.iter().map(|r| Vertex::parse(r)).collect()
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
		Ok(self.scalar_i64("MATCH (v:Vertex) RETURN count(v)")?.unwrap_or(0))
	}

	// -- message passing (the ADBC inter-instance channel) --------------------

	/// Writes a message row targeting `to_id` in `round`. `server` is derived from `to_id` so a
	/// worker can read exactly the messages aimed at its own shard, and the `round` column keeps
	/// superstep barriers clean: a message is only consumed by the worker for the shard+round it
	/// targets.
	pub fn write_msg_round(&mut self, to_id: i64, kind: i64, payload: i64, round: i64) -> Result<()> {
		let server = to_id % NUM_SERVERS;
		self.update(&format!(
			"CREATE (:Msg {{to_id: {to_id}, server: {server}, kind: {kind}, payload: {payload}, round: {round}}})"
		))
	}

	/// Reads messages of one round targeting `server`'s shard, returning `(to_id, kind, payload)`.
	pub fn read_msgs_round(&mut self, server: i64, round: i64) -> Result<Vec<(i64, i64, i64)>> {
		let rows = self.query_params(
			"MATCH (m:Msg) WHERE m.server = $s AND m.round = $r RETURN m.to_id, m.kind, m.payload",
			&[("s", Value::Int64(server)), ("r", Value::Int64(round))],
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

	pub fn write_msg(&mut self, to_id: i64, kind: i64, payload: i64) -> Result<()> {
		self.write_msg_round(to_id, kind, payload, 0)
	}

	pub fn read_msgs(&mut self, server: i64) -> Result<Vec<(i64, i64, i64)>> {
		self.read_msgs_round(server, 0)
	}

	pub fn count_msgs(&mut self) -> Result<i64> {
		Ok(self.scalar_i64("MATCH (m:Msg) RETURN count(m)")?.unwrap_or(0))
	}

	pub fn count_msgs_round(&mut self, round: i64) -> Result<i64> {
		Ok(self
			.scalar_i64(&format!("MATCH (m:Msg) WHERE m.round = {round} RETURN count(m)"))?
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
	pub fn persist_vertex(&mut self, v: &Vertex) -> Result<()> {
		self.update(&format!(
			"MATCH (x:Vertex {{id: {}}}) SET x.value = {}, x.core = {}, x.active = {}, x.degree = {}",
			v.id, v.value, v.core, bool_literal(v.active), v.degree
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
	if b {
		"true".into()
	} else {
		"false".into()
	}
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

/// Seeds the given undirected edges: creates the `Vertex` rows (with per-shard routing, initial
/// degree, value = own id) and both directed `Edge` relationships.
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
			value: id,
			degree,
			core: 0,
			active: true,
		};
		db.create_vertex(&v)?;
		vertices.push(v);
	}

	for (a, b) in edges {
		db.update(&format!(
			"MATCH (x:Vertex {{id: {a}}}), (y:Vertex {{id: {b}}}) CREATE (x)-[:Edge]->(y)"
		))?;
		db.update(&format!(
			"MATCH (x:Vertex {{id: {b}}}), (y:Vertex {{id: {a}}}) CREATE (x)-[:Edge]->(y)"
		))?;
	}
	Ok(vertices)
}
