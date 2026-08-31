//! Distributed message-passing graph algorithms over the shared ADBC graph store.
//!
//! Each algorithm runs in **supersteps** (a bulk-synchronous parallel / Pregel-style model). The
//! vertex set is partitioned across a fixed number of shards ("servers"), and each server owns one
//! shard. Servers never contact each other directly; their only channel is the shared LadybugDB
//! graph store, and every read/write through that channel goes over [`adbc_core`] (see
//! [`crate::graph::GraphDb`]).
//!
//! A superstep is a barrier round:
//!
//! 1. Each server's worker reads the messages aimed at its shard for the current round (via an
//!    ADBC read over the `Msg` table) plus the current state of the vertices it owns.
//! 2. Each vertex applies the messages to its local state and, if its state changed, writes
//!    messages to its neighbors for the next round (via ADBC writes to the `Msg` table).
//! 3. The coordinator counts how many messages were produced; when a round produces none, the
//!    fixed point is reached and the algorithm terminates.
//!
//! The `round` column on `Msg` keeps the barriers clean across shards: a message written in round
//! `r` is only ever consumed by the round-`r+1` pass, so a later server in the same superstep never
//! observes an earlier server's just-written messages.
//!
//! Two algorithms are implemented:
//!
//! - [`Algorithm::KCore`] — k-core membership by message-passing peeling. A vertex whose effective
//!   degree drops below `k` leaves the core and tells its neighbors by a *decrement* message; the
//!   neighbors lower their degree and peel in turn. Surviving vertices form the k-core, persisted
//!   as `active = true, core = k`.
//! - [`Algorithm::Wcc`] — weakly connected components. Components are labels; each vertex adopts
//!   the smallest label it hears (its own to start) and propagates the improvement to its
//!   neighbors. The final per-vertex label is persisted in `value`.

use anyhow::{Context, Result, bail};

use crate::graph::{GraphDb, Vertex, num_servers};

/// Message kinds written to the shared `Msg` table.
pub mod kind {
	/// Tells a vertex that one of its neighbors left the k-core, so its effective degree drops.
	pub const DECREMENT: i64 = 1;
	/// Carries a candidate component label (WCC).
	pub const COMPONENT: i64 = 2;
}

/// A message-passing graph algorithm run over the shared store.
#[derive(Debug, Clone, Copy)]
pub enum Algorithm {
	/// k-core membership for a fixed `k`.
	KCore { k: i64 },
	/// Weakly connected components.
	Wcc,
}

/// The result of a running an algorithm to a fixed point.
#[derive(Debug)]
pub struct RunOutcome {
	pub rounds: i64,
	pub vertices: Vec<Vertex>,
}

const MAX_SUPERSTEPS: i64 = 10_000;

/// Runs one superstep for the vertices owned by `shard`. `round` is the current superstep index.
/// Returns the number of messages this shard wrote for the next round. Public so a Rivet worker
/// actor can run exactly its own shard of one superstep.
pub fn run_superstep(db: &mut GraphDb, shard: i64, round: i64, algo: Algorithm) -> Result<i64> {
	let vertices = db.read_vertices(shard)?;
	// Only rounds >= 1 have messages aimed at them: superstep 0 processes the initial state.
	let msgs = if round > 0 {
		db.read_msgs_round(shard, round - 1)?
	} else {
		Vec::new()
	};

	let mut produced = 0i64;
	for v in &vertices {
		if !v.active {
			continue;
		}
		run_vertex(db, v, &msgs, round, algo, &mut produced)?;
	}
	Ok(produced)
}

fn run_vertex(
	db: &mut GraphDb,
	v: &Vertex,
	msgs: &[(i64, i64, i64)],
	round: i64,
	algo: Algorithm,
	produced: &mut i64,
) -> Result<()> {
	match algo {
		Algorithm::KCore { k } => {
			// Sum the incoming decrements targeting this vertex.
			let decr: i64 = msgs
				.iter()
				.filter(|(to, k2, _)| *to == v.id && *k2 == kind::DECREMENT)
				.map(|(_, _, p)| *p)
				.sum();
			let new_degree = v.degree - decr;

			if new_degree < k {
				// Leave the k-core and propagate the drop to every remaining neighbor, which
				// decrements their effective degree in the next superstep.
				let removed = Vertex {
					degree: new_degree,
					core: k,
					active: false,
					..v.clone()
				};
				db.persist_vertex(&removed)
					.context("persist k-core removal")?;
				for n in db.neighbors(v.id)? {
					db.write_msg_round(n, kind::DECREMENT, 1, round)?;
					*produced += 1;
				}
			} else if new_degree != v.degree {
				let updated = Vertex {
					degree: new_degree,
					..v.clone()
				};
				db.persist_vertex(&updated).context("persist degree")?;
			}
		}
		Algorithm::Wcc => {
			// Smallest component label this vertex hears.
			let mut label = v.value;
			for (to, k2, p) in msgs {
				if *to == v.id && *k2 == kind::COMPONENT && *p < label {
					label = *p;
				}
			}
			if label < v.value {
				let updated = Vertex {
					value: label,
					..v.clone()
				};
				db.persist_vertex(&updated).context("persist wcc label")?;
				for n in db.neighbors(v.id)? {
					db.write_msg_round(n, kind::COMPONENT, label, round)?;
					*produced += 1;
				}
			} else if round == 0 {
				// Seed: in the first superstep each vertex offers its own label to its neighbors
				// so propagation has a starting point.
				for n in db.neighbors(v.id)? {
					db.write_msg_round(n, kind::COMPONENT, v.value, round)?;
					*produced += 1;
				}
			}
		}
	}
	Ok(())
}

/// Drives an algorithm to a fixed point across `num_servers` shards and persists the final state.
///
/// This is the coordinator. It is shard-agnostic; it only advances the superstep barrier and
/// counts messages, both through ADBC reads/writes on the shared store.
pub struct Coordinator {
	db: GraphDb,
}

impl Coordinator {
	pub fn new(db: GraphDb) -> Self {
		Coordinator { db }
	}

	pub fn db(&mut self) -> &mut GraphDb {
		&mut self.db
	}

	pub fn into_db(self) -> GraphDb {
		self.db
	}

	/// Runs `algo` to a fixed point. The graph must already be seeded and `start_run` called.
	pub fn run(mut self, algo: Algorithm) -> Result<RunOutcome> {
		let mut produced = i64::MAX;
		let mut round = 0i64;
		let mut rounds = 0i64;

		while produced > 0 {
			rounds += 1;
			if rounds > MAX_SUPERSTEPS {
				bail!("algorithm did not converge within {MAX_SUPERSTEPS} supersteps");
			}
			produced = 0;
			for shard in 0..num_servers() {
				produced += run_superstep(&mut self.db, shard, round, algo)?;
			}
			round += 1;
		}

		// Finalize: surviving k-core vertices carry the core value; report all vertices.
		if let Algorithm::KCore { k } = algo {
			for shard in 0..num_servers() {
				for v in self.db.read_vertices(shard)? {
					if v.active {
						let survivor = Vertex { core: k, ..v };
						self.db
							.persist_vertex(&survivor)
							.context("set survivor core")?;
					}
				}
			}
		}

		self.db.clear_msgs()?;

		let mut vertices = Vec::new();
		for shard in 0..num_servers() {
			vertices.extend(self.db.read_vertices(shard)?);
		}
		vertices.sort_by_key(|v| v.id);
		Ok(RunOutcome { rounds, vertices })
	}
}
