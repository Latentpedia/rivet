//! Distributed message-passing graph algorithms over the partitioned store.
//!
//! Each algorithm runs in **supersteps** (a bulk-synchronous parallel model).
//! The vertex set is partitioned across live communities ("clusters"), and
//! each cluster is owned by exactly one actor (see [`crate::clusters`]).
//! Owners never contact each other directly; their only channel is the shared
//! LadybugDB graph store, and every read/write through that channel goes over
//! [`adbc_core`] (see [`crate::graph::GraphDb`]).
//!
//! A superstep is a barrier round:
//!
//! 1. Each cluster's owner reads its community's rows (via a pruned ADBC read
//!    over its `Vertex` partition) plus its inbox slice for the current round
//!    (a pruned read over its `Msg` partition).
//! 2. Each vertex applies the messages to its local state and, if its state
//!    changed, writes messages to its neighbors for the next round, addressed
//!    by each neighbor's current home cluster.
//! 3. The coordinator counts how many messages were produced; when a round
//!    produces none, the fixed point is reached and the algorithm terminates.
//!
//! The `round` column on `Msg` keeps the barrier clean across owners: a
//! message written in round `r` is only ever consumed by the round-`r+1`
//! pass, so a later owner in the same superstep never observes an earlier
//! owner's just-written messages.
//!
//! Two algorithms are implemented:
//!
//! - [`Algorithm::KCore`] — k-core membership by message-passing peeling. A vertex whose effective
//!   degree drops below `k` leaves the core and tells its neighbors by a *decrement* message; the
//!   neighbors lower their degree and peel in turn. Surviving vertices form the k-core, persisted
//!   as `active = true, core = k`.
//! - [`Algorithm::Wcc`] — weakly connected components. Components are labels; each vertex adopts
//!   the smallest label it hears (its own to start) and propagates the improvement to its
//!   neighbors. The label is the live `cluster` partition key, so every improvement
//!   physically migrates the row with delete + insert
//!   ([`GraphDb::move_vertex_to_cluster`]); the final per-vertex label is also mirrored in
//!   `value`.

use anyhow::{Result, bail};

use crate::clusters::{EngineStats, directory, live_clusters, run_cluster_superstep};
use crate::graph::{GraphDb, Vertex};

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

/// Drives an algorithm to a fixed point across the live clusters and persists the final state.
///
/// This is the coordinator. It is ownership-agnostic: it only advances the superstep barrier
/// and counts messages. The per-cluster compute lives in
/// [`run_cluster_superstep`](crate::clusters::run_cluster_superstep), which the Rivet
/// [`Coordinator`](crate::actors::Coordinator) invokes once per owner per round; this driver
/// runs the same protocol inline for tests and the standalone demo.
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

	/// Runs `algo` to a fixed point with message-locality counters. The graph must already be
	/// seeded and `start_run` called.
	pub fn run_with_stats(mut self, algo: Algorithm) -> Result<(RunOutcome, EngineStats)> {
		let mut stats = EngineStats::default();
		// The directory snapshot assigns every vertex to its round's owner; reports update
		// it, so the next round routes invocations (and the next run's reads) correctly.
		let mut directory = directory(&mut self.db)?;
		let mut produced = i64::MAX;
		let mut round = 0i64;
		let mut rounds = 0i64;

		while produced > 0 {
			rounds += 1;
			if rounds > MAX_SUPERSTEPS {
				bail!("algorithm did not converge within {MAX_SUPERSTEPS} supersteps");
			}
			produced = 0;
			for cluster in live_clusters(&directory) {
				let member_ids: Vec<i64> = directory
					.iter()
					.filter(|(_, home)| **home == cluster)
					.map(|(id, _)| *id)
					.collect();
				let step = run_cluster_superstep(&mut self.db, cluster, &member_ids, round, algo)?;
				produced += step.produced;
				stats.local_msgs += step.local;
				stats.remote_msgs += step.remote;
				for m in step.migrated {
					directory.insert(m.id, m.to);
				}
			}
			round += 1;
		}

		// Finalize: surviving k-core vertices carry the core value; report all vertices.
		// The store already holds every update (it is the live state, not a sink), so this
		// only stamps the survivors and clears the spent inbox slices.
		if let Algorithm::KCore { k } = algo {
			for v in self.db.read_all_vertices()? {
				if v.active {
					self.db.persist_vertex(&Vertex { core: k, ..v })?;
				}
			}
		}

		self.db.clear_msgs()?;

		let mut vertices = self.db.read_all_vertices()?;
		vertices.sort_by_key(|v| v.id);
		Ok((RunOutcome { rounds, vertices }, stats))
	}

	/// Runs `algo` to a fixed point. The graph must already be seeded and `start_run` called.
	pub fn run(self, algo: Algorithm) -> Result<RunOutcome> {
		Ok(self.run_with_stats(algo)?.0)
	}
}
