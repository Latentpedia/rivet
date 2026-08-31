//! Validates the distributed k-core against monolithic ground truth.
//!
//! `tests/kcore.rs` asserts hand-computed answers for one hand-built graph. That proves the
//! demo works; it does not prove the *exchange* is right, because a hand-built graph can be
//! peeled correctly by an implementation that would still diverge once a removal has to cascade
//! across a shard boundary several times.
//!
//! This test asserts agreement with the definition instead. [`monolithic_kcore`] is a direct
//! transcription of the k-core recurrence — repeatedly delete every vertex whose surviving
//! degree is below `k` — computed in one process with no sharding, no messages, and no database.
//! The distributed [`Coordinator`] then runs the same graph over `NUM_SERVERS` shards that
//! communicate only through `Msg` rows in the shared store, and the two active sets must be
//! identical.
//!
//! The k-core of a graph is unique, so this is an exact comparison: no tolerance, no ordering
//! question, just set equality. Two shapes of input are covered:
//!
//! - **Random graphs** ([`distributed_kcore_matches_monolithic_on_random_graphs`]) at several
//!   densities and values of `k`, so peeling depth varies from "nothing to do" to a five-round
//!   cascade. Generation is a deterministic xorshift seeded per case, so a failure reproduces
//!   exactly rather than only on the run that found it.
//! - **A forced long cascade** ([`long_cross_shard_cascade`]). A triangle survives `k = 2` while
//!   a path glued to it must peel one vertex per superstep. Consecutive ids land on consecutive
//!   shards (`id % NUM_SERVERS`), so every single peel in the chain is a cross-shard event and
//!   the run takes as many supersteps as the tail is long.

use std::collections::{BTreeMap, BTreeSet};

use example_ladybug_graph::algorithm::{Algorithm, Coordinator};
use example_ladybug_graph::graph::{GraphDb, seed_edges};
use example_ladybug_graph::ladybug_server::{LadybugServer, ServerHandle};
use tempfile::TempDir;

/// The k-core, computed monolithically: delete every vertex whose surviving degree is below `k`,
/// and repeat until nothing more can be deleted. Deliberately the naive definition rather than a
/// clever peeling order, so it is obviously correct by inspection.
fn monolithic_kcore(edges: &[(i64, i64)], k: i64) -> BTreeSet<i64> {
	let mut adj: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
	for (a, b) in edges {
		adj.entry(*a).or_default().insert(*b);
		adj.entry(*b).or_default().insert(*a);
	}

	let mut alive: BTreeSet<i64> = adj.keys().copied().collect();
	loop {
		let doomed: Vec<i64> = alive
			.iter()
			.copied()
			.filter(|v| adj[v].iter().filter(|n| alive.contains(n)).count() < k as usize)
			.collect();
		if doomed.is_empty() {
			return alive;
		}
		for v in doomed {
			alive.remove(&v);
		}
	}
}

/// A deterministic xorshift64 generator, so a failing case reproduces from its seed alone.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		let mut x = self.0;
		x ^= x << 13;
		x ^= x >> 7;
		x ^= x << 17;
		self.0 = x;
		x
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}
}

/// `m` distinct undirected edges over `n` vertices, without self-loops, in canonical `(lo, hi)`
/// order. Isolated vertices simply do not appear, which matches how [`seed_edges`] derives the
/// vertex set from the edge list.
fn random_graph(seed: u64, n: i64, m: usize) -> Vec<(i64, i64)> {
	let mut rng = Rng(seed);
	let mut edges: BTreeSet<(i64, i64)> = BTreeSet::new();
	while edges.len() < m {
		let a = rng.below(n as u64) as i64;
		let b = rng.below(n as u64) as i64;
		if a == b {
			continue;
		}
		edges.insert(if a < b { (a, b) } else { (b, a) });
	}
	edges.into_iter().collect()
}

/// Seeds `edges` into a fresh server-owned store and runs the distributed k-core over it,
/// returning the surviving vertex ids and the superstep count.
fn distributed_kcore(edges: &[(i64, i64)], k: i64) -> (BTreeSet<i64>, i64) {
	let dir = TempDir::new().unwrap();
	let server = LadybugServer::open(dir.path().join("graph.lbdb")).unwrap();
	let handle = ServerHandle::start(server).unwrap();

	let mut db = GraphDb::open(&handle.url).unwrap();
	db.create_schema().unwrap();
	db.start_run(1, k).unwrap();
	seed_edges(&mut db, edges).unwrap();

	let outcome = Coordinator::new(db).run(Algorithm::KCore { k }).unwrap();
	let active = outcome
		.vertices
		.iter()
		.filter(|v| v.active)
		.map(|v| v.id)
		.collect();
	(active, outcome.rounds)
}

#[test]
fn distributed_kcore_matches_monolithic_on_random_graphs() {
	// (seed, vertices, edges, k)
	let cases: &[(u64, i64, usize, i64)] = &[
		(0x1234_5678, 12, 18, 2),
		(0xdead_beef, 20, 32, 2),
		(0xfeed_face, 20, 40, 3),
		(0x0bad_c0de, 30, 55, 3),
		(0xcafe_babe, 30, 90, 4),
		(0x5eed_0001, 40, 70, 2),
		(0x5eed_0002, 40, 120, 3),
	];

	// Collect every mismatch before failing, so one run reports the whole picture rather than
	// only the first divergent case.
	let mut failures = Vec::new();
	for (seed, n, m, k) in cases.iter().copied() {
		let edges = random_graph(seed, n, m);
		let expected = monolithic_kcore(&edges, k);
		let (actual, rounds) = distributed_kcore(&edges, k);

		if actual == expected {
			println!(
				"ok  seed={seed:#x} n={n} m={m} k={k} rounds={rounds} core_size={}",
				actual.len()
			);
			continue;
		}
		failures.push(format!(
			"seed={seed:#x} n={n} m={m} k={k} rounds={rounds}\n  \
			 monolithic  = {expected:?}\n  \
			 distributed = {actual:?}\n  \
			 peeled but should have survived = {:?}\n  \
			 survived but should have peeled = {:?}",
			expected.difference(&actual).collect::<Vec<_>>(),
			actual.difference(&expected).collect::<Vec<_>>(),
		));
	}

	assert!(
		failures.is_empty(),
		"distributed k-core diverged from monolithic ground truth:\n{}",
		failures.join("\n")
	);
}

#[test]
fn long_cross_shard_cascade() {
	// A triangle (survives k = 2) with a long path glued to it. Every path vertex must peel, one
	// per superstep, and consecutive ids sit on different shards, so each peel in the chain is a
	// cross-shard decrement.
	let mut edges = vec![(0i64, 1i64), (1, 2), (2, 0)];
	for id in 2..25i64 {
		edges.push((id, id + 1));
	}

	let expected = monolithic_kcore(&edges, 2);
	let (actual, rounds) = distributed_kcore(&edges, 2);

	println!("cascade rounds={rounds} monolithic={expected:?} distributed={actual:?}");
	assert_eq!(
		actual, expected,
		"a peel cascading across shard boundaries must reach the same fixed point"
	);
	assert!(
		rounds >= 20,
		"the tail should peel one vertex per superstep, not collapse at once (got {rounds})"
	);
}
