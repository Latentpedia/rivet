//! Ownership tests for model `B`: each actor owns one cluster slice.
//!
//! These drive the same store-backed protocol the Rivet actors run over
//! action calls, without the engine: seed the partitioned tables, superstep
//! one owned slice at a time through [`run_cluster_superstep`](example_ladybug_graph::clusters::run_cluster_superstep),
//! and assert results, the ownership invariant, and the locality story
//! (intra-cluster traffic stays inside the owning slice).

use std::collections::HashSet;

use example_ladybug_graph::algorithm::{Algorithm, Coordinator};
use example_ladybug_graph::clusters::{
	check_store_ownership, directory, live_clusters, run_cluster_superstep,
};
use example_ladybug_graph::graph::{GraphDb, seed_demo_graph};
use example_ladybug_graph::ladybug_server::{LadybugServer, ServerHandle};
use tempfile::TempDir;

/// A real LadybugDB server with a unique on-disk store per test, so every
/// test exercises the full remote columnar protocol rather than an embedded
/// shortcut.
struct TestServer {
	_dir: TempDir,
	handle: ServerHandle,
}

impl TestServer {
	fn new() -> TestServer {
		let dir = TempDir::new().unwrap();
		let server = LadybugServer::open(dir.path().join("graph.lbdb")).unwrap();
		let handle = ServerHandle::start(server).unwrap();
		TestServer { _dir: dir, handle }
	}

	fn open_db(&self) -> GraphDb {
		GraphDb::open(&self.handle.url).unwrap()
	}
}

fn seed_demo(server: &TestServer, run_id: i64, k: i64) -> GraphDb {
	let mut db = server.open_db();
	db.create_schema().unwrap();
	db.start_run(run_id, k).unwrap();
	seed_demo_graph(&mut db).unwrap();
	db
}

#[test]
fn kcore2_peels_tail_without_migrations() {
	let server = TestServer::new();
	let db = seed_demo(&server, 1, 2);
	let (outcome, stats) = Coordinator::new(db).run_with_stats(Algorithm::KCore { k: 2 }).unwrap();

	let active: HashSet<i64> =
		outcome.vertices.iter().filter(|v| v.active).map(|v| v.id).collect();
	assert_eq!(active, HashSet::from([0, 1, 2, 3, 4]));
	let removed: HashSet<i64> =
		outcome.vertices.iter().filter(|v| !v.active).map(|v| v.id).collect();
	assert_eq!(removed, HashSet::from([5, 6, 7]));
	for v in outcome.vertices.iter().filter(|v| v.active) {
		assert_eq!(v.core, 2);
	}
	assert!(outcome.rounds >= 2);
	// K-core never moves a vertex: every row still owns its seed cluster, so
	// every message crosses slices — the no-locality contrast to WCC below.
	for v in &outcome.vertices {
		assert_eq!(v.cluster, v.id);
	}
	assert!(stats.remote_msgs > 0, "peeling must cross slices");
	assert_eq!(stats.local_msgs, 0, "singleton owners never message themselves");

	let mut reopened = server.open_db();
	check_store_ownership(&mut reopened).unwrap();
}

#[test]
fn wcc_collapses_to_one_slice_with_local_traffic() {
	let server = TestServer::new();
	let db = seed_demo(&server, 2, 0);
	let (outcome, stats) = Coordinator::new(db).run_with_stats(Algorithm::Wcc).unwrap();

	assert_eq!(
		outcome.vertices.iter().map(|v| v.value).collect::<HashSet<_>>(),
		HashSet::from([0])
	);
	assert_eq!(
		outcome.vertices.iter().map(|v| v.cluster).collect::<HashSet<_>>(),
		HashSet::from([0]),
		"all rows must live in one community slice"
	);
	// The locality payoff: early rounds route between singleton slices, then
	// traffic turns local as the community collapses into one partition.
	assert!(stats.remote_msgs > 0, "initial offers cross slices");
	assert!(stats.local_msgs > 0, "collapsed community talks to itself");

	let mut reopened = server.open_db();
	check_store_ownership(&mut reopened).unwrap();
	let members: HashSet<i64> = reopened
		.read_cluster(0)
		.unwrap()
		.iter()
		.map(|v| v.id)
		.collect();
	assert_eq!(members, HashSet::from([0, 1, 2, 3, 4, 5, 6, 7]));
}

#[test]
fn wcc_splits_disconnected_graph_into_two_slices() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();
	db.start_run(3, 0).unwrap();
	example_ladybug_graph::graph::seed_edges(
		&mut db,
		&[(0, 1), (1, 2), (2, 0), (10, 11), (11, 12), (12, 10)],
	)
	.unwrap();

	let before = directory(&mut db).unwrap();
	assert_eq!(live_clusters(&before).len(), 6);
	let (outcome, _) = Coordinator::new(db).run_with_stats(Algorithm::Wcc).unwrap();
	let labels: HashSet<i64> = outcome.vertices.iter().map(|v| v.value).collect();
	assert_eq!(labels, HashSet::from([0, 10]));
	let homes: HashSet<i64> = outcome.vertices.iter().map(|v| v.cluster).collect();
	assert_eq!(homes, HashSet::from([0, 10]));
}

#[test]
fn owner_processes_only_its_snapshot() {
	let server = TestServer::new();
	let mut db = seed_demo(&server, 4, 0);
	let before = directory(&mut db).unwrap();

	// Owner of cluster 0 runs round 0 over its one assigned member and offers
	// its label to every neighbor's home slice.
	let step = run_cluster_superstep(&mut db, 0, &[0], 0, Algorithm::Wcc).unwrap();
	assert_eq!(step.processed, 1);
	assert_eq!(step.skipped, 0);
	assert_eq!(step.migrated.len(), 0);
	assert_eq!(step.produced, db.neighbors(0).unwrap().len() as i64);

	// The directory still assigns everyone home: nothing migrated in round 0.
	let after = directory(&mut db).unwrap();
	assert_eq!(after, before);

	// A stranger's id in the assignment fails loudly instead of computing on
	// a slice it does not own.
	let err = run_cluster_superstep(&mut db, 0, &[1], 0, Algorithm::Wcc).unwrap_err();
	assert!(
		format!("{err:#}").contains("ownership"),
		"cross-slice compute should fail loudly, got {err:#}"
	);
}

#[test]
fn messages_land_in_the_addressed_slice() {
	let server = TestServer::new();
	let mut db = seed_demo(&server, 5, 0);
	let homes = directory(&mut db).unwrap();

	// Owner of cluster 0 offers its label in round 0. Every neighbor's home
	// slice must then hold a round-0 COMPONENT message for that neighbor —
	// the store-side half of routing a message to its owning actor.
	run_cluster_superstep(&mut db, 0, &[0], 0, Algorithm::Wcc).unwrap();
	let neighbors = db.neighbors(0).unwrap();
	assert!(!neighbors.is_empty());
	for n in &neighbors {
		let home = homes[n];
		let inbox = db.read_cluster_msgs(home, 0).unwrap();
		assert!(
			inbox.iter().any(|(to, kind, _)| *to == *n
				&& *kind == example_ladybug_graph::algorithm::kind::COMPONENT),
			"neighbor {n} (home {home}) should have a round-0 offer in its slice"
		);
	}
	check_store_ownership(&mut db).unwrap();
}

#[test]
fn empty_graph_converges() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();
	db.start_run(6, 1).unwrap();
	let outcome = Coordinator::new(db).run(Algorithm::KCore { k: 1 }).unwrap();
	assert_eq!(outcome.vertices.len(), 0);
	assert!(outcome.rounds >= 1);
}
