//! End-to-end tests for the distributed message-passing graph algorithms.
//!
//! Each test drives `NUM_SERVERS` shard workers against a single shared LadybugDB store. The
//! workers talk to the store (and so to each other) only through the ADBC bridge, mirroring how
//! `NUM_SERVERS` separate Rivet servers would exchange messages over the graph database.

use std::collections::HashSet;

use example_ladybug_graph::algorithm::{Algorithm, Coordinator};
use example_ladybug_graph::graph::{GraphDb, seed_demo_graph};
use tempfile::TempDir;

fn active_ids(db: &mut GraphDb) -> HashSet<i64> {
	let mut ids = HashSet::new();
	for shard in 0..example_ladybug_graph::graph::NUM_SERVERS {
		for v in db.read_vertices(shard).unwrap() {
			if v.active {
				ids.insert(v.id);
			}
		}
	}
	ids
}

fn component_label(db: &mut GraphDb) -> HashSet<i64> {
	let mut labels = HashSet::new();
	for shard in 0..example_ladybug_graph::graph::NUM_SERVERS {
		for v in db.read_vertices(shard).unwrap() {
			labels.insert(v.value);
		}
	}
	labels
}

#[test]
fn kcore2_computes_and_persists() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("kcore2.lbdb");

	let mut db = GraphDb::open(&path).unwrap();
	db.create_schema().unwrap();
	db.start_run(1, 2).unwrap();
	let _seeded = seed_demo_graph(&mut db).unwrap();

	let outcome = Coordinator::new(db).run(Algorithm::KCore { k: 2 }).unwrap();

	// The 2-core is the house {0..4}; the pendant tail {5,6,7} peels off via message passing.
	let active: HashSet<i64> = outcome
		.vertices
		.iter()
		.filter(|v| v.active)
		.map(|v| v.id)
		.collect();
	assert_eq!(active, HashSet::from([0, 1, 2, 3, 4]), "2-core should keep the house");
	let removed: HashSet<i64> = outcome
		.vertices
		.iter()
		.filter(|v| !v.active)
		.map(|v| v.id)
		.collect();
	assert_eq!(removed, HashSet::from([5, 6, 7]), "pendant tail should be peeled");

	// Survivors carry core == k.
	for v in outcome.vertices.iter().filter(|v| v.active) {
		assert_eq!(v.core, 2, "survivor {} should record core 2", v.id);
	}
	assert!(outcome.rounds >= 2, "message passing should take >= 2 supersteps");

	// Results are persisted in LadybugDB: reopen the store from disk and read them back.
	let mut reopened = GraphDb::open(&path).unwrap();
	assert_eq!(active_ids(&mut reopened), HashSet::from([0, 1, 2, 3, 4]));
	assert_eq!(reopened.count_vertex().unwrap(), 8);
}

#[test]
fn kcore3_empties_the_core() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("kcore3.lbdb");

	let mut db = GraphDb::open(&path).unwrap();
	db.create_schema().unwrap();
	db.start_run(2, 3).unwrap();
	let _seeded = seed_demo_graph(&mut db).unwrap();

	let outcome = Coordinator::new(db).run(Algorithm::KCore { k: 3 }).unwrap();
	// Every vertex had degree <= 4 and the k=3 core collapses to empty.
	assert!(
		outcome.vertices.iter().all(|v| !v.active),
		"3-core of this graph is empty"
	);
	assert_eq!(outcome.vertices.len(), 8);
}

#[test]
fn wcc_finds_a_single_component() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("wcc.lbdb");

	let mut db = GraphDb::open(&path).unwrap();
	db.create_schema().unwrap();
	db.start_run(3, 0).unwrap();
	let _seeded = seed_demo_graph(&mut db).unwrap();

	let outcome = Coordinator::new(db).run(Algorithm::Wcc).unwrap();

	// The demo graph is connected, so every vertex converges to the single smallest label (0).
	assert_eq!(
		outcome.vertices.iter().map(|v| v.value).collect::<HashSet<_>>(),
		HashSet::from([0]),
		"connected graph should collapse to one component"
	);
	// Persisted.
	let mut reopened = GraphDb::open(&path).unwrap();
	assert_eq!(component_label(&mut reopened), HashSet::from([0]));
}

#[test]
fn wcc_splits_disconnected_graph() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("wcc2.lbdb");

	let mut db = GraphDb::open(&path).unwrap();
	db.create_schema().unwrap();
	db.start_run(4, 0).unwrap();
	// Two disconnected triangles.
	example_ladybug_graph::graph::seed_edges(&mut db, &[(0, 1), (1, 2), (2, 0), (10, 11), (11, 12), (12, 10)])
		.unwrap();

	let outcome = Coordinator::new(db).run(Algorithm::Wcc).unwrap();
	let labels: HashSet<i64> = outcome.vertices.iter().map(|v| v.value).collect();
	assert_eq!(labels, HashSet::from([0, 10]), "two components -> two labels");
}

/// The pure-DB algorithm must tolerate an empty graph (converges in one round, no error).
#[test]
fn empty_graph_converges() {
	let mut db = GraphDb::in_memory().unwrap();
	db.create_schema().unwrap();
	db.start_run(5, 1).unwrap();
	let outcome = Coordinator::new(db).run(Algorithm::KCore { k: 1 }).unwrap();
	assert_eq!(outcome.vertices.len(), 0);
	assert!(outcome.rounds >= 1);
}
