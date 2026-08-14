//! End-to-end tests for the distributed message-passing graph algorithms.
//!
//! Each test stands up a real LadybugDB server on an ephemeral port and drives `num_servers`
//! shard workers against it. The workers talk to the store (and so to each other) only through
//! the remote ADBC bridge, mirroring how `num_servers` separate Rivet servers would exchange
//! messages over the graph database across machines.

use std::collections::HashSet;

use example_ladybug_graph::algorithm::{Algorithm, Coordinator};
use example_ladybug_graph::graph::{GraphDb, seed_demo_graph};
use example_ladybug_graph::ladybug_server::{LadybugServer, ServerHandle};
use tempfile::TempDir;

/// A real LadybugDB server process (in-thread) with a unique on-disk store per test, so every
/// test exercises the full remote columnar protocol rather than an embedded shortcut.
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

fn active_ids(db: &mut GraphDb) -> HashSet<i64> {
	let mut ids = HashSet::new();
	for shard in 0..example_ladybug_graph::graph::num_servers() {
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
	for shard in 0..example_ladybug_graph::graph::num_servers() {
		for v in db.read_vertices(shard).unwrap() {
			labels.insert(v.value);
		}
	}
	labels
}

#[test]
fn kcore2_computes_and_persists() {
	let server = TestServer::new();
	let mut db = server.open_db();
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
	assert_eq!(
		active,
		HashSet::from([0, 1, 2, 3, 4]),
		"2-core should keep the house"
	);
	let removed: HashSet<i64> = outcome
		.vertices
		.iter()
		.filter(|v| !v.active)
		.map(|v| v.id)
		.collect();
	assert_eq!(
		removed,
		HashSet::from([5, 6, 7]),
		"pendant tail should be peeled"
	);

	// Survivors carry core == k.
	for v in outcome.vertices.iter().filter(|v| v.active) {
		assert_eq!(v.core, 2, "survivor {} should record core 2", v.id);
	}
	assert!(
		outcome.rounds >= 2,
		"message passing should take >= 2 supersteps"
	);

	// Results are persisted in LadybugDB: reopen the remote store and read them back.
	let mut reopened = server.open_db();
	assert_eq!(active_ids(&mut reopened), HashSet::from([0, 1, 2, 3, 4]));
	assert_eq!(reopened.count_vertex().unwrap(), 8);
}

#[test]
fn kcore3_empties_the_core() {
	let server = TestServer::new();
	let mut db = server.open_db();
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
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();
	db.start_run(3, 0).unwrap();
	let _seeded = seed_demo_graph(&mut db).unwrap();

	let outcome = Coordinator::new(db).run(Algorithm::Wcc).unwrap();

	// The demo graph is connected, so every vertex converges to the single smallest label (0).
	assert_eq!(
		outcome
			.vertices
			.iter()
			.map(|v| v.value)
			.collect::<HashSet<_>>(),
		HashSet::from([0]),
		"connected graph should collapse to one component"
	);
	// Persisted.
	let mut reopened = server.open_db();
	assert_eq!(component_label(&mut reopened), HashSet::from([0]));
}

#[test]
fn wcc_splits_disconnected_graph() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();
	db.start_run(4, 0).unwrap();
	// Two disconnected triangles.
	example_ladybug_graph::graph::seed_edges(
		&mut db,
		&[(0, 1), (1, 2), (2, 0), (10, 11), (11, 12), (12, 10)],
	)
	.unwrap();

	let outcome = Coordinator::new(db).run(Algorithm::Wcc).unwrap();
	let labels: HashSet<i64> = outcome.vertices.iter().map(|v| v.value).collect();
	assert_eq!(
		labels,
		HashSet::from([0, 10]),
		"two components -> two labels"
	);
}

/// The pure-DB algorithm must tolerate an empty graph (converges in one round, no error).
#[test]
fn empty_graph_converges() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();
	db.start_run(5, 1).unwrap();
	let outcome = Coordinator::new(db).run(Algorithm::KCore { k: 1 }).unwrap();
	assert_eq!(outcome.vertices.len(), 0);
	assert!(outcome.rounds >= 1);
}

/// Vertex placement, message routing and the coordinator's barrier must all agree on how many
/// shards exist. If the coordinator drives fewer shards than the router used, the extra shards are
/// never polled: their vertices never peel and the messages aimed at them are never consumed, with
/// no error raised anywhere. Sweeping the shards the coordinator would drive has to account for
/// every vertex in the store, and for every message the router placed.
#[test]
fn every_vertex_and_message_lands_on_a_shard_the_coordinator_drives() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();
	db.start_run(6, 2).unwrap();
	seed_demo_graph(&mut db).unwrap();

	let shards = example_ladybug_graph::graph::num_servers();
	let swept: i64 = (0..shards)
		.map(|shard| db.read_vertices(shard).unwrap().len() as i64)
		.sum();
	assert_eq!(
		swept,
		db.count_vertex().unwrap(),
		"sweeping shards 0..{shards} must reach every seeded vertex"
	);

	// Route one message at every vertex and check the coordinator's sweep collects them all.
	for v in 0..8i64 {
		db.write_msg_round(v, example_ladybug_graph::algorithm::kind::DECREMENT, 1, 0)
			.unwrap();
	}
	let routed: i64 = (0..shards)
		.map(|shard| db.read_msgs_round(shard, 0).unwrap().len() as i64)
		.sum();
	assert_eq!(
		routed,
		db.count_msgs().unwrap(),
		"every routed message must land on a shard the coordinator polls"
	);
}
