//! End-to-end tests for the distributed message-passing graph algorithms.
//!
//! Each test stands up a real LadybugDB server on an ephemeral port and drives `NUM_SERVERS`
//! shard workers against it. The workers talk to the store (and so to each other) only through
//! the remote ADBC bridge, mirroring how `NUM_SERVERS` separate Rivet servers would exchange
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

/// LIST partitioning: seeding creates one partition per distinct cluster value on demand
/// (each vertex starts as its own community, `cluster = id`), and the router discovers the
/// engine's value-to-partition map from the catalog.
#[test]
fn list_partitions_map_each_initial_cluster() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();

	assert_eq!(
		db.router().lifecycle(),
		&[
			"created Vertex LIST(cluster)".to_string(),
			"created Msg HASH(server) x3".to_string()
		]
	);

	let seeded = seed_demo_graph(&mut db).unwrap();
	assert_eq!(seeded.len(), 8);

	// One live partition per seeded cluster value (plus the empty unkeyed DDL partition).
	let map = db.router().cluster_map().clone();
	assert_eq!(map.len(), 8, "a partition per distinct cluster, {map:?}");
	for id in 0..8 {
		assert!(map.contains_key(&id), "cluster {id} has a partition");
		// The pruned cluster read returns exactly that community's vertices.
		let members: Vec<i64> = db.read_cluster(id).unwrap().iter().map(|v| v.id).collect();
		assert_eq!(members, vec![id]);
	}
	assert_eq!(db.count_vertex().unwrap(), 8);
}

/// The partition column is immutable in place: `SET cluster` is refused with a
/// delete-and-reinsert hint, while ordinary columns stay mutable.
#[test]
fn set_partition_key_is_refused() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();
	seed_demo_graph(&mut db).unwrap();

	let err = db
		.update("MATCH (x:Vertex {id: 0}) SET x.cluster = 1")
		.unwrap_err();
	assert!(
		format!("{err:#}").contains("partition"),
		"refusal should name partitioning, got {err:#}"
	);
	// Non-partition columns update in place.
	db.update("MATCH (x:Vertex {id: 0}) SET x.value = 99")
		.unwrap();
	assert_eq!(db.read_vertex(0).unwrap().unwrap().value, 99);
	assert_eq!(db.read_vertex(0).unwrap().unwrap().cluster, 0);
}

/// Delete + insert migration: moving a vertex to another cluster relocates its row into that
/// community's partition, preserves every other field, and rewires its incident edges onto
/// the concrete partition pairs.
#[test]
fn move_vertex_migrates_row_and_rewires_edges() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();
	// Two disconnected triangles.
	example_ladybug_graph::graph::seed_edges(
		&mut db,
		&[(0, 1), (1, 2), (2, 0), (10, 11), (11, 12), (12, 10)],
	)
	.unwrap();

	let before = db.read_vertex(10).unwrap().unwrap();
	assert_eq!(before.cluster, 10);
	let moved = db.move_vertex_to_cluster(&before, 0).unwrap();
	assert_eq!(moved.cluster, 0);
	assert_eq!(moved.id, before.id);
	assert_eq!(moved.server, before.server);
	assert_eq!(moved.degree, before.degree);

	// The row now lives in cluster 0's partition and is gone from cluster 10's.
	let members: HashSet<i64> = db.read_cluster(0).unwrap().iter().map(|v| v.id).collect();
	assert_eq!(members, HashSet::from([0, 10]));
	let old: Vec<i64> = db.read_cluster(10).unwrap().iter().map(|v| v.id).collect();
	assert!(old.is_empty(), "cluster 10 should be empty, got {old:?}");

	// Adjacency is unchanged across the move (both directions, both endpoints).
	let mut n10 = db.neighbors(10).unwrap();
	n10.sort_unstable();
	assert_eq!(n10, vec![11, 12]);
	let mut n11 = db.neighbors(11).unwrap();
	n11.sort_unstable();
	assert_eq!(n11, vec![10, 12]);
	assert_eq!(db.count_vertex().unwrap(), 6);
}

/// WCC physically collapses communities: every label improvement migrates its row, so a
/// connected graph ends with all rows in a single partition while shard reads stay correct.
#[test]
fn wcc_collapses_partitions() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();
	db.start_run(6, 0).unwrap();
	seed_demo_graph(&mut db).unwrap();

	let outcome = Coordinator::new(db).run(Algorithm::Wcc).unwrap();
	assert_eq!(
		outcome
			.vertices
			.iter()
			.map(|v| v.cluster)
			.collect::<HashSet<_>>(),
		HashSet::from([0]),
		"all vertices should share one live cluster"
	);

	let mut db = server.open_db();
	let members: HashSet<i64> = db.read_cluster(0).unwrap().iter().map(|v| v.id).collect();
	assert_eq!(
		members,
		HashSet::from([0, 1, 2, 3, 4, 5, 6, 7]),
		"cluster 0's partition should hold the whole graph"
	);
	// Compute sharding is orthogonal: each server still reads exactly its vertices.
	for shard in 0..example_ladybug_graph::graph::NUM_SERVERS {
		for v in db.read_vertices(shard).unwrap() {
			assert_eq!(v.server, shard);
			assert_eq!(v.cluster, 0);
		}
	}
}

/// Edges span partitions: rel creation names concrete `<parent>_p<i>` pairs (the engine refuses
/// parent-bound rel writes on partitioned tables) while reads still cross partitions.
#[test]
fn edges_span_partitions() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();
	seed_demo_graph(&mut db).unwrap();

	// Vertex 2 (cluster 2) neighbors 0, 1, 3, 4, which live in other LIST partitions.
	let mut neighbors = db.neighbors(2).unwrap();
	neighbors.sort_unstable();
	assert_eq!(neighbors, vec![0, 1, 3, 4]);
	// Tail vertex 7 (cluster 7) reaches back across its partition boundary.
	assert_eq!(db.neighbors(7).unwrap(), vec![6]);
}

/// The `Msg` table is partitioned by target shard too: routed writes land in the owning
/// partition and the shard's next-round read finds them there.
#[test]
fn msg_writes_route_to_shard_partitions() {
	let server = TestServer::new();
	let mut db = server.open_db();
	db.create_schema().unwrap();

	// to_id 4 -> server 1, to_id 5 -> server 2.
	db.write_msg_round(4, 1, 10, 0).unwrap();
	db.write_msg_round(5, 1, 20, 0).unwrap();

	let msg_p1 = db.msg_partition(1).unwrap();
	let msg_p2 = db.msg_partition(2).unwrap();
	assert_ne!(msg_p1, msg_p2, "shards 1 and 2 own different partitions");
	let in_p1 = db
		.query(&format!("MATCH (m:{msg_p1}) RETURN m.to_id"))
		.unwrap();
	let in_p2 = db
		.query(&format!("MATCH (m:{msg_p2}) RETURN m.to_id"))
		.unwrap();
	assert_eq!(in_p1.len() + in_p2.len(), 2);
	assert_eq!(db.read_msgs_round(1, 0).unwrap(), vec![(4, 1, 10)]);
	assert_eq!(db.read_msgs_round(2, 0).unwrap(), vec![(5, 1, 20)]);
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
