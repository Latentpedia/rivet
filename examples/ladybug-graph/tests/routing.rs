//! Remote partitions through the real engine hooks.
//!
//! This target runs in its own process (Cargo builds one binary per `tests/` file), which
//! matters because partition-routing hooks are process-global: nothing else here installs
//! a guard, so this test owns the installation for its lifetime. It claims every
//! partition of one dedicated `Remote` table — a HASH parent, since rel coverage and the
//! LIST dynamic-creation path assume local storage — and drives point writes, bulk
//! `COPY` writes, parent-union reads, and drop lifecycle through the engine, all over
//! the same ADBC bridge production clients use.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use example_ladybug_graph::graph::GraphDb;
use example_ladybug_graph::ladybug_server::{LadybugServer, ServerHandle};
use lbug::{Callbacks, LogicalType, RoutingGuard, Value as LbugValue};
use tempfile::TempDir;

/// A real LadybugDB server process (in-thread) with a unique on-disk store, mirroring the
/// `kcore` harness. The engine is embedded here, so hooks installed in this process fire
/// for the served store.
struct TestServer {
	// Declared handle-first so drops join the server thread before the temp dir is
	// removed from under the still-open store.
	handle: ServerHandle,
	_dir: TempDir,
}

impl TestServer {
	fn new() -> TestServer {
		let dir = TempDir::new().unwrap();
		let server = LadybugServer::open(dir.path().join("graph.lbdb")).unwrap();
		let handle = ServerHandle::start(server).unwrap();
		TestServer { handle, _dir: dir }
	}

	fn open_db(&self) -> GraphDb {
		GraphDb::open(&self.handle.url).unwrap()
	}
}

/// Resolve a table's parent ID from the catalog (the `id` column sits next to `name`).
fn parent_table_id(db: &mut GraphDb, name: &str) -> u64 {
	let rows = db.query("CALL show_tables() RETURN *").unwrap();
	for row in &rows {
		let is_name =
			matches!(row.get(1).cloned().flatten(), Some(LbugValue::String(n)) if n == name);
		if !is_name {
			continue;
		}
		return match row.first().cloned().flatten() {
			Some(LbugValue::Int64(id)) => id as u64,
			other => panic!("expected Int64 table id, got {other:?}"),
		};
	}
	panic!("table {name} not in catalog");
}

/// Extract `(id, v)` pairs from `[id, v]` result rows.
fn pairs(rows: &[Vec<Option<LbugValue>>]) -> Vec<(i64, i64)> {
	rows.iter()
		.map(|r| {
			let cell = |i: usize| match r.get(i).cloned().flatten() {
				Some(LbugValue::Int64(v)) => v,
				other => panic!("expected Int64 cell, got {other:?}"),
			};
			(cell(0), cell(1))
		})
		.collect()
}

/// Claimed-parent ID shared with the `locate` closure (unknown until the table exists).
type ClaimedParent = Arc<Mutex<Option<u64>>>;
/// Observed `(parent, schema-ordered cells)` rows per routed write.
type ObservedRows = Arc<Mutex<Vec<(u64, Vec<LbugValue>)>>>;
/// Observed `(parent, partition index)` lifecycle events.
type LifecycleEvents = Arc<Mutex<Vec<(u64, u64)>>>;

#[test]
fn remote_partitions_serve_end_to_end() {
	// Declared first so the guard drops last, after every Database is gone.
	let claimed: ClaimedParent = Arc::new(Mutex::new(None));
	let observed: ObservedRows = Arc::new(Mutex::new(Vec::new()));
	let created: LifecycleEvents = Arc::new(Mutex::new(Vec::new()));
	let dropped: LifecycleEvents = Arc::new(Mutex::new(Vec::new()));
	let claimed_cb = claimed.clone();
	let observed_cb = observed.clone();
	let created_cb = created.clone();
	let dropped_cb = dropped.clone();
	let guard = RoutingGuard::install(Callbacks {
		// Claim only the Remote parent: the ID is unknown until the table exists, so the
		// closure reads it from shared state (still None during DDL, when nothing routes).
		locate: Some(Box::new(move |r| {
			(*claimed_cb.lock().unwrap() == Some(r.parent_table_id)).then_some(0xC0FFEE)
		})),
		on_partition_create: Some(Box::new(move |r| {
			created_cb
				.lock()
				.unwrap()
				.push((r.parent_table_id, r.partition_index));
		})),
		on_partition_drop: Some(Box::new(move |r| {
			dropped_cb
				.lock()
				.unwrap()
				.push((r.parent_table_id, r.partition_index));
		})),
		insert_row: Some(Box::new(move |r, row| {
			observed_cb.lock().unwrap().push((r.parent_table_id, row));
		})),
	})
	.unwrap();
	assert!(guard.is_installed());

	let server = TestServer::new();
	let mut db = server.open_db();
	db.update(
		"CREATE NODE TABLE Remote(id INT64, v INT64, PRIMARY KEY(id)) PARTITION BY HASH(v) PARTITIONS 3",
	)
	.unwrap();
	let pid = parent_table_id(&mut db, "Remote");
	*claimed.lock().unwrap() = Some(pid);
	guard
		.register_parent_schema(
			pid,
			vec![
				("id".to_string(), LogicalType::Int64),
				("v".to_string(), LogicalType::Int64),
			],
		)
		.unwrap();

	// Point writes across hash partitions, then bulk writes through COPY (insertChunk fans
	// out to the same row callback in the shim).
	for (id, v) in [(1, 10), (2, 20), (3, 30)] {
		db.update(&format!("CREATE (:Remote {{id: {id}, v: {v}}})"))
			.unwrap();
	}
	let csv_dir = TempDir::new().unwrap();
	std::fs::write(csv_dir.path().join("bulk.csv"), "4,40\n5,50\n").unwrap();
	db.update(&format!(
		"COPY Remote FROM '{}'",
		csv_dir.path().join("bulk.csv").to_string_lossy()
	))
	.unwrap();

	// Parent-union reads come back through the bundled scan, in order.
	let rows = db
		.query("MATCH (r:Remote) RETURN r.id, r.v ORDER BY r.id")
		.unwrap();
	assert_eq!(
		pairs(&rows),
		vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
	);

	// NOTE: a direct `Remote_p<i>` read is rejected by the engine (claimed partitions own
	// no local storage and only parent-union scans route through `bindScan`), so the
	// parent scan above is the read path for remote content.

	// Every routed row was observed with its owning parent and schema-ordered cells.
	let seen: HashSet<i64> = observed
		.lock()
		.unwrap()
		.iter()
		.flat_map(|(parent, row)| {
			assert_eq!(*parent, pid);
			match row.first().cloned() {
				Some(LbugValue::Int64(id)) => Some(id),
				other => panic!("expected Int64 id, got {other:?}"),
			}
		})
		.collect();
	assert_eq!(seen, HashSet::from([1, 2, 3, 4, 5]));

	// Lifecycle: three HASH partitions created, then dropped with the parent.
	let created_here: Vec<u64> = created
		.lock()
		.unwrap()
		.iter()
		.filter(|(parent, _)| *parent == pid)
		.map(|(_, index)| *index)
		.collect();
	assert_eq!(created_here.len(), 3);
	db.update("DROP TABLE Remote").unwrap();
	let dropped_here: Vec<u64> = dropped
		.lock()
		.unwrap()
		.iter()
		.filter(|(parent, _)| *parent == pid)
		.map(|(_, index)| *index)
		.collect();
	assert_eq!(dropped_here.len(), 3);

	// Teardown order: clients, then the server (joining its engine thread drops every
	// Database), then the guard resets the hooks.
	drop(db);
	drop(server);
	drop(csv_dir);
	guard.uninstall();
}
