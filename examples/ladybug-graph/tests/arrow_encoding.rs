//! Regression tests for the Arrow IPC result encoding in [`ladybug_server`].
//!
//! Result sets cross the wire as Arrow, which means every column has to commit to a single
//! `DataType` before any value is written. Getting that inference wrong does not raise an error:
//! the values still arrive, just in the wrong Rust type, and the typed readers in `graph` use
//! `filter_map` over `Value::Int64(..)` patterns — so a mistyped column is silently *dropped*
//! rather than rejected. A dropped `Msg` row is a wrong k-core with no diagnostic, so the
//! inference rules are worth pinning down.

use example_ladybug_graph::graph::GraphDb;
use example_ladybug_graph::ladybug_server::{LadybugServer, ServerHandle};
use lbug::Value;
use tempfile::TempDir;

/// A fresh server-owned store plus a remote client for it.
fn store() -> (TempDir, ServerHandle, GraphDb) {
	let dir = TempDir::new().unwrap();
	let server = LadybugServer::open(dir.path().join("graph.lbdb")).unwrap();
	let handle = ServerHandle::start(server).unwrap();
	let db = GraphDb::open(&handle.url).unwrap();
	(dir, handle, db)
}

/// A NULL in the first row must not type the whole column as text.
#[test]
fn null_first_row_does_not_coerce_the_column_to_text() {
	let (_dir, _handle, mut db) = store();
	db.update("CREATE NODE TABLE T(id INT64, n INT64, PRIMARY KEY(id))")
		.unwrap();
	// Row 1 leaves `n` NULL; row 2 sets it to an INT64.
	db.update("CREATE (:T {id: 1})").unwrap();
	db.update("CREATE (:T {id: 2, n: 42})").unwrap();

	let rows = db
		.query("MATCH (t:T) RETURN t.id, t.n ORDER BY t.id")
		.unwrap();

	assert_eq!(rows.len(), 2);
	assert_eq!(rows[0][1], None, "a NULL cell arrives as None");
	assert_eq!(
		rows[1][1],
		Some(Value::Int64(42)),
		"a value after a NULL keeps its type; encoding it as text would make the typed \
		 readers in `graph` drop the row"
	);
}

/// The same column, with the NULL last, has always worked — pin it so a future change to the
/// inference cannot fix one order by breaking the other.
#[test]
fn null_last_row_keeps_the_column_typed() {
	let (_dir, _handle, mut db) = store();
	db.update("CREATE NODE TABLE T(id INT64, n INT64, PRIMARY KEY(id))")
		.unwrap();
	db.update("CREATE (:T {id: 1, n: 7})").unwrap();
	db.update("CREATE (:T {id: 2})").unwrap();

	let rows = db
		.query("MATCH (t:T) RETURN t.id, t.n ORDER BY t.id")
		.unwrap();

	assert_eq!(rows[0][1], Some(Value::Int64(7)));
	assert_eq!(rows[1][1], None);
}

/// An all-NULL column has no value to infer from and falls back to text, so every cell is still
/// reported as absent rather than as an empty string.
#[test]
fn all_null_column_reports_every_cell_as_none() {
	let (_dir, _handle, mut db) = store();
	db.update("CREATE NODE TABLE T(id INT64, n INT64, PRIMARY KEY(id))")
		.unwrap();
	db.update("CREATE (:T {id: 1})").unwrap();
	db.update("CREATE (:T {id: 2})").unwrap();

	let rows = db
		.query("MATCH (t:T) RETURN t.id, t.n ORDER BY t.id")
		.unwrap();

	assert_eq!(rows.len(), 2);
	assert!(rows.iter().all(|r| r[1].is_none()));
}

/// The scalar types the graph layer actually binds must survive the Arrow round trip unchanged.
#[test]
fn scalars_round_trip_through_arrow() {
	let (_dir, _handle, mut db) = store();
	db.update("CREATE NODE TABLE T(id INT64, s STRING, b BOOLEAN, d DOUBLE, PRIMARY KEY(id))")
		.unwrap();
	db.update("CREATE (:T {id: 1, s: 'hi', b: true, d: 1.5})")
		.unwrap();

	let rows = db.query("MATCH (t:T) RETURN t.id, t.s, t.b, t.d").unwrap();

	assert_eq!(
		rows[0],
		vec![
			Some(Value::Int64(1)),
			Some(Value::String("hi".to_owned())),
			Some(Value::Bool(true)),
			Some(Value::Double(1.5)),
		]
	);
}

/// A query matching nothing still carries its column names, so it encodes as an empty batch
/// rather than failing.
#[test]
fn empty_result_set_encodes_cleanly() {
	let (_dir, _handle, mut db) = store();
	db.update("CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))")
		.unwrap();

	assert_eq!(db.query("MATCH (t:T) RETURN t.id").unwrap(), Vec::<Vec<_>>::new());
}
