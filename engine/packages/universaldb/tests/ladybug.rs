use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Context;
use lbug::Value;
use universaldb::driver::ladybug::{LadybugNodeSpec, LadybugConfig, LadybugDatabaseDriver};

static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Returns a fresh on-disk database path that is guaranteed not to collide with a previous run.
/// The database files live at `{dir}/graph`; the parent directory is created and never removed so
/// the reopen-across-driver persistence test observes the same data.
fn tmp_db_path(label: &str) -> std::path::PathBuf {
	let n = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
	let dir = std::env::temp_dir().join(format!(
		"universaldb-ladybug-{label}-{n}-{}",
		std::process::id()
	));
	let _ = std::fs::create_dir_all(&dir);
	dir.join("graph")
}

/// Creates the node/rel schema and seeds a small friend graph.
async fn seed(driver: &LadybugDatabaseDriver) -> anyhow::Result<()> {
	let txn = driver.graph_txn();

	// Schema DDL runs as a single buffered transaction so the whole schema lands atomically.
	txn.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))");
	txn.execute("CREATE REL TABLE Follows(FROM Person TO Person, since INT64)");
	txn.commit().await?;

	// Typed helpers buffer parameterized node creation.
	let txn = driver.graph_txn();
	txn.create_node(&LadybugNodeSpec {
		label: "Person".into(),
		props: vec![
			("id".into(), Value::Int64(1)),
			("name".into(), Value::String("Alice".into())),
			("age".into(), Value::Int64(30)),
		],
	})?;
	txn.create_node(&LadybugNodeSpec {
		label: "Person".into(),
		props: vec![
			("id".into(), Value::Int64(2)),
			("name".into(), Value::String("Bob".into())),
			("age".into(), Value::Int64(25)),
		],
	})?;
	txn.commit().await?;

	// Raw traversal string for the relationship.
	let txn = driver.graph_txn();
	txn.execute(
		"MATCH (a:Person {id: 1}), (b:Person {id: 2}) \
		 CREATE (a)-[:Follows {since: 2020}]->(b)",
	);
	txn.commit().await?;

	Ok(())
}

#[tokio::test]
async fn ladybug_persistence_and_retrieval() -> anyhow::Result<()> {
	let db_path = tmp_db_path("persistence");
	let driver = LadybugDatabaseDriver::new(db_path.clone(), LadybugConfig::default()).await?;
	seed(&driver).await?;

	// Read back within the same driver.
	let txn = driver.graph_txn();
	let rows = txn
		.query("MATCH (p:Person) RETURN p.id, p.name, p.age ORDER BY p.id")
		.await?;
	assert_eq!(rows.len(), 2);
	assert_eq!(rows[0].get("name"), Some(&Value::String("Alice".into())));

	// Drop the driver, reopen the same path, and read the graph back from disk.
	drop(driver);

	let driver =
		LadybugDatabaseDriver::new(db_path.clone(), LadybugConfig::default()).await?;
	let txn = driver.graph_txn();
	let rows = txn
		.query("MATCH (a:Person)-[r:Follows]->(b:Person) RETURN a.name, r.since, b.name")
		.await?;
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get("since"), Some(&Value::Int64(2020)));
	assert_eq!(rows[0].get("b.name"), Some(&Value::String("Bob".into())));

	Ok(())
}

#[tokio::test]
async fn ladybug_transaction_commit_and_rollback() -> anyhow::Result<()> {
	let db_path = tmp_db_path("txn");
	let driver = LadybugDatabaseDriver::new(db_path.clone(), LadybugConfig::default()).await?;
	seed(&driver).await?;

	// A transaction whose buffered writes are committed is visible.
	let txn = driver.graph_txn();
	txn.execute("CREATE (:Person {id: 4, name: 'Dave', age: 40})");
	txn.commit().await?;
	let txn = driver.graph_txn();
	let rows = txn
		.query("MATCH (p:Person {id: 4}) RETURN p.name")
		.await?;
	assert_eq!(rows.len(), 1);

	// A transaction whose buffered writes are aborted is not visible.
	let txn = driver.graph_txn();
	txn.execute("CREATE (:Person {id: 5, name: 'Eve', age: 50})");
	txn.abort();
	let txn = driver.graph_txn();
	let rows = txn
		.query("MATCH (p:Person {id: 5}) RETURN p.name")
		.await?;
	assert_eq!(rows.len(), 0);

	Ok(())
}

#[tokio::test]
async fn ladybug_checkpoint() -> anyhow::Result<()> {
	let db_path = tmp_db_path("checkpoint");
	let driver = LadybugDatabaseDriver::new(db_path.clone(), LadybugConfig::default()).await?;

	let txn = driver.graph_txn();
	txn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id))");
	txn.commit().await?;
	txn.execute("CREATE (:Person {id: 1})");
	txn.commit().await?;

	// Flush the write-ahead log into the data files. This must not error on a live database.
	driver.force_checkpoint().context("force_checkpoint failed")?;

	// The checkpointed data is still queryable.
	let txn = driver.graph_txn();
	let rows = txn.query("MATCH (p:Person) RETURN p.id").await?;
	assert_eq!(rows.len(), 1);

	Ok(())
}
