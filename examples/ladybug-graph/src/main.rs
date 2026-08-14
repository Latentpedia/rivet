//! Standalone demo of the `ladybug-graph` platform.
//!
//! Builds a small graph, seeds it into a LadybugDB store served by an in-process `ladybug-server`
//! on an ephemeral port, then runs the requested message-passing algorithm across `NUM_SERVERS`
//! concurrent worker "servers" that connect to that store and exchange messages only through the
//! remote ADBC interface (see [`adbc`]). This runs without the Rivet engine; the [`actors`] module
//! wraps the exact same compute as Rivet actors for the multi-process deployment driven by
//! `scripts/run-ladybug-demo.sh`.
//!
//! Usage:
//!
//! ```text
//! cargo run -p example-ladybug-graph --release -- kcore 2   # default
//! cargo run -p example-ladybug-graph --release -- wcc
//! ```

use anyhow::{Context, Result, bail};
use std::sync::{Arc, Mutex};

use example_ladybug_graph::algorithm::Algorithm;
use example_ladybug_graph::graph::{GraphDb, NUM_SERVERS, seed_demo_graph};
use example_ladybug_graph::ladybug_server::{LadybugServer, ServerHandle};

const MAX_SUPERSTEPS: i64 = 10_000;

/// Runs the superstep loop where every shard is a separate thread. All threads share ONE remote
/// `GraphDb` client; the per-round `round` column keeps the barrier clean across shards, and the
/// server serializes writers.
fn run_parallel(
	shared: Arc<Mutex<GraphDb>>,
	algo: Algorithm,
) -> Result<example_ladybug_graph::algorithm::RunOutcome> {
	let mut produced = i64::MAX;
	let mut round = 0i64;
	let mut rounds = 0i64;

	while produced > 0 {
		rounds += 1;
		if rounds > MAX_SUPERSTEPS {
			bail!("algorithm did not converge within {MAX_SUPERSTEPS} supersteps");
		}
		let results: Vec<i64> = std::thread::scope(|scope| {
			let handles: Vec<_> = (0..NUM_SERVERS)
				.map(|shard| {
					let shared = shared.clone();
					scope.spawn(move || -> Result<i64> {
						let mut db = shared.lock().unwrap();
						example_ladybug_graph::algorithm::run_superstep(&mut db, shard, round, algo)
					})
				})
				.collect();
			handles
				.into_iter()
				.map(|h| {
					h.join()
						.unwrap_or_else(|_| panic!("worker thread panicked"))
				})
				.collect::<Result<Vec<i64>>>()
		})?;
		produced = results.iter().sum();
		round += 1;
	}

	// Finalize survivors and gather the outcome.
	let mut db = shared.lock().unwrap();
	if let Algorithm::KCore { k } = algo {
		for shard in 0..NUM_SERVERS {
			for v in db.read_vertices(shard)? {
				if v.active {
					db.persist_vertex(&example_ladybug_graph::graph::Vertex { core: k, ..v })?;
				}
			}
		}
	}
	db.clear_msgs()?;
	let mut vertices = Vec::new();
	for shard in 0..NUM_SERVERS {
		vertices.extend(db.read_vertices(shard)?);
	}
	vertices.sort_by_key(|v| v.id);
	Ok(example_ladybug_graph::algorithm::RunOutcome { rounds, vertices })
}

fn main() -> Result<()> {
	let args: Vec<String> = std::env::args().skip(1).collect();
	let algo = match args.first().map(|s| s.as_str()) {
		Some("wcc") | Some("WCC") => Algorithm::Wcc,
		Some("kcore") | Some("k-core") | None => {
			let k = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(2);
			Algorithm::KCore { k }
		}
		Some(other) => bail!("unknown algorithm `{other}` (use `kcore [k]` or `wcc`)"),
	};

	// Create a durable, unique temp dir under the OS temp root (no engine needed).
	let dir = std::env::temp_dir().join(format!("ladybug-demo-{}", std::process::id()));
	std::fs::create_dir_all(&dir).context("create demo dir")?;
	let db_path = dir.join("demo.lbdb");

	// One LadybugDB server owns the file; the shard workers are remote clients of it.
	let server = LadybugServer::open(&db_path)?;
	let handle = ServerHandle::start(server)?;
	let db = Arc::new(Mutex::new(GraphDb::open(&handle.url)?));
	{
		let mut guard = db.lock().unwrap();
		guard.create_schema()?;
		guard.start_run(
			1,
			match algo {
				Algorithm::KCore { k } => k,
				Algorithm::Wcc => 0,
			},
		)?;
		seed_demo_graph(&mut guard)?;
	}
	println!(
		"== ladybug-graph: {} over {}",
		describe(&algo),
		db_path.display()
	);
	println!(
		"seeded demo graph across {NUM_SERVERS} shard servers (server-owned store at {})",
		handle.url
	);

	let outcome = run_parallel(db, algo)?;

	println!(
		"converged after {} supersteps ({} vertices)\n",
		outcome.rounds,
		outcome.vertices.len()
	);
	for v in &outcome.vertices {
		let tag = if v.active { "IN" } else { "OUT" };
		match algo {
			Algorithm::KCore { k: _ } => println!(
				"  vertex {:>2}  server {:>1}  degree {:>1}  core {}  [{tag}]",
				v.id, v.server, v.degree, v.core
			),
			Algorithm::Wcc => println!(
				"  vertex {:>2}  server {:>1}  component {:>2}",
				v.id, v.server, v.value
			),
		}
	}
	Ok(())
}

fn describe(algo: &Algorithm) -> String {
	match algo {
		Algorithm::KCore { k } => format!("k-core  (k = {k})"),
		Algorithm::Wcc => "weakly connected components".into(),
	}
}
