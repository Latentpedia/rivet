//! Standalone demo of the `ladybug-graph` platform.
//!
//! Builds a small graph, seeds it into a LadybugDB store served by an in-process `ladybug-server`
//! on an ephemeral port, then runs the requested message-passing algorithm across the live
//! communities: each cluster's slice is processed in turn against the shared store, exchanging
//! messages only through the remote ADBC interface (see [`adbc`]). This runs without the Rivet
//! engine; the [`actors`] module runs the exact same per-cluster compute as Rivet actors for the
//! multi-process deployment driven by `scripts/run-ladybug-demo.sh`.
//!
//! Usage:
//!
//! ```text
//! cargo run -p example-ladybug-graph --release -- kcore 2   # default
//! cargo run -p example-ladybug-graph --release -- wcc
//! ```

use anyhow::{Context, Result, bail};

use example_ladybug_graph::algorithm::{Algorithm, Coordinator};
use example_ladybug_graph::graph::{GraphDb, seed_demo_graph};
use example_ladybug_graph::ladybug_server::{LadybugServer, ServerHandle, install_local_hooks};

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

	// Real engine hooks, installed before the store opens and held to process exit.
	let _hooks = install_local_hooks()?;
	// One LadybugDB server owns the file; the cluster owners are remote clients of it.
	let server = LadybugServer::open(&db_path)?;
	let handle = ServerHandle::start(server)?;
	let mut db = GraphDb::open(&handle.url)?;
	db.create_schema()?;
	db.start_run(
		1,
		match algo {
			Algorithm::KCore { k } => k,
			Algorithm::Wcc => 0,
		},
	)?;
	seed_demo_graph(&mut db)?;
	println!(
		"== ladybug-graph: {} over {}",
		describe(&algo),
		db_path.display()
	);
	println!("seeded demo graph (server-owned store at {})", handle.url);

	// The cluster barrier runs sequentially here, exactly as the Rivet coordinator drives
	// it: one owner per live community per round, all state in the partitioned tables.
	let (outcome, stats) = Coordinator::new(db).run_with_stats(algo)?;

	println!(
		"converged after {} supersteps ({} vertices; {} local / {} remote messages)\n",
		outcome.rounds,
		outcome.vertices.len(),
		stats.local_msgs,
		stats.remote_msgs,
	);
	for v in &outcome.vertices {
		let tag = if v.active { "IN" } else { "OUT" };
		match algo {
			Algorithm::KCore { k: _ } => println!(
				"  vertex {:>2}  cluster {:>1}  degree {:>1}  core {}  [{tag}]",
				v.id, v.cluster, v.degree, v.core
			),
			Algorithm::Wcc => println!(
				"  vertex {:>2}  cluster {:>2}  component {:>2}",
				v.id, v.cluster, v.value
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
