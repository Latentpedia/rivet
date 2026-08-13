//! A Rivet server process for the ladybug-graph platform.
//!
//! Each of the `NUM_SERVERS` local servers runs this binary. They all point at the same shared
//! LadybugDB file (`LADYBUG_DB`) and the same Rivet engine, and each hosts a [`VertexWorker`]
//! actor plus the [`Coordinator`] actor, so Rivet provides the cross-process control plane while
//! the graph message passing runs over `adbc_core`.
//!
//! Subcommands:
//!
//! - `serve` (default) — host the worker + coordinator actors.
//! - `seed <db> <k>` — create the schema and seed the demo graph once (before starting servers).

use anyhow::{Context, Result};

/// Hosts the platform actors on this server process until interrupted.
async fn serve() -> Result<()> {
	example_ladybug_graph::actors::registry().start().await
}

fn seed(db_path: &str, k: i64) -> Result<()> {
	let mut db = example_ladybug_graph::graph::GraphDb::open(db_path)?;
	db.create_schema()?;
	db.start_run(1, k)?;
	example_ladybug_graph::graph::seed_demo_graph(&mut db)?;
	println!("seeded demo graph at {db_path}");
	Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
	let mut args = std::env::args().skip(1);
	match args.next().as_deref() {
		Some("seed") => {
			let db = args.next().context("seed requires <db>")?;
			let k = args.next().and_then(|s| s.parse().ok()).unwrap_or(2);
			seed(&db, k)
		}
		_ => serve().await.context("serve the platform actors"),
	}
}
