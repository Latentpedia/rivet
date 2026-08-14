//! A Rivet server process for the ladybug-graph platform.
//!
//! Each of the `NUM_SERVERS` local servers runs this binary. They all point at the same shared
//! LadybugDB file (`LADYBUG_DB`) and the same Rivet engine, and each hosts a [`VertexWorker`]
//! actor plus the [`Coordinator`] actor, so Rivet provides the cross-process control plane while
//! the graph message passing runs over `adbc_core`.
//!
//! Subcommands:
//!
//! - `serve` (default) — host the worker + coordinator actors. Once the actors are reachable, a
//!   client task triggers `runAlgorithm` (disable with `GRAPH_AUTO_RUN=0`) so the computation runs
//!   and its progress is visible in the logs.
//! - `seed <db> <k>` — create the schema and seed the demo graph once (before starting servers).

use std::time::Duration;

use anyhow::{Context, Result};
use rivetkit::{TypedClientExt, client::ClientConfig};
use tracing::{error, info, warn};

use example_ladybug_graph::actors::{COORDINATOR_ACTOR, Coordinator, RunAlgorithm};

/// Sends tracing events (actor lifecycle, superstep progress, engine health) to stderr so the
/// demo is observable from the terminal. `RUST_LOG` controls the level (default `info`).
fn init_logging() {
	let filter = tracing_subscriber::EnvFilter::try_from_default_env()
		.unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
	let _ = tracing_subscriber::fmt()
		.with_env_filter(filter)
		.with_writer(std::io::stderr)
		.try_init();
}

/// Builds the client used to trigger the algorithm, mirroring the actor-runtime client config so
/// the trigger talks to the same engine, namespace, and pool as the hosted actors.
fn trigger_client() -> Result<rivetkit::client::Client> {
	let endpoint = std::env::var("RIVET_ENDPOINT")
		.unwrap_or_else(|_| "http://127.0.0.1:6420".to_owned());
	let token = std::env::var("RIVET_TOKEN").ok();
	let namespace =
		std::env::var("RIVET_NAMESPACE").unwrap_or_else(|_| "default".to_owned());
	let pool_name =
		std::env::var("RIVET_POOL_NAME").unwrap_or_else(|_| "rivetkit-rust".to_owned());
	Ok(rivetkit::client::Client::new(
		ClientConfig::new(endpoint)
			.token_opt(token)
			.namespace(namespace)
			.pool_name(pool_name)
			.disable_metadata_lookup(true),
	))
}

/// Waits for the engine to accept the coordinator actor, then runs the algorithm once and logs the
/// result. Retries until the first successful run so the trigger survives slow envoy startup.
async fn trigger_algorithm() {
	let run_id: i64 = std::env::var("GRAPH_RUN_ID").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
	let k: i64 = std::env::var("GRAPH_K").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
	let algo_idx: i64 = std::env::var("GRAPH_ALGO").ok().and_then(|v| v.parse().ok()).unwrap_or(1);

	for attempt in 0..120u32 {
		match try_run_algorithm(run_id, k, algo_idx).await {
			Ok(result) => {
				info!(
					rounds = result.rounds,
					vertices = result.vertices,
					active = result.active,
					"algorithm complete"
				);
				return;
			}
			Err(error) if attempt < 119 => {
				warn!(?error, attempt, "runAlgorithm not ready yet, retrying");
				tokio::time::sleep(Duration::from_millis(500)).await;
			}
			Err(error) => {
				error!(?error, "runAlgorithm failed after 60s of retries");
				return;
			}
		}
	}
}

async fn try_run_algorithm(run_id: i64, k: i64, algo_idx: i64) -> Result<example_ladybug_graph::actors::CoordinatorResult> {
	let client = trigger_client()?;
	let coordinator = client
		.get_or_create_typed::<Coordinator>(COORDINATOR_ACTOR, Vec::<String>::new(), Default::default())
		.context("get graph coordinator")?;
	info!(run_id, k, algo_idx, "triggering runAlgorithm");
	let result = coordinator
		.call(RunAlgorithm { run_id, k, algo_idx })
		.await
		.context("run distributed algorithm")?;
	Ok(result)
}

/// Hosts the platform actors on this server process until interrupted.
async fn serve() -> Result<()> {
	let auto_run = std::env::var("GRAPH_AUTO_RUN")
		.map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
		.unwrap_or(true);
	if auto_run {
		tokio::spawn(async move { trigger_algorithm().await });
	}
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
	init_logging();
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
