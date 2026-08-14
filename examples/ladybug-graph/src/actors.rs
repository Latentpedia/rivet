//! Rivet actors that host the distributed algorithms across N local server processes.
//!
//! Each server process runs a [`VertexWorker`] actor for its shard. The worker owns no graph data
//! of its own; when told to run a superstep it opens an ADBC connection to the shared LadybugDB
//! store and processes exactly its shard, reading/writing messages through the graph. A
//! [`Coordinator`] actor (on any server) drives the superstep barrier by invoking each worker's
//! [`RunSuperstep`] action over Rivet and counting the resulting messages.
//!
//! This is the `rivet` half of the platform: Rivet owns actor lifecycle and the cross-process
//! control plane (the `Coordinator` invoking `VertexWorker`s across servers), while `adbc_core`
//! owns the data plane (the vertices, edges, messages, and results persisted in LadybugDB). The
//! store itself lives in a dedicated `ladybug-server` process; every worker and coordinator is a
//! **remote client** that connects to it via the columnar protocol, so N separate processes and
//! machines can share one store without touching a shared file.
//!
//! Configuration comes from the environment so the same binary can be a worker or a coordinator
//! on any server:
//!
//! - `LADYBUG_DB` — LadybugDB server URL (for example `http://127.0.0.1:8123`).
//! - `SERVER_ID` — this process's shard index (used by a worker).
//! - `NUM_SERVERS` — total shard count (used by a coordinator).
//! - `GRAPH_RUN_ID`, `GRAPH_K`, `GRAPH_ALGO` — coordinator run parameters.

use std::{
	collections::HashMap,
	future::Future,
	pin::Pin,
	sync::{Arc, LazyLock, Mutex},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use rivetkit::{
	Action, Actor, Ctx, Handles, KeepAwakeRegion, Registry, action, client::GetOrCreateOptions,
	typed_client::TypedClientExt,
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::algorithm::Algorithm;

/// Names used when registering and addressing the actors.
pub const WORKER_ACTOR: &str = "vertexWorker";
pub const COORDINATOR_ACTOR: &str = "graphCoordinator";

/// Decodes an algorithm selector crossing the action boundary: 1 = k-core, 2 = WCC.
fn algo(algo_idx: i64, k: i64) -> Result<Algorithm> {
	match algo_idx {
		1 => Ok(Algorithm::KCore { k }),
		2 => Ok(Algorithm::Wcc),
		_ => bail!("unsupported algorithm index {algo_idx} (expected 1=k-core, 2=wcc)"),
	}
}

fn db_url_from_env() -> Result<String> {
	std::env::var("LADYBUG_DB").context(
		"LADYBUG_DB must be set to the LadybugDB server URL (for example http://127.0.0.1:8123)",
	)
}

/// A process opens one remote client connection and shares it across every worker/coordinator
/// actor in that process (the client is a thin pooled HTTP handle, so sharing is cheap). The
/// server process is the single writer; clients never own the file.
static SHARED_DB: Mutex<Option<Arc<Mutex<crate::graph::GraphDb>>>> = Mutex::new(None);

/// Returns the process-wide shared graph store, opening it from `LADYBUG_DB` on first use.
fn shared_db() -> Result<Arc<Mutex<crate::graph::GraphDb>>> {
	let mut guard = SHARED_DB.lock().unwrap();
	if let Some(db) = &*guard {
		return Ok(db.clone());
	}
	let url = db_url_from_env()?;
	let db = Arc::new(Mutex::new(crate::graph::GraphDb::open(&url)?));
	*guard = Some(db.clone());
	Ok(db)
}

/// Keeps the worker actors resident across supersteps for the lifetime of the process. Without it,
/// an idle worker hibernates after each superstep and the engine pays a full actor cold-start to
/// resume it for the next round, which is far slower than the algorithm's message passing itself.
/// Workers stay warm (one keep-awake region per shard) so the barrier loop runs hot.
static KEEP_AWAKE: LazyLock<Mutex<HashMap<i64, KeepAwakeRegion>>> =
	LazyLock::new(|| Mutex::new(HashMap::new()));

// ---------------------------------------------------------------------------
// VertexWorker
// ---------------------------------------------------------------------------

type BoxFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

/// Asks a worker to run one superstep for its shard at `round`, returning how many messages it
/// produced for the next round.
#[derive(Debug, Serialize, Deserialize)]
pub struct RunSuperstep {
	pub round: i64,
	pub k: i64,
	pub algo_idx: i64,
}

impl Action for RunSuperstep {
	type Output = i64;
	const NAME: &'static str = "runSuperstep";
}

/// The worker actor: handles the vertices of one shard, over ADBC, when asked.
#[derive(Default, Serialize, Deserialize)]
pub struct WorkerState;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct WorkerConnParams {
	pub server: Option<i64>,
}

pub struct VertexWorker;

#[async_trait]
impl Actor for VertexWorker {
	type State = WorkerState;
	type Input = ();
	type Actions = (RunSuperstep,);
	type Events = ();
	type Queue = ();
	type ConnParams = WorkerConnParams;
	type ConnState = WorkerConnParams;
	type Action = action::Raw;

	async fn create_state(_ctx: &Ctx<Self>, _input: Self::Input) -> Result<Self::State> {
		Ok(WorkerState)
	}

	async fn create_conn_state(
		self: Arc<Self>,
		_ctx: Ctx<Self>,
		params: Self::ConnParams,
	) -> Result<Self::ConnState> {
		Ok(params)
	}

	async fn create(_ctx: &Ctx<Self>) -> Result<Self> {
		Ok(VertexWorker)
	}
}

impl Handles<RunSuperstep> for VertexWorker {
	type Future = BoxFuture<i64>;

	fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: RunSuperstep) -> Self::Future {
		Box::pin(async move {
			let server = ctx
				.conn()
				.and_then(|c| c.state().ok())
				.and_then(|s| s.server)
				.unwrap_or_else(|| {
					std::env::var("SERVER_ID")
						.ok()
						.and_then(|s| s.parse().ok())
						.unwrap_or(0)
				});
			let algo = algo(action.algo_idx, action.k)?;
			// Hold this worker awake for the rest of the run so it does not hibernate between
			// superstep actions (a cold-start resume between every round would dominate the cost).
			KEEP_AWAKE
				.lock()
				.unwrap()
				.entry(server)
				.or_insert_with(|| ctx.keep_awake_region());
			let db = shared_db()?;
			let mut db = db.lock().unwrap();
			let produced = crate::algorithm::run_superstep(&mut db, server, action.round, algo)?;
			info!(
				server,
				round = action.round,
				produced,
				"vertex worker superstep complete"
			);
			Ok(produced)
		})
	}
}

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------

/// Drives an algorithm to a fixed point by calling every worker's superstep over Rivet.
#[derive(Debug, Serialize, Deserialize)]
pub struct RunAlgorithm {
	pub run_id: i64,
	pub k: i64,
	pub algo_idx: i64,
}

impl Action for RunAlgorithm {
	type Output = CoordinatorResult;
	const NAME: &'static str = "runAlgorithm";
}

/// Summary returned to whoever started the run (already persisted back into the graph).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CoordinatorResult {
	pub rounds: i64,
	pub vertices: i64,
	pub active: i64,
}

#[derive(Default, Serialize, Deserialize)]
pub struct CoordinatorState;

pub struct Coordinator;

#[async_trait]
impl Actor for Coordinator {
	type State = CoordinatorState;
	type Input = ();
	type Actions = (RunAlgorithm,);
	type Events = ();
	type Queue = ();
	type ConnParams = ();
	type ConnState = ();
	type Action = action::Raw;

	async fn create_state(_ctx: &Ctx<Self>, _input: Self::Input) -> Result<Self::State> {
		Ok(CoordinatorState)
	}

	async fn create(_ctx: &Ctx<Self>) -> Result<Self> {
		Ok(Coordinator)
	}
}

impl Handles<RunAlgorithm> for Coordinator {
	type Future = BoxFuture<CoordinatorResult>;

	fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: RunAlgorithm) -> Self::Future {
		Box::pin(async move {
			let _ensure_store = shared_db()?;
			let num_servers: i64 = std::env::var("NUM_SERVERS")
				.ok()
				.and_then(|s| s.parse().ok())
				.unwrap_or(3);
			let algo = algo(action.algo_idx, action.k)?;

			// The control plane: reach every worker actor across the servers via Rivet.
			let client = ctx.client()?;
			info!(
				run_id = action.run_id,
				num_servers,
				?algo,
				"starting distributed algorithm run"
			);
			let mut produced = i64::MAX;
			let mut round = 0i64;
			let mut rounds = 0i64;
			while produced > 0 {
				rounds += 1;
				if rounds > 10_000 {
					bail!("algorithm did not converge");
				}
				produced = 0;
				for s in 0..num_servers {
					// The shard rides the connection params so the worker's `ctx.conn()` knows which
					// shard it owns (the actor key tags it, but the conn state carries the shard id).
					let worker = client
						.get_or_create_typed::<VertexWorker>(
							WORKER_ACTOR,
							[s.to_string()],
							GetOrCreateOptions {
								params: Some(serde_json::json!({ "server": s })),
								..Default::default()
							},
						)
						.context("get vertex worker")?;
					produced += worker
						.call(RunSuperstep {
							round,
							k: action.k,
							algo_idx: action.algo_idx,
						})
						.await
						.context("run worker superstep")?;
				}
				info!(round, produced, "coordinator superstep barrier complete");
				round += 1;
			}

			// The data plane: finalize/persist via the shared store (ADBC) and report.
			let db = shared_db()?;
			let mut db = db.lock().unwrap();
			if let Algorithm::KCore { k } = algo {
				for s in 0..num_servers {
					for v in db.read_vertices(s)? {
						if v.active {
							db.persist_vertex(&crate::graph::Vertex { core: k, ..v })?;
						}
					}
				}
			}
			db.mark_done(action.run_id, rounds)?;
			db.clear_msgs()?;

			let mut total = 0i64;
			let mut active = 0i64;
			for s in 0..num_servers {
				for v in db.read_vertices(s)? {
					total += 1;
					if v.active {
						active += 1;
					}
				}
			}
			info!(
				rounds,
				vertices = total,
				active,
				"algorithm result persisted"
			);
			Ok(CoordinatorResult {
				rounds,
				vertices: total,
				active,
			})
		})
	}
}

/// Builds a registry containing the worker and coordinator actors for one server process.
pub fn registry() -> Registry {
	let mut registry = Registry::new();
	registry.register_actor::<VertexWorker>(WORKER_ACTOR);
	registry.register_actor::<Coordinator>(COORDINATOR_ACTOR);
	registry
}
