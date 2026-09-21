//! Rivet actors that own one graph community each.
//!
//! Model `B`: every [`VertexWorker`] actor owns a single `cluster` partition
//! as its slice's **exclusive writer**, but the rows themselves live in the
//! LadybugDB partitioned tables — never in Rivet storage. A worker's
//! persisted [`State`](rivetkit::Actor::State) is only its owned-cluster
//! identity; the coordinator's directory is rebuilt from the store on every
//! run. Actors are addressed by cluster id (`key = [cluster.to_string()]`)
//! and created from [`WorkerInput`].
//!
//! Rivet's role is the control plane on top of LadybugDB's distributed
//! hooks: two-level routing plus placement. LadybugDB routes *rows* into
//! partitions by the `cluster` key; Rivet routes *invocations* to the actor
//! named by that key, driven by a directory snapshot taken from the store
//! each run and updated from per-round migration reports. For load balance,
//! every superstep reports its cost (members processed, messages produced)
//! and the coordinator invokes the heaviest partitions first while logging
//! the LPT placement plan ([`placement`](crate::clusters::placement)) that a
//! multi-host deployment would feed to each host's `locate()` hook claims —
//! keeping compute next to the partitions it owns. Intra-community traffic
//! (the bulk of a distributed Leiden local-moving phase) stays inside one
//! `Msg` partition slice, so the hot path never crosses a partition
//! boundary.
//!
//! The barrier is sequential over clusters in cost-descending order. That
//! keeps one invariant simple: each vertex is processed exactly once per
//! round, by the owner named in the round-start snapshot, which only ever
//! writes its own slice (neighbor lookups and directory scans may read
//! across partitions). Message followership across same-round migrations is
//! handled in the store by [`forward_msgs`](crate::graph::GraphDb::forward_msgs).
//!
//! Configuration comes from the environment so the same binary can host any
//! subset of the actors:
//!
//! - `LADYBUG_DB` — LadybugDB server URL (the live store, not a sink).
//! - `GRAPH_HOSTS` — comma-separated host names for the placement plan
//!   (default: `local`).
//! - `GRAPH_RUN_ID`, `GRAPH_K`, `GRAPH_ALGO` — coordinator run parameters.

use std::{
	collections::{HashMap, HashSet},
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
use crate::clusters::{
	StepResult, directory, live_clusters, placement, run_cluster_superstep,
};

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

/// A process opens one remote client connection and shares it across every
/// worker/coordinator actor in that process (the client is a thin pooled HTTP
/// handle, so sharing is cheap). Every superstep read/write flows through it
/// to the store-owned partitions.
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

/// Keeps partition actors resident across supersteps for the lifetime of the
/// process. Without it, an idle worker hibernates after each superstep and
/// the engine pays a full actor cold-start to resume it for the next round.
/// Workers stay warm (one keep-awake region per owned cluster) so the barrier
/// loop runs hot.
static KEEP_AWAKE: LazyLock<Mutex<HashMap<i64, KeepAwakeRegion>>> =
	LazyLock::new(|| Mutex::new(HashMap::new()));

// ---------------------------------------------------------------------------
// VertexWorker: exclusive writer of one cluster slice
// ---------------------------------------------------------------------------

type BoxFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

/// Creation input naming the cluster this actor instance owns. The actor key
/// carries the same id; the input initializes freshly created state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerInput {
	pub cluster: i64,
}

/// The worker's whole persisted state: which slice it owns. Members, edges,
/// and inbox live in that slice's `Vertex`/`Msg` partitions, not here.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerState {
	pub cluster: i64,
}

/// Runs one superstep for the owned slice on the round-start assignment.
/// Reads the community's rows plus its inbox slice, persists updates and
/// migrations, and writes next-round messages addressed by home cluster.
/// Returns what happened for barrier counting and directory updates.
#[derive(Debug, Serialize, Deserialize)]
pub struct RunSuperstep {
	pub round: i64,
	pub k: i64,
	pub algo_idx: i64,
	pub member_ids: Vec<i64>,
}

impl Action for RunSuperstep {
	type Output = StepResult;
	const NAME: &'static str = "runSuperstep";
}

/// Stamps `core = k` on the owned slice's active members after a k-core run
/// converges, so finalization writes stay with the owning writer.
#[derive(Debug, Serialize, Deserialize)]
pub struct Finalize {
	pub k: i64,
}

impl Action for Finalize {
	type Output = i64;
	const NAME: &'static str = "finalize";
}

pub struct VertexWorker;

#[async_trait]
impl Actor for VertexWorker {
	type State = WorkerState;
	type Input = WorkerInput;
	type Actions = (RunSuperstep, Finalize);
	type Events = ();
	type Queue = ();
	type ConnParams = ();
	type ConnState = ();
	type Action = action::Raw;

	async fn create_state(_ctx: &Ctx<Self>, input: Self::Input) -> Result<Self::State> {
		Ok(WorkerState {
			cluster: input.cluster,
		})
	}

	async fn create(_ctx: &Ctx<Self>) -> Result<Self> {
		Ok(VertexWorker)
	}
}

fn worker_cluster(ctx: &Ctx<VertexWorker>) -> Result<i64> {
	// The key names the owned cluster; fall back to persisted state.
	if let Some(cluster) = ctx.key().as_slice().first().and_then(|segment| {
		match segment {
			rivetkit::ActorKeySegment::String(s) => s.parse().ok(),
			rivetkit::ActorKeySegment::Number(n) => Some(*n as i64),
		}
	}) {
		return Ok(cluster);
	}
	Ok(ctx.state().cluster)
}

impl Handles<RunSuperstep> for VertexWorker {
	type Future = BoxFuture<StepResult>;

	fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: RunSuperstep) -> Self::Future {
		Box::pin(async move {
			let cluster = worker_cluster(&ctx)?;
			let algo = algo(action.algo_idx, action.k)?;
			// Stay warm across the barrier; a cold-start resume between every
			// round would dominate the message-passing cost.
			KEEP_AWAKE
				.lock()
				.unwrap()
				.entry(cluster)
				.or_insert_with(|| ctx.keep_awake_region());
			let db = shared_db()?;
			let mut db = db.lock().unwrap();
			let step =
				run_cluster_superstep(&mut db, cluster, &action.member_ids, action.round, algo)?;
			info!(
				cluster,
				round = action.round,
				processed = step.processed,
				produced = step.produced,
				local = step.local,
				remote = step.remote,
				migrations = step.migrated.len(),
				"partition superstep complete"
			);
			Ok(step)
		})
	}
}

impl Handles<Finalize> for VertexWorker {
	type Future = BoxFuture<i64>;

	fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: Finalize) -> Self::Future {
		Box::pin(async move {
			let cluster = worker_cluster(&ctx)?;
			let db = shared_db()?;
			let mut db = db.lock().unwrap();
			// Owner-only writes: only this actor stamps its own slice.
			let members = db.read_cluster(cluster)?;
			let mut stamped = 0i64;
			for v in &members {
				if v.active && v.core != action.k {
					db.persist_vertex(&crate::graph::Vertex { core: action.k, ..v.clone() })?;
					stamped += 1;
				}
			}
			info!(cluster, stamped, "partition finalized");
			Ok(stamped)
		})
	}
}

// ---------------------------------------------------------------------------
// Coordinator: directory + barrier + placement
// ---------------------------------------------------------------------------

/// Drives an algorithm to a fixed point across the partition owners.
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

/// Summary returned to whoever started the run (the store already holds every
/// update; the coordinator only stamps run bookkeeping before replying).
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

/// One partition owner by cluster, creating it with its owned-cluster input
/// on first touch.
fn worker_key(cluster: i64) -> Vec<String> {
	vec![cluster.to_string()]
}

fn worker_input(cluster: i64) -> GetOrCreateOptions {
	GetOrCreateOptions {
		create_with_input: Some(serde_json::json!({ "cluster": cluster })),
		..Default::default()
	}
}

/// Placement hosts for the run's load plan (default: one local host).
fn hosts() -> Vec<String> {
	std::env::var("GRAPH_HOSTS")
		.map(|v| {
			v.split(',')
				.map(|h| h.trim().to_string())
				.filter(|h| !h.is_empty())
				.collect()
		})
		.ok()
		.filter(|v: &Vec<String>| !v.is_empty())
		.unwrap_or_else(|| vec!["local".to_string()])
}

impl Handles<RunAlgorithm> for Coordinator {
	type Future = BoxFuture<CoordinatorResult>;

	fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: RunAlgorithm) -> Self::Future {
		Box::pin(async move {
			let algo = algo(action.algo_idx, action.k)?;
			let client = ctx.client()?;

			// The directory snapshot assigns every vertex to its round's
			// owner. It is rebuilt from the store on every run and updated
			// from migration reports — action-local scratch, never persisted.
			let mut directory = {
				let db = shared_db()?;
				directory(&mut db.lock().unwrap())?
			};
			let hosts = hosts();
			{
				let costs: HashMap<i64, i64> = {
					let mut counts: HashMap<i64, i64> = HashMap::new();
					for home in directory.values() {
						*counts.entry(*home).or_insert(0) += 1;
					}
					counts
				};
				let plan = placement::assign_lpt(&costs, &hosts);
				info!(
					run_id = action.run_id,
					partitions = directory.values().collect::<HashSet<_>>().len(),
					vertices = directory.len(),
					?algo,
					?plan,
					"starting distributed algorithm run"
				);
			}

			// Barrier: heaviest partitions first so no owner idles behind a
			// long pole; the sequential order also keeps the exactly-once
			// snapshot invariant (see `run_cluster_superstep`).
			let mut costs: HashMap<i64, i64> = {
				let mut counts: HashMap<i64, i64> = HashMap::new();
				for home in directory.values() {
					*counts.entry(*home).or_insert(0) += 1;
				}
				counts
			};
			let mut produced = i64::MAX;
			let mut round = 0i64;
			let mut rounds = 0i64;
			let mut total_local = 0i64;
			let mut total_remote = 0i64;
			while produced > 0 {
				rounds += 1;
				if rounds > 10_000 {
					bail!("algorithm did not converge");
				}
				produced = 0;
				let mut live = live_clusters(&directory);
				if live.is_empty() {
					break;
				}
				live.sort_by(|a, b| {
					costs
						.get(b)
						.unwrap_or(&0)
						.cmp(costs.get(a).unwrap_or(&0))
						.then_with(|| a.cmp(b))
				});
				for cluster in live {
					let member_ids: Vec<i64> = directory
						.iter()
						.filter(|(_, home)| **home == cluster)
						.map(|(id, _)| *id)
						.collect();
					let worker = client
						.get_or_create_typed::<VertexWorker>(
							WORKER_ACTOR,
							worker_key(cluster),
							worker_input(cluster),
						)
						.context("get partition owner")?;
					let step = worker
						.call(RunSuperstep {
							round,
							k: action.k,
							algo_idx: action.algo_idx,
							member_ids,
						})
						.await
						.context("run partition superstep")?;
					produced += step.produced;
					total_local += step.local;
					total_remote += step.remote;
					costs.insert(cluster, step.processed + step.produced);
					for m in step.migrated {
						directory.insert(m.id, m.to);
					}
				}
				info!(
					round,
					produced,
					live_partitions = directory.values().collect::<HashSet<_>>().len(),
					"coordinator superstep barrier complete"
				);
				round += 1;
			}
			info!(
				total_local,
				total_remote,
				"message locality: local stayed inside the owning slice"
			);

			// Finalize through the owners, then report from the store (which
			// already holds every update) and stamp run bookkeeping.
			if let Algorithm::KCore { k } = algo {
				for cluster in live_clusters(&directory) {
					let worker = client
						.get_or_create_typed::<VertexWorker>(
							WORKER_ACTOR,
							worker_key(cluster),
							worker_input(cluster),
						)
						.context("get partition owner")?;
					worker
						.call(Finalize { k })
						.await
						.context("finalize partition")?;
				}
			}
			let (total, active) = {
				let db = shared_db()?;
				let mut db = db.lock().unwrap();
				let vertices = db.read_all_vertices()?;
				let mut total = 0i64;
				let mut active = 0i64;
				for v in &vertices {
					total += 1;
					if v.active {
						active += 1;
					}
					let tag = if v.active { "IN" } else { "OUT" };
					info!(
						id = v.id,
						server = v.server,
						cluster = v.cluster,
						degree = v.degree,
						core = v.core,
						tag,
						"vertex result"
					);
				}
				db.mark_done(action.run_id, rounds)?;
				db.clear_msgs()?;
				(total, active)
			};
			info!(rounds, vertices = total, active, "algorithm run complete");
			Ok(CoordinatorResult {
				rounds,
				vertices: total,
				active,
			})
		})
	}
}

/// Builds a registry containing the partition owners and the coordinator.
pub fn registry() -> Registry {
	let mut registry = Registry::new();
	registry.register_actor::<VertexWorker>(WORKER_ACTOR);
	registry.register_actor::<Coordinator>(COORDINATOR_ACTOR);
	registry
}
