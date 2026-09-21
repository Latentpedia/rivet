//! Cluster-owned supersteps over the LadybugDB partitioned tables.
//!
//! Model `B`: every community's rows live in the store — `Vertex`
//! `PARTITION BY LIST(cluster)` plus a colocated `Msg` inbox slice under the
//! same key — and one actor owns each cluster as its slice's **exclusive
//! writer**. Rivet holds no graph data: a worker's persisted state is only
//! its owned-cluster identity, and the coordinator's directory is rebuilt
//! from the store on every run. Two-level routing delivers each message to
//! the right owner: LadybugDB routes *rows* into partitions by the `cluster`
//! key, and Rivet routes *invocations* to the actor named by that key (the
//! same mapping a host's `locate()` hook claims would carry in a multi-host
//! deployment).
//!
//! Why this wins for computations like distributed Leiden: the local-moving
//! phase does most of its message passing *within* a community, and those
//! intra-cluster messages are written to and read from the owner's own `Msg`
//! partition — colocated with its `Vertex` rows, never crossing a partition
//! boundary. Only border edges address another community's slice.
//!
//! The barrier is sequential over clusters in a fixed (cost-descending)
//! order, which keeps one invariant simple: a vertex is processed exactly
//! once per round, by the owner named in the round-start directory snapshot.
//! Workers take their assigned member ids as an action argument and ignore
//! rows that arrived mid-round (they wait for the next round). Sends address
//! neighbors by fresh store lookups, so they follow vertices that migrated
//! earlier in the same round — and when a worker migrates one of its own
//! vertices out, it forwards that vertex's already-written round messages
//! behind it ([`GraphDb::forward_msgs`]), so no offer orphans in the old
//! partition. The cluster domain is fixed at seed time (every initial vertex
//! starts as its own community, `cluster = id`), so a migration target
//! always names a pre-seeded partition.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::algorithm::{Algorithm, kind};
use crate::graph::{GraphDb, Vertex};

/// Report of one vertex changing homes in a superstep. The row itself
/// already moved in the store (delete + insert by its old owner); this only
/// updates the coordinator's action-local directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationReport {
	pub id: i64,
	pub from: i64,
	pub to: i64,
}

/// What one owner's superstep did: how many members it processed, how many
/// messages it produced (split into partition-local vs cross-partition, the
/// Leiden locality signal), and who migrated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepResult {
	pub processed: i64,
	pub skipped: i64,
	pub produced: i64,
	pub local: i64,
	pub remote: i64,
	pub migrated: Vec<MigrationReport>,
}

/// Ratings proving the locality story: WCC starts almost fully remote (every
/// vertex its own community) and turns local as communities collapse.
#[derive(Debug, Default, Clone, Copy)]
pub struct EngineStats {
	pub local_msgs: i64,
	pub remote_msgs: i64,
}

/// The `vertex id -> cluster` directory, rebuilt from the store. Action-local
/// scratch for one run, never persisted: the store is the source of truth.
pub fn directory(db: &mut GraphDb) -> Result<HashMap<i64, i64>> {
	Ok(db
		.read_all_vertices()?
		.into_iter()
		.map(|v| (v.id, v.cluster))
		.collect())
}

/// Live clusters in ascending order: the invocation set for one barrier round.
pub fn live_clusters(directory: &HashMap<i64, i64>) -> Vec<i64> {
	let mut set: HashSet<i64> = directory.values().copied().collect();
	let mut live: Vec<i64> = set.drain().collect();
	live.sort_unstable();
	live
}

/// Runs one superstep for the owned cluster, entirely against the store.
///
/// Only `member_ids` — the round-start directory snapshot for this cluster —
/// are processed; rows that migrated in mid-round wait for the next round.
/// Every write targets the owned slice (own `Vertex` rows, own `Msg`
/// partition, or a delete + insert out of it); neighbor lookups and directory
/// scans may read across partitions but never write there.
pub fn run_cluster_superstep(
	db: &mut GraphDb,
	cluster: i64,
	member_ids: &[i64],
	round: i64,
	algo: Algorithm,
) -> Result<StepResult> {
	let assigned: HashSet<i64> = member_ids.iter().copied().collect();
	let mut members: HashMap<i64, Vertex> = HashMap::new();
	let mut skipped = 0i64;
	for v in db.read_cluster(cluster)? {
		if !assigned.contains(&v.id) {
			// Arrived mid-round via someone else's migration; next round's
			// snapshot will assign it here.
			skipped += 1;
			continue;
		}
		if v.cluster != cluster {
			bail!("vertex {} sits in partition {cluster} but claims cluster {}", v.id, v.cluster);
		}
		members.insert(v.id, v);
	}
	// Assigned ids must live here: only this owner moves them out, and it has
	// not run yet this round. Anything else is an ownership violation, which
	// fails loudly instead of computing on a torn view.
	for id in member_ids {
		if members.contains_key(id) {
			continue;
		}
		match db.read_vertex(*id)? {
			Some(v) => bail!(
				"vertex {id} assigned to cluster {cluster} but lives in cluster {}: ownership violated",
				v.cluster
			),
			None => bail!("vertex {id} assigned to cluster {cluster} but has no row"),
		}
	}

	// Only rounds >= 1 have messages aimed at them: superstep 0 processes the
	// initial state. The inbox is the owner's own `Msg` partition slice.
	let inbox_all = if round > 0 {
		db.read_cluster_msgs(cluster, round - 1)?
	} else {
		Vec::new()
	};
	let inbox: Vec<(i64, i64, i64)> = inbox_all
		.into_iter()
		.filter(|(to, _, _)| assigned.contains(to))
		.collect();

	let mut result = StepResult {
		processed: 0,
		skipped,
		produced: 0,
		local: 0,
		remote: 0,
		migrated: Vec::new(),
	};
	let mut ids: Vec<i64> = members.keys().copied().collect();
	ids.sort_unstable();
	for id in ids {
		let v = members[&id].clone();
		if !v.active {
			continue;
		}
		result.processed += 1;
		// Sends address neighbors by fresh store lookup, so they follow
		// vertices that migrated earlier in this same round.
		let mut send = |db: &mut GraphDb, to: i64, kind: i64, payload: i64| -> Result<()> {
			let home = db
				.read_vertex(to)?
				.with_context(|| format!("message target vertex {to} has no row"))?
				.cluster;
			db.write_cluster_msg(to, home, kind, payload, round)?;
			if home == cluster {
				result.local += 1;
			} else {
				result.remote += 1;
			}
			result.produced += 1;
			Ok(())
		};
		match algo {
			Algorithm::KCore { k } => {
				let decr: i64 = inbox
					.iter()
					.filter(|(to, k2, _)| *to == id && *k2 == kind::DECREMENT)
					.map(|(_, _, p)| *p)
					.sum();
				let new_degree = v.degree - decr;
				if new_degree < k {
					// Leave the k-core and propagate the drop to every
					// remaining neighbor for the next superstep.
					db.persist_vertex(&Vertex {
						degree: new_degree,
						core: k,
						active: false,
						..v.clone()
					})
					.context("persist k-core removal")?;
					for n in db.neighbors(id)? {
						send(db, n, kind::DECREMENT, 1)?;
					}
				} else if new_degree != v.degree {
					db.persist_vertex(&Vertex {
						degree: new_degree,
						..v.clone()
					})
					.context("persist degree")?;
				}
			}
			Algorithm::Wcc => {
				let mut label = v.cluster;
				for (to, k2, p) in &inbox {
					if *to == id && *k2 == kind::COMPONENT && *p < label {
						label = *p;
					}
				}
				if label < v.cluster {
					// Join the better community: delete + insert migrates the
					// row into the label's partition (the engine refuses
					// in-place partition-key updates) and rewires its
					// incident edges. `value` mirrors the label.
					let updated = Vertex {
						value: label,
						..v.clone()
					};
					db.move_vertex_to_cluster(&updated, label)
						.context("migrate wcc vertex")?;
					// Offers written earlier this round still sit in the old
					// inbox slice; forward them behind the vertex.
					db.forward_msgs(id, cluster, label, round)?;
					result.migrated.push(MigrationReport {
						id,
						from: cluster,
						to: label,
					});
					for n in db.neighbors(id)? {
						send(db, n, kind::COMPONENT, label)?;
					}
				} else if round == 0 {
					// Seed: in the first superstep each vertex offers its own
					// label to its neighbors so propagation has a start.
					for n in db.neighbors(id)? {
						send(db, n, kind::COMPONENT, v.cluster)?;
					}
				}
			}
		}
	}
	Ok(result)
}

/// Asserts the ownership invariant directly against the store, for tests:
/// every `Vertex` row in a cluster's partition carries that cluster, every
/// `Msg` row in a cluster's partition is addressed to that cluster, and the
/// directory built from the parent scan agrees throughout.
pub fn check_store_ownership(db: &mut GraphDb) -> Result<()> {
	// Fresh handles discover nothing until they read: refresh both maps so the snapshot
	// below observes every partition the engine holds, including emptied ones.
	db.refresh_partitions()?;
	let directory = directory(db)?;
	// Vertex slices.
	let vertex_map = db.router().cluster_map().clone();
	for (cluster, table) in &vertex_map {
		let rows = db.query(&format!("MATCH (v:{table}) RETURN v.id, v.cluster"))?;
		for r in &rows {
			let id = match r.get(0).cloned().flatten() {
				Some(lbug::Value::Int64(v)) => v,
				other => bail!("expected Int64 vertex id in {table}, got {other:?}"),
			};
			let home = match r.get(1).cloned().flatten() {
				Some(lbug::Value::Int64(v)) => v,
				other => bail!("expected Int64 cluster in {table}, got {other:?}"),
			};
			if home != *cluster {
				bail!("row {id} sits in {table} but claims cluster {home}");
			}
			match directory.get(&id) {
				Some(dir) if dir == cluster => {}
				other => bail!("row {id} in {table} but directory says {other:?}"),
			}
		}
	}
	let held: HashSet<i64> = vertex_map
		.keys()
		.flat_map(|c| {
			db.read_cluster(*c)
				.unwrap_or_default()
				.into_iter()
				.map(|v| v.id)
		})
		.collect();
	if held.len() != directory.len() {
		bail!(
			"partitions hold {} vertices but the directory tracks {}",
			held.len(),
			directory.len()
		);
	}
	// Inbox slices.
	let msg_map = db.router().msg_cluster_map().clone();
	for (cluster, table) in &msg_map {
		let rows = db.query(&format!("MATCH (m:{table}) RETURN m.to_id, m.cluster"))?;
		for r in &rows {
			let to_id = match r.get(0).cloned().flatten() {
				Some(lbug::Value::Int64(v)) => v,
				other => bail!("expected Int64 to_id in {table}, got {other:?}"),
			};
			let home = match r.get(1).cloned().flatten() {
				Some(lbug::Value::Int64(v)) => v,
				other => bail!("expected Int64 cluster in {table}, got {other:?}"),
			};
			if home != *cluster {
				bail!("message for {to_id} sits in {table} but is addressed to {home}");
			}
		}
	}
	Ok(())
}

/// Placement policy: where Rivet should run each cluster's actor so no host
/// idles while another queues. Greedy longest-processing-time-first over the
/// latest per-cluster costs; in a multi-host deployment the winning mapping
/// is exactly what each host's `locate()` hook claims, keeping compute next
/// to the partitions it owns.
pub mod placement {
	use std::collections::HashMap;

	/// Assigns every cluster to one host, balancing total cost. `costs` maps
	/// cluster to its latest weight (members + inbox size); `hosts` are tried
	/// in order with ties broken toward the earlier host, so the mapping is
	/// deterministic for a fixed input. Returns host -> clusters (sorted).
	pub fn assign_lpt(costs: &HashMap<i64, i64>, hosts: &[String]) -> HashMap<String, Vec<i64>> {
		assert!(!hosts.is_empty(), "placement needs at least one host");
		let mut order: Vec<i64> = costs.keys().copied().collect();
		order.sort_unstable_by(|a, b| {
			costs[b]
				.cmp(&costs[a])
				.then_with(|| a.cmp(b))
		});
		let mut load: HashMap<&str, i64> = HashMap::new();
		let mut out: HashMap<String, Vec<i64>> = HashMap::new();
		for h in hosts {
			load.insert(h.as_str(), 0);
			out.insert(h.clone(), Vec::new());
		}
		for cluster in order {
			let host = hosts
				.iter()
				.min_by(|a, b| {
					load[a.as_str()]
						.cmp(&load[b.as_str()])
						.then_with(|| a.cmp(b))
				})
				.unwrap();
			*load.get_mut(host.as_str()).unwrap() += costs[&cluster];
			out.get_mut(host).unwrap().push(cluster);
		}
		for clusters in out.values_mut() {
			clusters.sort_unstable();
		}
		out
	}

	#[cfg(test)]
	mod tests {
		use super::*;

		#[test]
		fn balances_heavy_clusters_apart() {
			let costs: HashMap<i64, i64> = [(0, 100), (1, 90), (2, 10), (3, 10)].into();
			let hosts = ["a".to_string(), "b".to_string()];
			let plan = assign_lpt(&costs, &hosts);
			// Heaviest two land on different hosts; totals 110 vs 100.
			assert!(plan["a"].contains(&0) != plan["b"].contains(&0));
			let load = |h: &str| plan[h].iter().map(|c| costs[c]).sum::<i64>();
			assert!((load("a") - load("b")).abs() <= 10);
		}

		#[test]
		fn single_host_takes_all() {
			let costs: HashMap<i64, i64> = [(0, 5), (1, 7)].into();
			let plan = assign_lpt(&costs, &["solo".to_string()]);
			assert_eq!(plan["solo"], vec![0, 1]);
		}

		#[test]
		fn deterministic_ties() {
			let costs: HashMap<i64, i64> = [(2, 1), (1, 1), (0, 1)].into();
			let hosts = ["a".to_string(), "b".to_string()];
			assert_eq!(assign_lpt(&costs, &hosts), assign_lpt(&costs, &hosts));
		}
	}
}
