# Per-host LadybugDB servers for ladybug-graph (model B, sharded)

Status: spec (not implemented). Builds on the merged cluster-ownership work:
one `VertexWorker` actor per cluster, all state in `Vertex`/`Msg`
`PARTITION BY LIST(cluster)` tables, Rivet doing invocation routing plus
cost-aware placement.

## Goal

Run one `ladybug-server` per host, each holding only the partitions assigned
to that host, with several clusters per host. Keep the programming model
(pruned slice reads, owner-only writes, sequential barrier, forward-on-migrate)
unchanged; change only *where* slices live and how cross-host traffic commits.

Non-goals: per-actor servers (too fine-grained, see §6), changing the
barrier semantics, multi-writer partitions (one cluster still has exactly one
owner at a time).

## Background: what the tree already has

- `examples/ladybug-graph`: single server owns the file; actors are remote
  ADBC clients. `PartitionRouter` discovers the engine's value→partition map
  per parent; `GraphDb::forward_msgs` closes the same-round migration race.
- `engine/packages/universaldb`: FDB-style transaction API (optimistic,
  versionstamps, conflict tracking) over three drivers — rocksdb, postgres,
  ladybug. There is **no FoundationDB cluster** in the tree (only
  `foundationdb-tuple` key encoding); "distributed" here means API shape,
  not a multi-writer store.
- `engine/packages/depot`: actor SQLite pages stored transactionally in
  UniversalDB. Durable and per-actor, not a shared log.
- NATS (`async-nats` via `universalpubsub`) is the service messaging
  backbone; persistent JetStream streams are not confirmed wired anywhere.
- `DatabaseDriver::snapshot` exists per UniversalDB driver (backup hook).

## Layout

- H hosts. Host h runs one `ladybug-server` with file `host-h.lbdb`
  holding its assigned clusters' `Vertex_*`/`Msg_*` partitions, and serves
  the same `POST /rpc` ADBC endpoint.
- One **shared control file** (tiny, single-writer, same server binary)
  holding `Run`, the `cluster → host` directory, and the `MoveIntent` log
  (§4). Shard the data, not the control plane.
- Assignment comes from the existing `placement::assign_lpt` plan over
  per-cluster costs; the coordinator persists the winning mapping into the
  control store at run start. Rebalance = reassign whole communities between
  hosts (never split one).
- Facade work: `GraphDb` needs multi-endpoint routing (one pooled client per
  host URL, route by directory). The ADBC driver itself is unchanged.

## Protocol changes

### Sends (cross-host write, single row)

Sender resolves `neighbor → (home cluster → host)` via the directory, then
writes the `Msg` row **to the owner's endpoint**. Inbox reads stay local and
pruned on every host; the network cost sits on senders, who already pay a
point read per send. Rationale for explicit endpoint routing over hook
`insert_row` delivery: claimed-LIST partitions have engine gaps (no direct
partition scans, dynamic-creation failures), while explicit addressing keeps
the followership reasoning from the single-file design intact.

### Moves

- **Intra-host** (both clusters on one host): identical to today — delete +
  insert + edge rewire + in-file forward, atomic under that host's single
  write lock. No log traffic. This is the common case once communities
  colocate, and the reason for several-clusters-per-host over per-actor
  servers.
- **Cross-host**: two-phase with intents in the shared control store:
  1. Old owner writes `MoveIntent { move_id, vertex snapshot, from, to }`
     and marks the row `moving` (readable, skipped by compute).
  2. New owner replays idempotently by `move_id`: insert row + edges into
     its file, ack into the control store.
  3. Old owner deletes its copy, forwards any same-round stragglers to the
     new owner's endpoint, clears the intent.
  4. Crash recovery: any host may replay an unacked intent; versionstamps
     order competing replays. Exactly-once by `move_id`, same dedup shape
     the `round` column already uses.

### Barrier and failover

- Coordinator resolves `(cluster → host endpoint)` per round, keeps the
  sequential cost-descending order (exactly-once snapshot invariant
  unchanged), and opens one facade per host.
- Host down = its slices unreadable; the barrier cannot converge without
  all owners, so failover is reassignment + restore: replay the host's
  files from the latest `snapshot` into a spare, replay open intents, flip
  the directory. RPO/RTO are snapshot cadence questions, explicitly
  out of scope for the example.

## Atomicity: can a Rivet storage engine be the distributed WAL?

Short answer: no turnkey distributed WAL exists in the tree; the practical
one is the shared control file above, optionally graduated later.

- **RocksDB** (UniversalDB driver): embedded, single node. Its WAL survives
  process crash, not disk loss; no distribution. Fine for a host-local
  intent staging area, not cross-host atomicity.
- **SQLite** (actor SQLite, depot-paged): local WAL file; depot makes pages
  durable and transactional *within one UniversalDB*, but it is per-actor
  storage, not a shared log. Wrong shape for cross-host intents.
- **Postgres** (UniversalDB driver): real WAL shipping with sync commit and
  standby failover — the closest thing to a distributed WAL present. But
  single-primary: it gives a durable, ordered intent log with failover, not
  multi-writer. If intents move off the control file, a postgres-backed
  UniversalDB with versionstamp-ordered intent keys is the natural second
  home (hosts already link UniversalDB via depot).
- **LadybugDB embedded** (UniversalDB driver and our store): has WAL +
  auto-checkpoint + checksums (`LadybugConfig`), but strictly local to the
  file owner. Our control file already exploits exactly this.
- **FoundationDB proper**: not deployed. UniversalDB mirrors its API, so a
  future FDB-backed driver would slot in without changing callers — but that
  is new infrastructure, not a reuse.
- **NATS JetStream**: would be the idiomatic distributed WAL here (ordered
  persistent streams, redelivery, consumer acks = the replay protocol in
  §4). Not confirmed wired; evaluate before adopting.

Recommendation: keep intents in the shared control file (no new infra, same
ADBC path, same operators as the data files). Move to postgres-backed
UniversalDB intents only if the control file's single writer shows up in
profiles; JetStream only if the system needs cross-service eventing anyway.

## Why not per-actor servers

WCC/Leiden *concentrate* data: a collapsed run ends with everything in one
community, i.e. seven idle servers and one full one. Static
partition→server assignment fights the algorithm's objective. Per-host with
several clusters each keeps rebalancing at whole-community granularity
(cheap: flip the directory, replay intents) instead of re-sharding truth.

## Rollout

1. Phase 0 (today): single file; placement plan logged, not enacted.
2. Phase 1: per-host files + shared control file; multi-endpoint facade;
   endpoint-routed sends; intent-logged cross-host moves; snapshot-based
   failover documented, not automated.
3. Phase 2 (only on measured need): hook-claimed placement (`locate()` per
   host from the directory), intent log graduation (§5), automated failover.

## Open questions

- Facade API shape for per-host clients (one `GraphDb` with a router table
  vs one `GraphDb` per host URL).
- `moving` flag representation on `Vertex` (new column vs `Run`-side table).
- Snapshot cadence and restore tooling for host failover.
- JetStream availability for cross-service intent streaming.
