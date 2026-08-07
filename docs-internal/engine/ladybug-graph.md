# Ladybug graph driver (Cypher backend)

The `ladybug` driver is the embedded property-graph (Cypher) backend for of UniversalDB,
parallel to the `rocksdb` and `postgres` key/value drivers. It wraps the `lbug` crate to
persist a property graph (node and relationship tables with typed properties) and query it
with the ladybug dialect of Cypher.

## Capabilities

- **Persistence** — `LadybugDatabaseDriver::new(path, config)` opens or creates a durable
  graph store at `path` (the path is the database prefix, not a directory; the parent must
  exist). `LadybugDatabaseDriver::in_memory()` opens a throwaway store.
- **Transactions** — writes are buffered by a `LadybugTransaction` and applied atomically by
  `commit()` inside `BEGIN TRANSACTION .. COMMIT`. `abort()` discards them and issues a
  `ROLLBACK`. Reads execute immediately against the committed graph.
- **Checkpointing** — `force_checkpoint()` / `DatabaseDriver::checkpoint()` flush the
  write-ahead log into the data files (`CHECKPOINT`). Unlike rocksdb, LadybugDB checkpoints in
  place, so the checkpoint path argument is ignored; there is no point-in-time directory copy.
- **Retrieval** — `LadybugTransaction::query` / `query_params` run Cypher and return named
  `LadybugRow`s.

## Relation to the key/value `DatabaseDriver` trait

A graph store is not a byte key/value store. The `DatabaseDriver`/`TransactionDriver` traits
are byte-oriented, so the ladybug driver implements them only for lifecycle parity (a
`DatabaseDriverHandle` can point at either backend), and every key/value method fails by
default with an explicit error. The supported surface is the graph API
(`graph_txn` / `run_graph`).

## How actors interact with the graph

Recommended model: a **strongly typed application-object layer over parameterized Cypher**,
not raw Cypher string interpolation.

- Queries are written once as parameterized Cypher (`WHERE p.id = $id`) and executed through
  `lbug` prepared statements (`LadybugTransaction::query_params`). Caller-supplied values are
  bound as parameters, not spliced into the query AST, which is Cypher-injection safe and lets
  the engine cache the plan.
- Actor DTOs map to node/rel table schemas on one side and to typed `LadybugRow` reads on the
  other; the mapper becomes the single place that knows column names and types, so the rest of
  the actor code deals in typed objects rather than strings.
- Raw ad-hoc traversal strings remain available through `LadybugTransaction::query` for
  exploratory queries, gated behind the same transaction path.

Concretely, an actor flow looks like:

```rust
let txn = driver.graph_txn();
txn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))");
txn.commit().await?;

let txn = driver.graph_txn();
txn.create_node(&LadybugNodeSpec {
    label: "Person".into(),
    props: vec![("id".into(), Value::Int64(1)), ("name".into(), Value::String("Alice".into()))],
})?;
txn.commit().await?;

let rows = driver
    .graph_txn()
    .query_params("MATCH (p:Person) WHERE p.id = $id RETURN p.name", &[("id", Value::Int64(1))])
    .await?;
```

## Caveats / next steps

- Reads mid-transaction see the last committed state, not buffered writes (writes are applied
  only at commit). A write-then-read-within-one-transaction semantic is future work.
- `create_rel` currently asks the caller to write the `MATCH .. CREATE` Cypher explicitly;
  automating rel creation from a schema requires the endpoint tables and their primary-key
  column/type, which is future work.
- The `run`/key-value path is intentionally unsupported; routing the engine's existing
  key/value callers onto the graph would require mapping opaque byte keys to graph entities and
  is out of scope.
