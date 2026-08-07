use std::{
	future::Future,
	pin::Pin,
	sync::Arc,
};

use anyhow::{Context, Result, bail};
use lbug::Value;

use super::database::SharedInternal;
use crate::{
	driver::TransactionDriver,
	key_selector::KeySelector,
	options::{ConflictRangeType, MutationType},
	range_option::RangeOption,
	utils::IsolationLevel,
	value::{Slice, Values},
};

/// A single row returned by a Cypher query, with named column access.
#[derive(Debug, Clone)]
pub struct LadybugRow {
	columns: Vec<String>,
	values: Vec<Value>,
}

impl LadybugRow {
	fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
		LadybugRow { columns, values }
	}

	/// The column names in the order they were projected by the query.
	pub fn columns(&self) -> &[String] {
		&self.columns
	}

	/// The raw values in position order.
	pub fn values(&self) -> &[Value] {
		&self.values
	}

	/// Returns the value at `index`, if present.
	pub fn get_index(&self, index: usize) -> Option<&Value> {
		self.values.get(index)
	}

	/// Returns the value under `column`, if the column was projected.
	pub fn get(&self, column: &str) -> Option<&Value> {
		let index = self.columns.iter().position(|c| c == column)?;
		self.values.get(index)
	}
}

/// Description of a node to create. `label` is the node-table label and `props` are the
/// typed property name/value pairs declared by that table's schema.
#[derive(Debug, Clone)]
pub struct LadybugNodeSpec {
	pub label: String,
	pub props: Vec<(String, Value)>,
}

/// Description of a relationship to create between two existing nodes. `label` is the
/// relationship-table label; the endpoint primary-key values are supplied as strings so the
/// caller does not have to know the exact PK column names.
#[derive(Debug, Clone)]
pub struct LadybugRelSpec {
	pub label: String,
	/// Primary-key value of the source node.
	pub from: Value,
	/// Primary-key value of the destination node.
	pub to: Value,
	/// Relationship property name/value pairs.
	pub props: Vec<(String, Value)>,
}

/// The supported transaction surface for the ladybug graph backend.
///
/// Reads (`query`, `query_params`) execute immediately against the committed graph. Writes
/// (`execute`, `create_node`, `create_rel`, `delete_node`) are buffered in memory and applied
/// atomically by [`LadybugTransaction::commit`] inside `BEGIN TRANSACTION .. COMMIT`. A
/// consequence of buffering writes is that reads issued mid-transaction see the last committed
/// state, not pending buffered writes; this is documented as the initial, deliberately simple
/// semantics rather than a silent surprise.
#[derive(Clone)]
pub struct LadybugTransaction {
	shared: Arc<SharedInternal>,
	inner: Arc<parking_lot::Mutex<TxnInner>>,
}

#[derive(Clone)]
struct TxnInner {
	committed: bool,
	aborted: bool,
	ops: Vec<String>,
}

impl LadybugTransaction {
	pub(crate) fn new(shared: Arc<SharedInternal>) -> Self {
		LadybugTransaction {
			shared,
			inner: Arc::new(parking_lot::Mutex::new(TxnInner {
				committed: false,
				aborted: false,
				ops: Vec::new(),
			})),
		}
	}

	/// Executes a Cypher query and returns every projected row. The query is a read and does
	/// not participate in the buffered write transaction.
	pub async fn query(&self, cypher: &str) -> Result<Vec<LadybugRow>> {
		let conn = lbug::Connection::new(&self.shared.db)
			.context("failed to create ladybug connection")?;
		let result = conn
			.query(cypher)
			.with_context(|| format!("ladybug query failed: {cypher}"))?;
		Ok(collect_rows(result))
	}

	/// Executes a parameterized Cypher query. Prepared statements keep caller-supplied values out
	/// of the query AST, which is the recommended way to pass actor data into a graph query.
	///
	/// Parameters are referenced in the query with `$name` (for example `WHERE p.id = $id`).
	pub async fn query_params(
		&self,
		cypher: &str,
		params: &[(&str, Value)],
	) -> Result<Vec<LadybugRow>> {
		let conn = lbug::Connection::new(&self.shared.db)
			.context("failed to create ladybug connection")?;
		let mut statement = conn
			.prepare(cypher)
			.with_context(|| format!("failed to prepare ladybug query: {cypher}"))?;
		let result = conn
			.execute(&mut statement, params.to_vec())
			.with_context(|| format!("ladybug query failed: {cypher}"))?;
		Ok(collect_rows(result))
	}

	/// Buffers a Cypher write statement to be applied on commit. Reads are not buffered.
	pub fn execute(&self, cypher: &str) {
		let mut inner = self.inner.lock();
		if !inner.committed && !inner.aborted {
			inner.ops.push(cypher.to_string());
		}
	}

	/// Buffers creation of a node with the given label and typed properties.
	///
	/// Values are rendered as literals. Driver-known schema values are fine here; for
	/// caller-supplied ad-hoc input prefer `query_params` so values stay out of the query AST.
	pub fn create_node(&self, spec: &LadybugNodeSpec) -> Result<()> {
		let mut props = String::new();
		for (name, value) in &spec.props {
			if !props.is_empty() {
				props.push_str(", ");
			}
			props.push_str(&format!("{}: {}", name, value_to_cypher_literal(value)));
		}
		let query = format!("CREATE (:{} {{{}}})", spec.label, props);
		self.inner.lock().ops.push(query);
		Ok(())
	}

	/// Buffers creation of a relationship between two existing nodes.
	pub fn create_rel(&self, _spec: &LadybugRelSpec) -> Result<()> {
		bail!("create_rel requires knowing the endpoint tables and their primary-key columns; \
		       construct the Cypher MATCH .. CREATE statement explicitly for now")
	}

	/// Buffers deletion of all nodes matching a label and predicate.
	pub fn delete_nodes(&self, cypher: &str) {
		self.execute(cypher);
	}

	/// Applies all buffered writes atomically. After commit the transaction is spent; further
	/// calls return a clear error rather than silently succeeding.
	pub async fn commit(&self) -> Result<()> {
		let mut inner = self.inner.lock();
		if inner.committed {
			bail!("ladybug transaction already committed");
		}
		if inner.aborted {
			bail!("ladybug transaction was aborted");
		}

		let ops = std::mem::take(&mut inner.ops);
		let _guard = self.shared.write_lock.lock();

		let conn = lbug::Connection::new(&self.shared.db)
			.context("failed to create ladybug connection for commit")?;
		conn.query("BEGIN TRANSACTION")
			.context("failed to begin ladybug transaction")?;

		let mut failed = false;
		let mut first_error: Option<anyhow::Error> = None;
		for op in &ops {
			if let Err(err) = conn.query(op) {
				failed = true;
				first_error = Some(err.context("ladybug write failed"));
			}
		}

		if failed {
			let _ = conn.query("ROLLBACK");
			let error = first_error.unwrap_or_else(|| anyhow::anyhow!("ladybug write failed"));
			inner.committed = true; // transaction is spent either way
			return Err(error);
		}

		conn.query("COMMIT").context("failed to commit ladybug transaction")?;
		inner.committed = true;
		Ok(())
	}

	/// Discards buffered writes without applying them.
	pub fn abort(&self) {
		let mut inner = self.inner.lock();
		inner.ops.clear();
		inner.aborted = true;
	}
}

/// Renders a `Value` as a literal suitable for interpolation into a Cypher `CREATE` clause.
///
/// This is used only by the typed helpers which construct statements from driver-known schema
/// values. Caller-supplied ad-hoc input should go through `query_params` instead so it stays out
/// of the query AST.
fn value_to_cypher_literal(value: &Value) -> String {
	match value {
		Value::Null(_) => "NULL".to_string(),
		Value::Bool(b) => b.to_string(),
		Value::Int8(v) => v.to_string(),
		Value::Int16(v) => v.to_string(),
		Value::Int32(v) => v.to_string(),
		Value::Int64(v) => v.to_string(),
		Value::Int128(v) => v.to_string(),
		Value::UInt8(v) => v.to_string(),
		Value::UInt16(v) => v.to_string(),
		Value::UInt32(v) => v.to_string(),
		Value::UInt64(v) => v.to_string(),
		Value::Float(v) => v.to_string(),
		Value::Double(v) => v.to_string(),
		Value::String(s) => format!("'{}'", s.replace('\'', "\\'")),
		Value::Blob(bytes) => {
			let hex_string: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
			format!("from_hex('{hex_string}')")
		}
		_ => format!("'{}'", value),
	}
}

/// Key/value transaction surface for the graph driver.
///
/// This exists so a `LadybugDatabaseDriver` can be held as a [`crate::driver::DatabaseDriverHandle`]
/// alongside rocksdb, but it is not the graph interface. Every method fails by default with an
/// explicit error rather than silently interpreting bytes as graph data. Use
/// [`LadybugTransaction`] for the real surface.
pub struct LadybugTransactionDriver;

/// Collects a `lbug::QueryResult` into owned rows, capturing the projected column names.
fn collect_rows<'a>(mut result: lbug::QueryResult<'a>) -> Vec<LadybugRow> {
	let columns = result.get_column_names();
	result
		.map(|values| LadybugRow::new(columns.clone(), values))
		.collect()
}

impl TransactionDriver for LadybugTransactionDriver {
	fn atomic_op(&self, _key: &[u8], _param: &[u8], _op_type: MutationType) {
		// Fire-and-forget in the trait; rejected at commit.
	}

	fn get<'a>(
		&'a self,
		_key: &[u8],
		_isolation_level: IsolationLevel,
	) -> Pin<Box<dyn Future<Output = Result<Option<Slice>>> + Send + 'a>> {
		Box::pin(async move {
			bail!(
				"ladybug graph driver: key/value `get` is not supported; \
				 use `LadybugTransaction::query`"
			)
		})
	}

	fn get_key<'a>(
		&'a self,
		_selector: &KeySelector<'a>,
		_isolation_level: IsolationLevel,
	) -> Pin<Box<dyn Future<Output = Result<Slice>> + Send + 'a>> {
		Box::pin(async move {
			bail!(
				"ladybug graph driver: key/value `get_key` is not supported; \
				 use `LadybugTransaction::query`"
			)
		})
	}

	fn get_range<'a>(
		&'a self,
		_opt: &RangeOption<'a>,
		_iteration: usize,
		_isolation_level: IsolationLevel,
	) -> Pin<Box<dyn Future<Output = Result<Values>> + Send + 'a>> {
		Box::pin(async move {
			bail!(
				"ladybug graph driver: key/value `get_range` is not supported; \
				 use `LadybugTransaction::query`"
			)
		})
	}

	fn get_ranges_keyvalues<'a>(
		&'a self,
		_opt: RangeOption<'a>,
		_isolation_level: IsolationLevel,
	) -> crate::value::Stream<'a, Value> {
		use futures_util::stream;
		Box::pin(stream::once(async {
			Err(anyhow::anyhow!(
				"ladybug graph driver: key/value `get_ranges_keyvalues` is not supported"
			))
		}))
	}

	fn set(&self, _key: &[u8], _value: &[u8]) {}

	fn clear(&self, _key: &[u8]) {}

	fn clear_range(&self, _begin: &[u8], _end: &[u8]) {}

	fn commit(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
		Box::pin(async move {
			bail!(
				"ladybug graph driver: key/value commit is not supported; \
				 use `LadybugTransaction::commit`"
			)
		})
	}

	fn reset(&mut self) {}

	fn cancel(&self) {}

	fn add_conflict_range(
		&self,
		_begin: &[u8],
		_end: &[u8],
		_conflict_type: ConflictRangeType,
	) -> Result<()> {
		Ok(())
	}

	fn get_estimated_range_size_bytes<'a>(
		&'a self,
		_begin: &'a [u8],
		_end: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<i64>> + Send + 'a>> {
		Box::pin(async move {
			bail!(
				"ladybug graph driver: key/value `get_estimated_range_size_bytes` is not supported"
			)
		})
	}
}
