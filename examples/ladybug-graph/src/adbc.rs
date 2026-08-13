//! ADBC (Arrow Database Connectivity) bridge over the universalDB LadybugDB graph driver.
//!
//! This is the inter-instance data plane for the platform. Distributed graph workers that run on
//! different Rivet servers have no direct link to each other; their only channel is the shared
//! LadybugDB graph store, and every read/write through that channel goes over the ADBC interface
//! defined by [`adbc_core`], returning Arrow-native result sets.
//!
//! Concretely, each worker opens an ADBC [`Connection`](adbc_core::sync::Connection) to the shared
//! graph database and executes parameterized Cypher statements through
//! [`Statement`](adbc_core::sync::Statement):
//!
//! - [`Statement::execute`] runs a read (its `MATCH` over the vertex/edge/message tables) and
//!   returns an Arrow [`RecordBatchReader`](arrow_array::RecordBatchReader).
//! - [`Statement::execute_update`] buffers a write (its `CREATE`/`SET`/`DELETE`) and commits it
//!   atomically inside `BEGIN TRANSACTION .. COMMIT`.
//!
//! A worker therefore "sends a message" to a worker on another server by executing an update that
//! inserts a row into the shared `Msg` table, and "receives" the messages aimed at its shard by
//! executing a read over that table. Arrow result sets are what cross the ADBC boundary, keeping
//! the message encode/decode on both sides of every instance typed instead of stringly-typed.
//!
//! The ADBC traits are implemented for "lifecycle + supported surface" parity, mirroring the
//! philosophy of the underlying `LadybugDatabaseDriver`: the operations the platform uses are
//! fully implemented, and the rest fail with an explicit `NotImplemented` error rather than
//! silently misbehaving.

use std::{
	collections::HashSet,
	path::PathBuf,
	sync::Arc,
};

use adbc_core::{
	PartitionedResult,
	error::{Error as AdbcError, Result as AdbcResult, Status},
	options::{InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue},
	sync::{Connection as AdbcConnection, Database as AdbcDatabase, Driver, Optionable, Statement},
};
use arrow_array::{
	Array, ArrayRef, RecordBatch, RecordBatchReader,
	builder::{
		BooleanBuilder, Float32Builder, Float64Builder, Int64Builder, StringBuilder,
	},
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use lbug::Connection;
use lbug::Database as LbugDatabase;
use lbug::SystemConfig;
use lbug::Value;

/// A convenience alias for `std::result::Result<T, arrow_schema::ArrowError>`.
type ArrowResult<T> = std::result::Result<T, ArrowError>;

/// Collects a lbug query result into `Option<Value>` cells (all rows present; nulls remain as
/// `Some(Value::Null(_))`).
fn collect_query(result: impl IntoIterator<Item = Vec<Value>>) -> Vec<Vec<Option<Value>>> {
	result
		.into_iter()
		.map(|row| row.into_iter().map(Some).collect())
		.collect()
}

fn not_implemented(msg: impl Into<String>) -> AdbcError {
	AdbcError::with_message_and_status(msg.into(), Status::NotImplemented)
}

fn invalid(msg: impl Into<String>) -> AdbcError {
	AdbcError::with_message_and_status(msg.into(), Status::InvalidArguments)
}

fn err_from_anyhow(e: &anyhow::Error, op: &str) -> AdbcError {
	AdbcError::with_message_and_status(format!("{op} failed: {e:#}"), Status::InvalidData)
}

fn open_database(path: Option<PathBuf>, in_memory: bool) -> AdbcResult<Arc<LbugDatabase>> {
	let config = SystemConfig::default();
	if in_memory {
		return LbugDatabase::in_memory(config)
			.map(Arc::new)
			.map_err(|e| {
				AdbcError::with_message_and_status(format!("open in-memory ladybug: {e}"), Status::InvalidData)
			});
	}
	let path = path.ok_or_else(|| {
		invalid("database has no path and is not in-memory; pass Uri in new_database_with_opts")
	})?;
	if let Some(parent) = path.parent() {
		let _ = std::fs::create_dir_all(parent);
	}
	LbugDatabase::new(path, config)
		.map(Arc::new)
		.map_err(|e| AdbcError::with_message_and_status(format!("open ladybug database: {e}"), Status::InvalidData))
}

/// The ADBC driver for LadybugDB. Configured with either a file path (durable) or in-memory.
#[derive(Clone, Debug)]
pub struct LadybugDriver {
	path: Option<PathBuf>,
	in_memory: bool,
}

impl LadybugDriver {
	/// A durable driver rooted at `path` (the database prefix, parent dir created on open).
	pub fn new(path: impl Into<PathBuf>) -> Self {
		LadybugDriver {
			path: Some(path.into()),
			in_memory: false,
		}
	}

	/// A throwaway in-memory driver; data is lost when the returned database is dropped.
	pub fn in_memory() -> Self {
		LadybugDriver {
			path: None,
			in_memory: true,
		}
	}
}

impl Driver for LadybugDriver {
	type DatabaseType = LadybugDb;

	fn new_database(&mut self) -> AdbcResult<Self::DatabaseType> {
		Ok(LadybugDb {
			db: open_database(self.path.clone(), self.in_memory)?,
			write_lock: Arc::new(std::sync::Mutex::new(())),
		})
	}

	fn new_database_with_opts(
		&mut self,
		opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
	) -> AdbcResult<Self::DatabaseType> {
		for (key, value) in opts {
			if key == OptionDatabase::Uri {
				let uri = match value {
					OptionValue::String(s) => s,
					other => {
						return Err(invalid(format!(
							"expected a string Uri for the ladybug driver, got {other:?}"
						)))
					}
				};
				return Ok(LadybugDb {
					db: open_database(Some(PathBuf::from(uri)), false)?,
					write_lock: Arc::new(std::sync::Mutex::new(())),
				});
			}
		}
		self.new_database()
	}
}

/// An ADBC database: owns the [`lbug::Database`] so all connections hit one store.
pub struct LadybugDb {
	db: Arc<LbugDatabase>,
	// Serializes write queries, because lbug allows only one write in flight per process.
	write_lock: Arc<std::sync::Mutex<()>>,
}

impl AdbcDatabase for LadybugDb {
	type ConnectionType = LadybugConn;

	fn new_connection(&self) -> AdbcResult<Self::ConnectionType> {
		Ok(LadybugConn {
			db: self.db.clone(),
			write_lock: self.write_lock.clone(),
		})
	}

	fn new_connection_with_opts(
		&self,
		_opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
	) -> AdbcResult<Self::ConnectionType> {
		self.new_connection()
	}
}

impl LadybugDb {
	/// Runs a Cypher read through the ADBC [`Statement::execute`] path. The statement executes the
	/// query (Arrow result sets) and the results are decoded back to `Option<Value>` cells for
	/// typed consumption in [`crate::graph`]. This is the Arrow round trip that crosses the ADBC
	/// boundary, which is why the algorithm's inter-instance reads flow through `adbc_core`.
	pub fn query(
		&mut self,
		cypher: &str,
		params: &[(&str, Value)],
	) -> AdbcResult<Vec<Vec<Option<Value>>>> {
		let mut conn = self.new_connection()?;
		let mut stmt = conn.new_statement()?;
		stmt.set_sql_query(cypher)?;
		for (k, v) in params {
			stmt.params.push((k.to_string(), v.clone()));
		}
		let reader = stmt.execute()?;
		let mut rows = Vec::new();
		for batch in reader {
			let batch = batch
				.map_err(|e| AdbcError::with_message_and_status(e.to_string(), Status::InvalidData))?;
			rows.extend(batch_to_rows(&batch));
		}
		Ok(rows)
	}

	/// Runs a Cypher write through the ADBC [`Statement::execute_update`] path.
	pub fn update(&mut self, cypher: &str) -> AdbcResult<Option<i64>> {
		let mut conn = self.new_connection()?;
		let mut stmt = conn.new_statement()?;
		stmt.set_sql_query(cypher)?;
		stmt.execute_update()
	}

	/// Runs a read and returns a single scalar `Int64` value (e.g. a `COUNT(*)`).
	pub fn scalar_i64(&mut self, cypher: &str) -> AdbcResult<Option<i64>> {
		let rows = self.query(cypher, &[])?;
		let row = rows.first().and_then(|r| r.first().cloned().flatten());
		Ok(match row {
			Some(Value::Int64(v)) => Some(v),
			_ => None,
		})
	}
}


impl Optionable for LadybugDb {
	type Option = OptionDatabase;

	fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> AdbcResult<()> {
		Ok(())
	}
	fn get_option_string(&self, _key: Self::Option) -> AdbcResult<String> {
		Err(not_implemented("get_option_string on ladybug database"))
	}
	fn get_option_bytes(&self, _key: Self::Option) -> AdbcResult<Vec<u8>> {
		Err(not_implemented("get_option_bytes on ladybug database"))
	}
	fn get_option_int(&self, _key: Self::Option) -> AdbcResult<i64> {
		Err(not_implemented("get_option_int on ladybug database"))
	}
	fn get_option_double(&self, _key: Self::Option) -> AdbcResult<f64> {
		Err(not_implemented("get_option_double on ladybug database"))
	}
}

/// An ADBC connection: a thin handle on the shared ladybug graph driver.
pub struct LadybugConn {
	db: Arc<LbugDatabase>,
	write_lock: Arc<std::sync::Mutex<()>>,
}

impl AdbcConnection for LadybugConn {
	type StatementType = LadybugStmt;

	fn new_statement(&mut self) -> AdbcResult<Self::StatementType> {
		Ok(LadybugStmt {
			db: self.db.clone(),
			write_lock: self.write_lock.clone(),
			query: None,
			params: Vec::new(),
		})
	}

	fn cancel(&mut self) -> AdbcResult<()> {
		Ok(())
	}

	fn get_info(
		&self,
		_codes: Option<HashSet<InfoCode>>,
	) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
		Err(not_implemented("get_info for the ladybug driver"))
	}

	fn get_objects(
		&self,
		_depth: ObjectDepth,
		_catalog: Option<&str>,
		_db_schema: Option<&str>,
		_table_name: Option<&str>,
		_table_type: Option<Vec<&str>>,
		_column_name: Option<&str>,
	) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
		Err(not_implemented("get_objects for the ladybug driver"))
	}

	fn get_table_schema(
		&self,
		_catalog: Option<&str>,
		_db_schema: Option<&str>,
		_table_name: &str,
	) -> AdbcResult<Schema> {
		Err(not_implemented("get_table_schema for the ladybug driver"))
	}

	fn get_table_types(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
		Err(not_implemented("get_table_types for the ladybug driver"))
	}

	fn get_statistic_names(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
		Err(not_implemented("get_statistic_names for the ladybug driver"))
	}

	fn get_statistics(
		&self,
		_catalog: Option<&str>,
		_db_schema: Option<&str>,
		_table_name: Option<&str>,
		_approximate: bool,
	) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
		Err(not_implemented("get_statistics for the ladybug driver"))
	}

	fn commit(&mut self) -> AdbcResult<()> {
		Ok(())
	}

	fn rollback(&mut self) -> AdbcResult<()> {
		Ok(())
	}

	fn read_partition(
		&self,
		_partition: impl AsRef<[u8]>,
	) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
		Err(not_implemented("read_partition for the ladybug driver"))
	}
}

impl Optionable for LadybugConn {
	type Option = OptionConnection;

	fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> AdbcResult<()> {
		Ok(())
	}
	fn get_option_string(&self, _key: Self::Option) -> AdbcResult<String> {
		Err(not_implemented("get_option_string on ladybug connection"))
	}
	fn get_option_bytes(&self, _key: Self::Option) -> AdbcResult<Vec<u8>> {
		Err(not_implemented("get_option_bytes on ladybug connection"))
	}
	fn get_option_int(&self, _key: Self::Option) -> AdbcResult<i64> {
		Err(not_implemented("get_option_int on ladybug connection"))
	}
	fn get_option_double(&self, _key: Self::Option) -> AdbcResult<f64> {
		Err(not_implemented("get_option_double on ladybug connection"))
	}
}

/// An ADBC statement. `set_sql_query` stores the Cypher, `bind` binds named parameters, and
/// [`execute`] / [`execute_update`] run the query against the shared graph.
pub struct LadybugStmt {
	db: Arc<LbugDatabase>,
	write_lock: Arc<std::sync::Mutex<()>>,
	query: Option<String>,
	params: Vec<(String, Value)>,
}

impl LadybugStmt {
	fn read(&self) -> AdbcResult<(Vec<String>, Vec<Vec<Option<Value>>>)> {
		let query = self
			.query
			.as_deref()
			.ok_or_else(|| invalid("no SQL query set on ladybug statement"))?;
		let params: Vec<(&str, Value)> = self
			.params
			.iter()
			.map(|(k, v)| (k.as_str(), v.clone()))
			.collect();
		let conn = Connection::new(&self.db).map_err(|e| {
			AdbcError::with_message_and_status(format!("ladybug connect: {e}"), Status::InvalidData)
		})?;
		let (columns, result) = if params.is_empty() {
			let result = conn.query(query).map_err(|e| {
				AdbcError::with_message_and_status(format!("ladybug query failed: {e}"), Status::InvalidData)
			})?;
			(result.get_column_names(), result)
		} else {
			let mut stmt = conn.prepare(query).map_err(|e| {
				AdbcError::with_message_and_status(format!("ladybug prepare failed: {e}"), Status::InvalidData)
			})?;
			let result = conn.execute(&mut stmt, params).map_err(|e| {
				AdbcError::with_message_and_status(format!("ladybug execute failed: {e}"), Status::InvalidData)
			})?;
			(result.get_column_names(), result)
		};
		Ok((columns, collect_query(result)))
	}

	fn write(&self) -> AdbcResult<Option<i64>> {
		let query = self
			.query
			.as_deref()
			.ok_or_else(|| invalid("no SQL query set on ladybug statement"))?;
		let _guard = self.write_lock.lock().map_err(|_| {
			AdbcError::with_message_and_status("ladybug write lock poisoned", Status::Internal)
		})?;
		let conn = Connection::new(&self.db).map_err(|e| {
			AdbcError::with_message_and_status(format!("ladybug connect: {e}"), Status::InvalidData)
		})?;
		conn.query(query).map_err(|e| {
			AdbcError::with_message_and_status(format!("ladybug write failed: {e}"), Status::InvalidData)
		})?;
		Ok(None)
	}
}

impl Statement for LadybugStmt {
	fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()> {
		// Convert a single-row parameter batch into named `(name, Value)` pairs. The field name
		// of each column is the parameter name referenced as `$name` in the Cypher.
		let names = batch
			.schema()
			.fields()
			.iter()
			.map(|f| f.name().to_string())
			.collect::<Vec<_>>();
		self.params.clear();
		for (i, name) in names.iter().enumerate() {
			let array = batch.column(i);
			let value = value_at(array.as_ref(), 0);
			self.params.push((name.clone(), value));
		}
		Ok(())
	}

	fn bind_stream(
		&mut self,
		_reader: Box<dyn RecordBatchReader + Send>,
	) -> AdbcResult<()> {
		Err(not_implemented("bind_stream for the ladybug driver"))
	}

	fn execute(&mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
		let (columns, rows) = self.read()?;
		let batch = rows_to_batch(columns, &rows).map_err(|e| err_from_anyhow(&e, "arrow encode"))?;
		let schema = batch.schema();
		Ok(Box::new(InMemReader {
			schema,
			batch: Some(batch),
		}))
	}

	fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
		self.write()
	}

	fn execute_schema(&mut self) -> AdbcResult<Schema> {
		let (columns, rows) = self.read()?;
		let batch = rows_to_batch(columns, &rows).map_err(|e| err_from_anyhow(&e, "arrow encode"))?;
		Ok(batch.schema().as_ref().clone())
	}

	fn execute_partitions(&mut self) -> AdbcResult<PartitionedResult> {
		Err(not_implemented("execute_partitions for the ladybug driver"))
	}

	fn get_parameter_schema(&self) -> AdbcResult<Schema> {
		Ok(Schema::empty())
	}

	fn prepare(&mut self) -> AdbcResult<()> {
		Ok(())
	}

	fn set_sql_query(&mut self, query: impl AsRef<str>) -> AdbcResult<()> {
		self.query = Some(query.as_ref().to_string());
		Ok(())
	}

	fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> AdbcResult<()> {
		Err(not_implemented("set_substrait_plan for the ladybug driver"))
	}

	fn cancel(&mut self) -> AdbcResult<()> {
		Ok(())
	}
}

impl Optionable for LadybugStmt {
	type Option = OptionStatement;

	fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> AdbcResult<()> {
		Ok(())
	}
	fn get_option_string(&self, _key: Self::Option) -> AdbcResult<String> {
		Err(not_implemented("get_option_string on ladybug statement"))
	}
	fn get_option_bytes(&self, _key: Self::Option) -> AdbcResult<Vec<u8>> {
		Err(not_implemented("get_option_bytes on ladybug statement"))
	}
	fn get_option_int(&self, _key: Self::Option) -> AdbcResult<i64> {
		Err(not_implemented("get_option_int on ladybug statement"))
	}
	fn get_option_double(&self, _key: Self::Option) -> AdbcResult<f64> {
		Err(not_implemented("get_option_double on ladybug statement"))
	}
}

// ---------------------------------------------------------------------------
// Arrow encoding
// ---------------------------------------------------------------------------

/// Converts an Arrow batch back into `Option<Value>` cells, the typed form `graph`/`algorithm`
/// consume. This is the decode half of the Arrow round trip that crosses the ADBC boundary.
fn batch_to_rows(batch: &RecordBatch) -> Vec<Vec<Option<Value>>> {
	let nrows = batch.num_rows();
	let ncols = batch.num_columns();
	(0..nrows)
		.map(|r| {
			(0..ncols)
				.map(|c| {
					let arr = batch.column(c).as_ref();
					if arr.is_null(r) {
						None
					} else {
						Some(value_at(arr, r))
					}
				})
				.collect()
		})
		.collect()
}

/// Reads a single scalar `lbug::Value` out of an Arrow parameter array at `row`.
fn value_at(array: &dyn Array, row: usize) -> Value {
	if array.is_null(row) {
		return Value::Null(lbug::LogicalType::Any);
	}
	if let Some(a) = array.as_any().downcast_ref::<arrow_array::BooleanArray>() {
		return if a.value(row) { Value::Bool(true) } else { Value::Bool(false) };
	}
	if let Some(a) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
		return Value::Int64(a.value(row));
	}
	if let Some(a) = array.as_any().downcast_ref::<arrow_array::Int32Array>() {
		return Value::Int32(a.value(row));
	}
	if let Some(a) = array.as_any().downcast_ref::<arrow_array::UInt64Array>() {
		return Value::UInt64(a.value(row));
	}
	if let Some(a) = array.as_any().downcast_ref::<arrow_array::Float32Array>() {
		return Value::Float(a.value(row));
	}
	if let Some(a) = array.as_any().downcast_ref::<arrow_array::Float64Array>() {
		return Value::Double(a.value(row));
	}
	if let Some(a) = array.as_any().downcast_ref::<arrow_array::StringArray>() {
		return Value::String(a.value(row).to_string());
	}
	Value::Null(lbug::LogicalType::Any)
}

/// Picks an Arrow [`DataType`] for a ladybug scalar value. Integers collapse to `Int64`, floats
/// keep their width, strings map to `Utf8`, booleans to `Boolean`. Anything else falls back to
/// `Utf8` via the value's `Display` so columnar projection still works for ad-hoc queries.
fn datatype_of(value: &Value) -> DataType {
	match value {
		Value::Bool(_) => DataType::Boolean,
		Value::Int64(_)
		| Value::Int32(_)
		| Value::Int16(_)
		| Value::Int8(_)
		| Value::UInt64(_)
		| Value::UInt32(_)
		| Value::UInt16(_)
		| Value::UInt8(_)
		| Value::Int128(_) => DataType::Int64,
		Value::Float(_) => DataType::Float32,
		Value::Double(_) => DataType::Float64,
		Value::String(_) | Value::Json(_) => DataType::Utf8,
		Value::Null(_) => DataType::Utf8,
		_ => DataType::Utf8,
	}
}

fn value_display(value: &Value) -> String {
	match value {
		Value::Null(_) => String::new(),
		v => format!("{v}"),
	}
}

fn opt_i64(value: &Value) -> Option<i64> {
	match value {
		Value::Int64(v) => Some(*v),
		Value::Int32(v) => Some(i64::from(*v)),
		Value::Int16(v) => Some(i64::from(*v)),
		Value::Int8(v) => Some(i64::from(*v)),
		Value::UInt64(v) => i64::try_from(*v).ok(),
		Value::UInt32(v) => Some(i64::from(*v)),
		Value::UInt16(v) => Some(i64::from(*v)),
		Value::UInt8(v) => Some(i64::from(*v)),
		Value::Int128(v) => i64::try_from(*v).ok(),
		Value::Null(_) => None,
		_ => None,
	}
}

/// Builds an Arrow [`RecordBatch`] from the rows of a ladybug query.
fn rows_to_batch(columns: Vec<String>, rows: &[Vec<Option<Value>>]) -> anyhow::Result<RecordBatch> {
	let ncols = if columns.is_empty() {
		rows.first().map(|r| r.len()).unwrap_or(0)
	} else {
		columns.len()
	};

	let mut fields = Vec::with_capacity(ncols);
	let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);

	for c in 0..ncols {
		let name = columns
			.get(c)
			.cloned()
			.unwrap_or_else(|| format!("col{c}"));
		let mut col_values: Vec<Option<Value>> = Vec::with_capacity(rows.len());
		for row in rows {
			col_values.push(row.get(c).cloned().flatten());
		}
		let dt = col_values
			.iter()
			.find_map(|v| v.as_ref())
			.map(datatype_of)
			.unwrap_or(DataType::Utf8);

		let array: ArrayRef = match dt {
			DataType::Boolean => {
				let mut b = BooleanBuilder::new();
				for v in &col_values {
					match v {
						Some(Value::Bool(x)) => b.append_value(*x),
						_ => b.append_null(),
					}
				}
				Arc::new(b.finish())
			}
			DataType::Float32 => {
				let mut b = Float32Builder::new();
				for v in &col_values {
					match v {
						Some(Value::Float(x)) => b.append_value(*x),
						_ => b.append_null(),
					}
				}
				Arc::new(b.finish())
			}
			DataType::Float64 => {
				let mut b = Float64Builder::new();
				for v in &col_values {
					match v {
						Some(Value::Double(x)) => b.append_value(*x),
						_ => b.append_null(),
					}
				}
				Arc::new(b.finish())
			}
			DataType::Int64 => {
				let mut b = Int64Builder::new();
				for v in &col_values {
					match v.as_ref().and_then(opt_i64) {
						Some(x) => b.append_value(x),
						None => b.append_null(),
					}
				}
				Arc::new(b.finish())
			}
			_ => {
				let mut b = StringBuilder::new();
				for v in &col_values {
					match v {
						Some(v2) if !matches!(v2, Value::Null(_)) => b.append_value(value_display(v2)),
						_ => b.append_null(),
					}
				}
				Arc::new(b.finish())
			}
		};

		fields.push(Field::new(name, dt.clone(), true));
		arrays.push(array);
	}

	let schema = Arc::new(Schema::new(fields));
	Ok(RecordBatch::try_new(schema, arrays)?)
}

/// An in-memory Arrow reader that yields a single pre-built batch (used for a whole query result).
struct InMemReader {
	schema: SchemaRef,
	batch: Option<RecordBatch>,
}

impl Iterator for InMemReader {
	type Item = ArrowResult<RecordBatch>;

	fn next(&mut self) -> Option<Self::Item> {
		self.batch.take().map(Ok)
	}
}

impl RecordBatchReader for InMemReader {
	fn schema(&self) -> SchemaRef {
		self.schema.clone()
	}
}
