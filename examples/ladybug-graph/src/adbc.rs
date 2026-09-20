//! ADBC (Arrow Database Connectivity) bridge over the remote LadybugDB server.
//!
//! The four ADBC traits (`Driver`, `Database`, `Connection`, `Statement`) are implemented as a
//! client of the columnar RPC protocol served by [`crate::ladybug_server`]: reads POST a
//! `Query` request and stream back an Arrow IPC result, writes POST an `Update`. This is the
//! inter-instance data plane for the platform. Distributed graph workers that run on different
//! machines hold their own ADBC connection to the same server-owned store and exchange messages
//! through it, exactly as they did with the old in-process store, but now with no shared file and
//! no per-process write lock: the server is the single writer.
//!
//! The wire format keeps results columnar end to end. The server encodes rows as an Arrow IPC
//! stream (`application/vnd.apache.arrow.stream`) and [`LadybugStmt::execute`] decodes
//! `RecordBatch`es directly off that stream. Values only degrade to row-major form at the typed
//! [`crate::graph`] layer when it asks for `Vec<Vec<Option<Value>>>`.
//!
//! The ADBC traits are implemented for "lifecycle + supported surface" parity, mirroring the
//! philosophy of the underlying driver: the operations the platform uses are fully implemented,
//! and the rest fail with an explicit `NotImplemented` error rather than silently misbehaving.

use std::{
	collections::{HashMap, HashSet},
	io::Cursor,
};

use adbc_core::{
	PartitionedResult,
	error::{Error as AdbcError, Result as AdbcResult, Status},
	options::{
		InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
	},
	sync::{Connection as AdbcConnection, Database as AdbcDatabase, Driver, Optionable, Statement},
};
use arrow_array::{
	Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
	RecordBatchReader, StringArray, UInt64Array,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use lbug::{LogicalType, Value};

use crate::protocol::{QueryKind, QueryRequest, UpdateResponse, value_to_wire};

/// A convenience alias for `std::result::Result<T, arrow_schema::ArrowError>`.
type ArrowResult<T> = std::result::Result<T, ArrowError>;

fn not_implemented(msg: impl Into<String>) -> AdbcError {
	AdbcError::with_message_and_status(msg.into(), Status::NotImplemented)
}

fn invalid(msg: impl Into<String>) -> AdbcError {
	AdbcError::with_message_and_status(msg.into(), Status::InvalidArguments)
}

/// Builds a shared blocking HTTP client off-thread. `reqwest::blocking` creates an internal tokio
/// runtime, and dropping that runtime inside an async runtime context panics; the driver is opened
/// from async actor handlers, so construct it on a plain thread instead.
fn blocking_client() -> reqwest::blocking::Client {
	std::thread::spawn(reqwest::blocking::Client::new)
		.join()
		.expect("spawn reqwest blocking client thread")
}

/// The ADBC driver for the remote LadybugDB server, configured with the server base URL.
#[derive(Clone, Debug)]
pub struct LadybugDriver {
	url: String,
}

impl LadybugDriver {
	/// A driver rooted at a LadybugDB server URL (for example `http://127.0.0.1:8123`).
	pub fn new(url: impl Into<String>) -> Self {
		LadybugDriver { url: url.into() }
	}
}

impl Driver for LadybugDriver {
	type DatabaseType = LadybugDb;

	fn new_database(&mut self) -> AdbcResult<Self::DatabaseType> {
		Ok(LadybugDb {
			client: blocking_client(),
			url: self.url.clone(),
		})
	}

	fn new_database_with_opts(
		&mut self,
		opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
	) -> AdbcResult<Self::DatabaseType> {
		let mut url = self.url.clone();
		for (key, value) in opts {
			if key == OptionDatabase::Uri {
				url = match value {
					OptionValue::String(s) => s,
					other => {
						return Err(invalid(format!(
							"expected a string Uri for the ladybug driver, got {other:?}"
						)));
					}
				};
			}
		}
		Ok(LadybugDb {
			client: blocking_client(),
			url,
		})
	}
}

/// An ADBC database: a shared HTTP client plus the server base URL, so every connection and
/// statement reuses the same pooled TCP connection to the server-owned store.
pub struct LadybugDb {
	client: reqwest::blocking::Client,
	url: String,
}

impl AdbcDatabase for LadybugDb {
	type ConnectionType = LadybugConn;

	fn new_connection(&self) -> AdbcResult<Self::ConnectionType> {
		Ok(LadybugConn {
			client: self.client.clone(),
			url: self.url.clone(),
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
	/// Runs a Cypher read through the ADBC [`Statement::execute`] path. The server streams an
	/// Arrow IPC result which is decoded back to `Option<Value>` cells for typed consumption in
	/// [`crate::graph`].
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
		stmt.read()
	}

	/// Runs a Cypher write through the ADBC [`Statement::execute_update`] path.
	pub fn update(&mut self, cypher: &str) -> AdbcResult<Option<i64>> {
		self.update_params(cypher, &[])
	}

	/// Runs a parameterized Cypher write through the ADBC [`Statement::execute_update`] path.
	pub fn update_params(
		&mut self,
		cypher: &str,
		params: &[(&str, Value)],
	) -> AdbcResult<Option<i64>> {
		let mut conn = self.new_connection()?;
		let mut stmt = conn.new_statement()?;
		stmt.set_sql_query(cypher)?;
		for (k, v) in params {
			stmt.params.push((k.to_string(), v.clone()));
		}
		stmt.execute_update()
	}

	/// Runs a read and returns a single scalar `Int64` value (for example a `COUNT(*)`).
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

/// An ADBC connection: a thin handle sharing the database's HTTP client and server URL.
pub struct LadybugConn {
	client: reqwest::blocking::Client,
	url: String,
}

impl AdbcConnection for LadybugConn {
	type StatementType = LadybugStmt;

	fn new_statement(&mut self) -> AdbcResult<Self::StatementType> {
		Ok(LadybugStmt {
			client: self.client.clone(),
			url: self.url.clone(),
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
		Err(not_implemented(
			"get_statistic_names for the ladybug driver",
		))
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
/// [`execute`](Statement::execute) / [`execute_update`](Statement::execute_update) run the query
/// against the remote server.
pub struct LadybugStmt {
	client: reqwest::blocking::Client,
	url: String,
	query: Option<String>,
	params: Vec<(String, Value)>,
}

impl LadybugStmt {
	/// POSTs the statement to the server and returns the decoded response.
	fn send(&self, kind: QueryKind) -> AdbcResult<reqwest::blocking::Response> {
		let query = self
			.query
			.as_deref()
			.ok_or_else(|| invalid("no SQL query set on ladybug statement"))?;
		let params: HashMap<String, crate::protocol::WireValue> = self
			.params
			.iter()
			.map(|(k, v)| (k.clone(), value_to_wire(v)))
			.collect();
		let request = QueryRequest {
			kind,
			cypher: query.to_string(),
			params,
		};
		let response = self
			.client
			.post(format!("{}/rpc", self.url.trim_end_matches('/')))
			.json(&request)
			.send()
			.map_err(|e| {
				AdbcError::with_message_and_status(
					format!("ladybug rpc failed: {e}"),
					Status::InvalidData,
				)
			})?;
		let status = response.status();
		if !status.is_success() {
			let body: serde_json::Value = response.json().unwrap_or_else(|_| serde_json::json!({}));
			let message = body
				.get("error")
				.and_then(|e| e.as_str())
				.unwrap_or("unknown server error");
			return Err(AdbcError::with_message_and_status(
				format!("ladybug server {status}: {message}"),
				Status::InvalidData,
			));
		}
		Ok(response)
	}

	/// Runs the read and decodes the Arrow IPC stream into row-major `Option<Value>` cells.
	fn read(&self) -> AdbcResult<Vec<Vec<Option<Value>>>> {
		let response = self.send(QueryKind::Query)?;
		let bytes = response.bytes().map_err(|e| {
			AdbcError::with_message_and_status(
				format!("read ladybug result body: {e}"),
				Status::InvalidData,
			)
		})?;
		let stream = StreamReader::try_new(Cursor::new(bytes.to_vec()), None).map_err(|e| {
			AdbcError::with_message_and_status(
				format!("decode arrow stream: {e}"),
				Status::InvalidData,
			)
		})?;
		let mut rows = Vec::new();
		for batch in stream {
			let batch = batch.map_err(|e| {
				AdbcError::with_message_and_status(e.to_string(), Status::InvalidData)
			})?;
			rows.extend(batch_to_rows(&batch));
		}
		Ok(rows)
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

	fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
		Err(not_implemented("bind_stream for the ladybug driver"))
	}

	fn execute(&mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
		let response = self.send(QueryKind::Query)?;
		let bytes = response.bytes().map_err(|e| {
			AdbcError::with_message_and_status(
				format!("read ladybug result body: {e}"),
				Status::InvalidData,
			)
		})?;
		let stream = StreamReader::try_new(Cursor::new(bytes.to_vec()), None).map_err(|e| {
			AdbcError::with_message_and_status(
				format!("decode arrow stream: {e}"),
				Status::InvalidData,
			)
		})?;
		Ok(Box::new(RemoteStream { stream }))
	}

	fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
		let response = self.send(QueryKind::Update)?;
		let update: UpdateResponse = response.json().map_err(|e| {
			AdbcError::with_message_and_status(
				format!("decode update ack: {e}"),
				Status::InvalidData,
			)
		})?;
		Ok(update.affected_rows)
	}

	fn execute_schema(&mut self) -> AdbcResult<Schema> {
		let response = self.send(QueryKind::Query)?;
		let bytes = response.bytes().map_err(|e| {
			AdbcError::with_message_and_status(
				format!("read ladybug result body: {e}"),
				Status::InvalidData,
			)
		})?;
		let stream = StreamReader::try_new(Cursor::new(bytes.to_vec()), None).map_err(|e| {
			AdbcError::with_message_and_status(
				format!("decode arrow stream: {e}"),
				Status::InvalidData,
			)
		})?;
		Ok(stream.schema().as_ref().clone())
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

/// Streams the `RecordBatch`es of a server response body directly off the Arrow IPC wire.
struct RemoteStream {
	stream: StreamReader<Cursor<Vec<u8>>>,
}

impl Iterator for RemoteStream {
	type Item = ArrowResult<RecordBatch>;

	fn next(&mut self) -> Option<Self::Item> {
		self.stream.next()
	}
}

impl RecordBatchReader for RemoteStream {
	fn schema(&self) -> SchemaRef {
		self.stream.schema()
	}
}

// ---------------------------------------------------------------------------
// Arrow decoding
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
		return Value::Null(LogicalType::Any);
	}
	if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
		return if a.value(row) {
			Value::Bool(true)
		} else {
			Value::Bool(false)
		};
	}
	if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
		return Value::Int64(a.value(row));
	}
	if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
		return Value::Int32(a.value(row));
	}
	if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
		return Value::UInt64(a.value(row));
	}
	if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
		return Value::Float(a.value(row));
	}
	if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
		return Value::Double(a.value(row));
	}
	if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
		return Value::String(a.value(row).to_string());
	}
	Value::Null(LogicalType::Any)
}
