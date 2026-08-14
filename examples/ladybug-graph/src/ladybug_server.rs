//! The LadybugDB server side of the columnar ADBC RPC protocol.
//!
//! A single process owns the embedded graph file (the one writer the embedded engine allows) and
//! serves any number of remote clients over the wire protocol in [`crate::protocol`]. Reads run
//! concurrently (the engine synchronizes connections internally); writes are serialized behind one
//! process-wide lock, which is exactly the single-writer guarantee the embedded engine requires.
//! Result sets are encoded as Arrow IPC streams, so they stay columnar across the wire instead of
//! paying row-by-row JSON overhead on either side.
//!
//! [`LadybugServer::open`] opens the store and [`LadybugServer::app`] builds the axum router.
//! [`ServerHandle::start`] binds an ephemeral port on a background thread and returns a handle
//! that tests and the standalone demo use to stand up a real server in-process.

use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::{Context, Result, anyhow};
use arrow_array::{
	ArrayRef, RecordBatch,
	builder::{BooleanBuilder, Float32Builder, Float64Builder, Int64Builder, StringBuilder},
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use axum::{
	Json, Router,
	body::Body,
	extract::State,
	http::{StatusCode, header::CONTENT_TYPE},
	response::{IntoResponse, Response},
	routing::{get, post},
};
use lbug::{Connection, Database as LbugDatabase, SystemConfig, Value};
use serde_json::json;
use tokio::sync::oneshot;

use crate::protocol::{
	ARROW_STREAM_CONTENT_TYPE, QueryKind, QueryRequest, UpdateResponse, WireValue, wire_to_value,
};

/// The shared server state handed to every handler: the store handle plus the write lock that
/// serializes writers (the embedded engine allows one write in flight per process).
#[derive(Clone)]
struct AppState {
	db: Arc<LbugDatabase>,
	write_lock: Arc<std::sync::Mutex<()>>,
}

/// The LadybugDB server: owns the graph file and executes remote Cypher for all clients.
pub struct LadybugServer {
	db: Arc<LbugDatabase>,
	write_lock: Arc<std::sync::Mutex<()>>,
}

impl LadybugServer {
	/// Opens (or creates) the durable graph store at `path`. Pass `:memory:` for a throwaway
	/// in-memory store (lbug treats it the same as `Database::in_memory`).
	pub fn open(path: impl AsRef<Path>) -> Result<Self> {
		let path = path.as_ref();
		let db = LbugDatabase::new(path, SystemConfig::default())
			.map_err(|e| anyhow!("open ladybug database at {}: {e}", path.display()))?;
		Ok(LadybugServer {
			db: Arc::new(db),
			write_lock: Arc::new(std::sync::Mutex::new(())),
		})
	}

	/// The axum router serving `POST /rpc` and `GET /health`.
	pub fn app(&self) -> Router {
		Router::new()
			.route("/rpc", post(rpc))
			.route("/health", get(health))
			.with_state(AppState {
				db: self.db.clone(),
				write_lock: self.write_lock.clone(),
			})
	}
}

/// A running server on a background thread, bound to an ephemeral port. Dropping the handle
/// signals graceful shutdown and joins the thread.
pub struct ServerHandle {
	/// Base URL clients connect to (for example `http://127.0.0.1:54321`).
	pub url: String,
	shutdown: Option<oneshot::Sender<()>>,
	join: Option<std::thread::JoinHandle<()>>,
}

impl ServerHandle {
	/// Binds an ephemeral loopback port and serves `server` on a background thread. The port is
	/// reserved synchronously before the thread starts so the URL is known immediately.
	pub fn start(server: LadybugServer) -> Result<Self> {
		let std_listener = std::net::TcpListener::bind("127.0.0.1:0")
			.context("bind ephemeral ladybug server port")?;
		let addr = std_listener.local_addr().context("read bound address")?;
		let (shutdown, rx) = oneshot::channel();
		let app = server.app();
		let join = std::thread::spawn(move || {
			let rt = tokio::runtime::Builder::new_multi_thread()
				.enable_all()
				.build()
				.expect("build server tokio runtime");
			rt.block_on(async move {
				let _ = std_listener.set_nonblocking(true);
				let listener = tokio::net::TcpListener::from_std(std_listener)
					.expect("convert ladybug server listener");
				axum::serve(listener, app)
					.with_graceful_shutdown(async move {
						let _ = rx.await;
					})
					.await
					.expect("serve ladybug rpc");
			});
		});
		Ok(ServerHandle {
			url: format!("http://{addr}"),
			shutdown: Some(shutdown),
			join: Some(join),
		})
	}
}

impl Drop for ServerHandle {
	fn drop(&mut self) {
		if let Some(shutdown) = self.shutdown.take() {
			let _ = shutdown.send(());
		}
		if let Some(join) = self.join.take() {
			let _ = join.join();
		}
	}
}

async fn health() -> &'static str {
	"ok"
}

/// A row-major query result: one `Option<Value>` cell per column, per row (nulls keep their
/// `Value::Null` type).
type Cells = Vec<Vec<Option<Value>>>;

/// Executes a read (a `MATCH .. RETURN`) and returns the column names plus row-major value cells.
fn run_query(
	db: &LbugDatabase,
	cypher: &str,
	params: &HashMap<String, WireValue>,
) -> Result<(Vec<String>, Cells)> {
	let conn = Connection::new(db).context("ladybug connect")?;
	if params.is_empty() {
		let result = conn.query(cypher).context("ladybug query failed")?;
		Ok((result.get_column_names(), collect_query(result)))
	} else {
		let mut stmt = conn.prepare(cypher).context("ladybug prepare failed")?;
		let params: Vec<(&str, Value)> = params
			.iter()
			.map(|(k, v)| (k.as_str(), wire_to_value(v)))
			.collect();
		let result = conn
			.execute(&mut stmt, params)
			.context("ladybug execute failed")?;
		Ok((result.get_column_names(), collect_query(result)))
	}
}

/// Executes a write under the single-writer lock and reports an optional affected-row count.
fn run_update(
	db: &LbugDatabase,
	write_lock: &std::sync::Mutex<()>,
	cypher: &str,
	params: &HashMap<String, WireValue>,
) -> Result<Option<i64>> {
	let _guard = write_lock
		.lock()
		.map_err(|_| anyhow!("ladybug write lock poisoned"))?;
	let conn = Connection::new(db).context("ladybug connect")?;
	if params.is_empty() {
		conn.query(cypher).context("ladybug write failed")?;
	} else {
		let mut stmt = conn.prepare(cypher).context("ladybug prepare failed")?;
		let params: Vec<(&str, Value)> = params
			.iter()
			.map(|(k, v)| (k.as_str(), wire_to_value(v)))
			.collect();
		conn.execute(&mut stmt, params)
			.context("ladybug write failed")?;
	}
	Ok(None)
}

/// A JSON error body carrying the server-side message, with an HTTP status that distinguishes
/// client query errors (400) from server faults (500).
struct ApiError {
	status: StatusCode,
	message: String,
}

impl ApiError {
	fn query(error: anyhow::Error) -> Self {
		ApiError {
			status: StatusCode::BAD_REQUEST,
			message: format!("{error:#}"),
		}
	}

	fn internal(error: anyhow::Error) -> Self {
		ApiError {
			status: StatusCode::INTERNAL_SERVER_ERROR,
			message: format!("{error:#}"),
		}
	}
}

impl IntoResponse for ApiError {
	fn into_response(self) -> Response {
		(self.status, Json(json!({ "error": self.message }))).into_response()
	}
}

/// The one RPC entry point: `Query` streams back an Arrow IPC body, `Update` a JSON ack.
async fn rpc(
	State(state): State<AppState>,
	Json(req): Json<QueryRequest>,
) -> Result<Response, ApiError> {
	let db = state.db.clone();
	let write_lock = state.write_lock.clone();
	match req.kind {
		QueryKind::Query => {
			let cypher = req.cypher.clone();
			let params = req.params.clone();
			let (columns, rows) =
				tokio::task::spawn_blocking(move || run_query(&db, &cypher, &params))
					.await
					.map_err(|e| ApiError::internal(anyhow!("query task join: {e}")))?
					.map_err(ApiError::query)?;
			let batch = rows_to_batch(columns, &rows).map_err(ApiError::internal)?;
			let bytes = ipc_stream_bytes(&batch)
				.map_err(|e| ApiError::internal(anyhow!("encode arrow stream: {e}")))?;
			Response::builder()
				.header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
				.body(Body::from(bytes))
				.map_err(|e| ApiError::internal(anyhow!("build arrow response: {e}")))
		}
		QueryKind::Update => {
			let cypher = req.cypher.clone();
			let params = req.params.clone();
			let affected =
				tokio::task::spawn_blocking(move || run_update(&db, &write_lock, &cypher, &params))
					.await
					.map_err(|e| ApiError::internal(anyhow!("update task join: {e}")))?
					.map_err(ApiError::query)?;
			Ok(Json(UpdateResponse {
				affected_rows: affected,
			})
			.into_response())
		}
	}
}

// ---------------------------------------------------------------------------
// Arrow encoding
// ---------------------------------------------------------------------------

/// Collects a lbug query result into `Option<Value>` cells (all rows present; nulls remain as
/// `Some(Value::Null(_))`).
fn collect_query(result: impl IntoIterator<Item = Vec<Value>>) -> Vec<Vec<Option<Value>>> {
	result
		.into_iter()
		.map(|row| row.into_iter().map(Some).collect())
		.collect()
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
		let name = columns.get(c).cloned().unwrap_or_else(|| format!("col{c}"));
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
						Some(v2) if !matches!(v2, Value::Null(_)) => {
							b.append_value(value_display(v2))
						}
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

/// Encodes one result batch as an Arrow IPC stream (schema, records, end-of-stream marker).
fn ipc_stream_bytes(batch: &RecordBatch) -> std::result::Result<Vec<u8>, ArrowError> {
	let mut buf = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())?;
		writer.write(batch)?;
		writer.finish()?;
	}
	Ok(buf)
}
