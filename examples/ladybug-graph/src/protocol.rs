//! Wire protocol between the LadybugDB server and remote ADBC clients.
//!
//! The request is a small JSON envelope: the Cypher text plus named scalar bindings, typed so the
//! server rebinds them losslessly instead of guessing from JSON. The response to a read is the
//! Arrow IPC streaming format (`application/vnd.apache.arrow.stream`), the columnar encoding:
//! result sets cross the wire as Arrow buffers, not row-by-row JSON. Writes get a tiny JSON ack.
//!
//! One RPC endpoint (`POST /rpc`) carries both reads and writes; the `kind` field selects the
//! server execution path. `GET /health` is a liveness probe for demo scripts.

use std::collections::HashMap;

use lbug::Value;
use serde::{Deserialize, Serialize};

/// The MIME type of the Arrow IPC stream response body.
pub const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

/// Which execution path the server should take.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryKind {
	Query,
	Update,
}

/// The request envelope. `params` are named scalar bindings for prepared statements.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
	pub kind: QueryKind,
	pub cypher: String,
	#[serde(default)]
	pub params: HashMap<String, WireValue>,
}

/// A scalar parameter value, tagged with its ladybug type so the server rebuilds an exact
/// [`Value`] rather than inferring one from JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "snake_case")]
pub enum WireValue {
	Null,
	Bool(bool),
	Int64(i64),
	Int32(i32),
	UInt64(u64),
	Float(f32),
	Double(f64),
	String(String),
}

/// The write ack. `affected_rows` is reserved for drivers that report row counts; lbug does not
/// today, so clients receive `null`.
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateResponse {
	pub affected_rows: Option<i64>,
}

/// Encodes a ladybug value for the wire. The graph layer only ever binds scalars (through Arrow
/// parameter batches), so the supported set covers those; anything exotic degrades to a string.
pub fn value_to_wire(value: &Value) -> WireValue {
	match value {
		Value::Null(_) => WireValue::Null,
		Value::Bool(b) => WireValue::Bool(*b),
		Value::Int64(v) => WireValue::Int64(*v),
		Value::Int32(v) => WireValue::Int32(*v),
		Value::UInt64(v) => WireValue::UInt64(*v),
		Value::Float(v) => WireValue::Float(*v),
		Value::Double(v) => WireValue::Double(*v),
		Value::String(s) => WireValue::String(s.clone()),
		other => WireValue::String(other.to_string()),
	}
}

/// Decodes a wire value back into the ladybug type it was bound as.
pub fn wire_to_value(wire: &WireValue) -> Value {
	match wire {
		WireValue::Null => Value::Null(lbug::LogicalType::Any),
		WireValue::Bool(b) => Value::Bool(*b),
		WireValue::Int64(v) => Value::Int64(*v),
		WireValue::Int32(v) => Value::Int32(*v),
		WireValue::UInt64(v) => Value::UInt64(*v),
		WireValue::Float(v) => Value::Float(*v),
		WireValue::Double(v) => Value::Double(*v),
		WireValue::String(s) => Value::String(s.clone()),
	}
}
