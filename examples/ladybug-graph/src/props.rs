//! Strongly typed property construction for the LadybugDB graph store.
//!
//! LadybugDB's node/relationship creators take a `Vec<(String, lbug::Value)>` of property
//! name/value pairs. Hand-rolling that vector is error prone: property names are string literals
//! with no cross-checking against the table schema, and values must be wrapped in the correct
//! `lbug::Value` variant by hand.
//!
//! This module provides two layers that remove those failure modes:
//!
//! 1. A [`Properties`] builder plus the [`props!`](crate::props) macro. `true`, `3`, `"x"` and
//!    any other `IntoValue` type convert without wrapping, so the least-error is the next most
//!    likely to compile, and `validate` can cross-check the result against a declared
//!    [`Table`] schema at runtime (catching typos and type drift instead of corrupting the graph).
//! 2. A [`TypedProps`] builder keyed off a [`Table`] schema. It knows the column names and types
//!    up front, so a wrong key is a runtime error and a wrong value type is caught by
//!    [`Table::check`] / [`PropHolder::checkagainst`] before anything reaches the database.
//!
//! The distributed algorithms in [`crate::algorithm`] use these builders, so the graph rows they
//! persist (`Vertex`, `Msg`, `Run`) are produced from typed application objects rather than raw
//! `Vec<(String, Value)>`.

use std::collections::HashSet;

/// Builds a [`Properties`] from `name => value` pairs without hand-wrapping values into
/// `lbug::Value`.
///
/// ```
/// use example_ladybug_graph::props;
/// let p = example_ladybug_graph::props! { "id" => 1i64, "name" => "Alice".to_string(), "enabled" => true };
/// ```
#[macro_export]
macro_rules! props {
	( $( $key:expr => $value:expr ),* $(,)? ) => {{
		let mut __props = $crate::props::Properties::new();
		$( __props = __props.set($key, $value); )*
		__props
	}};
}

/// A scalar type a LadybugDB column can hold. Mirrors the scalar types the graph store persists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColType {
	Int64,
	Int32,
	UInt64,
	Double,
	String,
	Bool,
}

impl ColType {
	fn check(&self, value: &lbug::Value) -> bool {
		match (self, value) {
			(ColType::Int64, lbug::Value::Int64(_)) => true,
			(ColType::Int32, lbug::Value::Int32(_)) => true,
			(ColType::UInt64, lbug::Value::UInt64(_)) => true,
			(ColType::Double, lbug::Value::Double(_)) => true,
			(ColType::String, lbug::Value::String(_)) => true,
			(ColType::Bool, lbug::Value::Bool(_)) => true,
			_ => false,
		}
	}
}

/// A declared node/rel table schema. The platform loads "application code" as a set of typed
/// tables like this, then creates and queries rows only through the matching [`TypedProps`]
/// builder, so a mismatch between a live row and its declaration is caught instead of silently
/// persisted.
#[derive(Clone, Debug)]
pub struct Table {
	/// The table label used in `CREATE NODE TABLE` / `CREATE` statements.
	pub label: String,
	/// Ordered column name/type pairs, exactly as declared in the DDL.
	pub cols: Vec<(String, ColType)>,
}

impl Table {
	pub fn new(label: impl Into<String>, cols: Vec<(&'static str, ColType)>) -> Self {
		Table {
			label: label.into(),
			cols: cols.into_iter().map(|(n, t)| (n.to_string(), t)).collect(),
		}
	}

	/// The column type for a name, if this table declares it.
	pub fn col_type(&self, name: &str) -> Option<ColType> {
		self.cols.iter().find(|(n, _)| n == name).map(|(_, t)| *t)
	}

	pub fn contains(&self, name: &str) -> bool {
		self.cols.iter().any(|(n, _)| n == name)
	}

	/// Returns an error listing every property name that is not a column of this table. This turns
	/// a typo in a property key into a loud, local failure instead of a wrong column in the graph.
	pub fn unknown_keys(&self, props: &[(String, lbug::Value)]) -> Vec<String> {
		props
			.iter()
			.filter(|(k, _)| !self.contains(k))
			.map(|(k, _)| k.clone())
			.collect()
	}

	/// Checks every value against the declared column type, returning the first mismatch.
	pub fn check_values<'a>(
		&self,
		props: &'a [(String, lbug::Value)],
	) -> Result<(), anyhow::Error> {
		for (name, value) in props {
			match self.col_type(name) {
				None => anyhow::bail!(
					"property `{name}` is not a column of table `{}`",
					self.label
				),
				Some(ct) if !ct.check(value) => {
					anyhow::bail!(
						"property `{name}` of table `{}` has value {value:?} which is not of type {ct:?}",
						self.label
					)
				}
				_ => {}
			}
		}
		Ok(())
	}
}

/// Values that can be stored as a LadybugDB scalar property without hand-wrapping into
/// `lbug::Value`. Numerics and strings convert via `lbug::Value: From<_>`; `bool` is handled here
/// because the `lbug` crate does not expose a `From<bool>`.
pub trait IntoValue {
	fn into_lbug_value(self) -> lbug::Value;
}

macro_rules! impl_into_value_via_from {
	($($t:ty),* $(,)?) => {$(
		impl IntoValue for $t {
			fn into_lbug_value(self) -> lbug::Value {
				lbug::Value::from(self)
			}
		}
	)*};
}
impl_into_value_via_from!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, String,);
impl IntoValue for &str {
	fn into_lbug_value(self) -> lbug::Value {
		lbug::Value::String(self.to_string())
	}
}
impl IntoValue for bool {
	fn into_lbug_value(self) -> lbug::Value {
		lbug::Value::Bool(self)
	}
}
impl IntoValue for lbug::Value {
	fn into_lbug_value(self) -> lbug::Value {
		self
	}
}

/// A table-name scoped builder that only accepts keys the [`Table`] declares.
///
/// Wrong keys fail eagerly (when [`TypedProps::try_set`] is used) and wrong value types are
/// rejected by [`TypedProps::build`] against the declared schema. This is the "strong types" half
/// of loading arbitrary application data into the graph: the schema is declared once and every
/// row produced by the application is checked against it.
#[derive(Clone, Debug)]
pub struct TypedProps {
	table: Table,
	kv: Vec<(String, lbug::Value)>,
	seen: HashSet<String>,
}

impl TypedProps {
	pub fn new(table: Table) -> Self {
		TypedProps {
			table,
			kv: Vec::new(),
			seen: HashSet::new(),
		}
	}

	/// Sets a property, verifying the key is a declared column.
	pub fn try_set(mut self, key: &str, value: impl IntoValue) -> Result<Self, anyhow::Error> {
		if !self.table.contains(key) {
			anyhow::bail!(
				"property `{key}` is not a column of table `{}` (declared: {:?})",
				self.table.label,
				self.table.cols.iter().map(|(n, _)| n).collect::<Vec<_>>()
			);
		}
		if self.seen.contains(key) {
			anyhow::bail!("property `{key}` set twice on table `{}`", self.table.label);
		}
		self.seen.insert(key.to_string());
		self.kv.push((key.to_string(), value.into_lbug_value()));
		Ok(self)
	}

	/// Validates all values against the declared schema and returns the property vector.
	pub fn build(self) -> Result<Vec<(String, lbug::Value)>, anyhow::Error> {
		self.table.check_values(&self.kv)?;
		Ok(self.kv)
	}
}

/// A free-form (unscoped) property builder. No schema is known up front, so `build` does not
/// type-check; use [`validate`](Properties::validate) when a [`Table`] is available.
#[derive(Clone, Debug, Default)]
pub struct Properties {
	kv: Vec<(String, lbug::Value)>,
}

impl Properties {
	pub fn new() -> Self {
		Properties { kv: Vec::new() }
	}

	pub fn set(mut self, key: &str, value: impl IntoValue) -> Self {
		self.kv.push((key.to_string(), value.into_lbug_value()));
		self
	}

	pub fn is_empty(&self) -> bool {
		self.kv.is_empty()
	}

	pub fn push(&mut self, key: &str, value: impl IntoValue) {
		self.kv.push((key.to_string(), value.into_lbug_value()));
	}

	pub fn as_slice(&self) -> &[(String, lbug::Value)] {
		&self.kv
	}

	pub fn into_vec(self) -> Vec<(String, lbug::Value)> {
		self.kv
	}

	/// Cross-checks the current properties against a declared schema without consuming `self`.
	pub fn validate(&self, table: &Table) -> Result<(), anyhow::Error> {
		table.check_values(&self.kv)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn props_macro_builds_typed_values() {
		let p = crate::props! { "id" => 1i64, "name" => "Alice".to_string(), "in_core" => true };
		assert_eq!(p.as_slice().len(), 3);
		// Values are pre-wrapped; no `Value::Int64(...)` noise in application code.
		assert!(matches!(&p.as_slice()[0].1, lbug::Value::Int64(1)));
		assert!(matches!(&p.as_slice()[2].1, lbug::Value::Bool(true)));
	}

	#[test]
	fn table_rejects_unknown_keys() {
		let t = Table::new("Vertex", vec![("id", ColType::Int64)]);
		let p = crate::props! { "id" => 1i64, "idd" => 2i64 };
		let unknown = t.unknown_keys(p.as_slice());
		assert_eq!(unknown, vec!["idd".to_string()]);
		assert!(t.check_values(p.as_slice()).is_err());
	}

	#[test]
	fn table_rejects_wrong_type() {
		let t = Table::new("Vertex", vec![("id", ColType::Int64)]);
		let p = crate::props! { "id" => "oops".to_string() };
		assert!(t.check_values(p.as_slice()).is_err());
	}

	#[test]
	fn typed_props_enforces_schema() {
		let t = Table::new(
			"Vertex",
			vec![
				("id", ColType::Int64),
				("name", ColType::String),
				("in_core", ColType::Bool),
			],
		);
		let good = TypedProps::new(t.clone())
			.try_set("id", 1i64)
			.unwrap()
			.try_set("name", "Bob".to_string())
			.unwrap()
			.try_set("in_core", true)
			.unwrap()
			.build()
			.unwrap();
		assert_eq!(good.len(), 3);

		// Unknown key is rejected eagerly.
		assert!(TypedProps::new(t.clone()).try_set("idd", 1i64).is_err());
		// Wrong value type is rejected at build time.
		let bad = TypedProps::new(t.clone())
			.try_set("id", 1i64)
			.unwrap()
			.try_set("name", "x".to_string())
			.unwrap()
			.try_set("in_core", "not a bool".to_string());
		let bad = bad.expect("key is valid; type mismatch should fail at build");
		assert!(bad.build().is_err());
	}
}
