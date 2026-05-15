// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::BTreeMap;

use reifydb_type::value::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OutputKey(pub Vec<Value>);

impl OutputKey {
	pub fn new(values: Vec<Value>) -> Self {
		Self(values)
	}

	pub fn as_slice(&self) -> &[Value] {
		&self.0
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedRow {
	pub columns: BTreeMap<String, Value>,
}

impl MaterializedRow {
	pub fn new() -> Self {
		Self {
			columns: BTreeMap::new(),
		}
	}

	pub fn from_pairs<I, K>(pairs: I) -> Self
	where
		I: IntoIterator<Item = (K, Value)>,
		K: Into<String>,
	{
		Self {
			columns: pairs.into_iter().map(|(k, v)| (k.into(), v)).collect(),
		}
	}

	pub fn get(&self, name: &str) -> Option<&Value> {
		self.columns.get(name)
	}

	pub fn set(&mut self, name: impl Into<String>, value: Value) {
		self.columns.insert(name.into(), value);
	}
}

impl Default for MaterializedRow {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedTable {
	pub rows: BTreeMap<OutputKey, MaterializedRow>,
}

impl MaterializedTable {
	pub fn empty() -> Self {
		Self {
			rows: BTreeMap::new(),
		}
	}

	pub fn len(&self) -> usize {
		self.rows.len()
	}

	pub fn is_empty(&self) -> bool {
		self.rows.is_empty()
	}

	pub fn insert(&mut self, key: OutputKey, row: MaterializedRow) {
		self.rows.insert(key, row);
	}

	pub fn remove(&mut self, key: &OutputKey) -> Option<MaterializedRow> {
		self.rows.remove(key)
	}

	pub fn get(&self, key: &OutputKey) -> Option<&MaterializedRow> {
		self.rows.get(key)
	}
}

impl Default for MaterializedTable {
	fn default() -> Self {
		Self::empty()
	}
}
