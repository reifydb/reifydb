// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	interface::change::{Change, Diff},
	value::column::columns::Columns,
};
use reifydb_value::value::{Value, row_number::RowNumber};

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
pub struct MaterializedView {
	pub rows: BTreeMap<OutputKey, MaterializedRow>,
	pub columns: Vec<String>,

	pub incoherent: Vec<String>,
}

impl MaterializedView {
	pub fn empty() -> Self {
		Self {
			rows: BTreeMap::new(),
			columns: Vec::new(),
			incoherent: Vec::new(),
		}
	}

	pub fn fold(&mut self, change: &Change) {
		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					for (key, row) in self.rows_of(post) {
						if self.rows.insert(key.clone(), row).is_some() {
							self.incoherent.push(format!(
								"insert of row {key:?} that already existed"
							));
						}
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					for (key, _) in self.rows_of(pre) {
						if self.rows.remove(&key).is_none() {
							self.incoherent.push(format!(
								"update whose pre row {key:?} is absent"
							));
						}
					}
					for (key, row) in self.rows_of(post) {
						self.rows.insert(key, row);
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					for (key, _) in self.rows_of(pre) {
						if self.rows.remove(&key).is_none() {
							self.incoherent.push(format!("remove of absent row {key:?}"));
						}
					}
				}
			}
		}
	}

	fn rows_of(&mut self, columns: &Columns) -> Vec<(OutputKey, MaterializedRow)> {
		let names: Vec<String> = columns.iter().map(|c| c.name().text().to_string()).collect();
		if self.columns.is_empty() {
			self.columns = names.clone();
		}
		(0..columns.row_count())
			.map(|i| {
				let number: RowNumber = columns.row_numbers()[i];
				let row = MaterializedRow::from_pairs(names.iter().cloned().zip(columns.row(i)));
				(OutputKey::new(vec![Value::Uint8(number.0.into())]), row)
			})
			.collect()
	}

	pub fn rekey(&self, key_columns: &[String]) -> MaterializedView {
		let mut out = MaterializedView::empty();
		out.columns = self.columns.clone();
		for row in self.rows.values() {
			let key = OutputKey::new(
				key_columns
					.iter()
					.map(|name| row.get(name).cloned().unwrap_or(Value::Boolean(false)))
					.collect(),
			);
			out.insert(key, row.clone());
		}
		out
	}

	pub fn projected(&self, indices: &[usize]) -> Vec<Vec<Value>> {
		let mut out: Vec<Vec<Value>> = self
			.rows
			.values()
			.map(|row| {
				indices.iter()
					.map(|i| {
						let name = self.columns.get(*i).unwrap_or_else(|| {
							panic!(
								"projection asked for column {i} but the operator only \
								 published {}: {:?}",
								self.columns.len(),
								self.columns
							)
						});
						row.get(name)
							.unwrap_or_else(|| {
								panic!("column {name:?} is absent from a published row")
							})
							.clone()
					})
					.collect()
			})
			.collect();
		out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
		out
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

impl Default for MaterializedView {
	fn default() -> Self {
		Self::empty()
	}
}

#[cfg(test)]
mod projection_tests {
	use super::*;

	fn row(pairs: &[(&str, Value)]) -> MaterializedRow {
		MaterializedRow::from_pairs(pairs.iter().map(|(k, v)| (k.to_string(), v.clone())))
	}

	#[test]
	fn projecting_resolves_positions_through_the_recorded_column_order() {
		// Rows are keyed by column NAME so a tolerance can be looked up by name, but a family that keys
		// on row numbers projects by POSITION and has no reason to know the names. The recorded emission
		// order is what bridges the two. If it were dropped and positions resolved against the row's own
		// name ordering instead, `&[0, 1]` would silently mean alphabetical rather than emitted order -
		// here that would swap the two columns and every comparison would still "work", against the
		// wrong values.
		let mut view = MaterializedView::empty();
		view.columns = vec!["total".to_string(), "g".to_string()];
		view.insert(
			OutputKey::new(vec![Value::Int4(1)]),
			row(&[("g", Value::Int4(7)), ("total", Value::Int4(99))]),
		);

		assert_eq!(
			view.projected(&[0, 1]),
			vec![vec![Value::Int4(99), Value::Int4(7)]],
			"position 0 must be `total` because that is what was emitted first, not `g` because it sorts first"
		);
		assert_eq!(
			view.projected(&[1]),
			vec![vec![Value::Int4(7)]],
			"a narrower projection must still resolve"
		);
	}

	#[test]
	fn projecting_keeps_duplicates_rather_than_collapsing_them() {
		// A window projection deliberately drops the window start, so one group contributes several rows
		// that are indistinguishable by value. Those duplicates carry meaning: two windows each totalling
		// 5 is a different state from one window totalling 5. Collapsing them would make the comparison
		// unable to tell those apart.
		let mut view = MaterializedView::empty();
		view.columns = vec!["g".to_string(), "total".to_string()];
		for number in 1..=2u64 {
			view.insert(
				OutputKey::new(vec![Value::Uint8(number.into())]),
				row(&[("g", Value::Int4(1)), ("total", Value::Int4(5))]),
			);
		}

		assert_eq!(
			view.projected(&[0, 1]).len(),
			2,
			"two distinct rows that project to the same tuple must both survive the projection"
		);
	}
}
