// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	interface::change::{Change, Diff},
	value::column::columns::Columns,
};
use reifydb_value::value::{Value, datetime::DateTime, row_number::RowNumber};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowKey {
	pub columns: Vec<String>,

	pub include_time: bool,
}

impl RowKey {
	pub fn columns(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
		Self {
			columns: names.into_iter().map(Into::into).collect(),
			include_time: false,
		}
	}

	pub fn with_time(mut self) -> Self {
		self.include_time = true;
		self
	}
}

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

	pub time: Option<DateTime>,
}

impl MaterializedRow {
	pub fn new() -> Self {
		Self {
			columns: BTreeMap::new(),
			time: None,
		}
	}

	pub fn from_pairs<I, K>(pairs: I) -> Self
	where
		I: IntoIterator<Item = (K, Value)>,
		K: Into<String>,
	{
		Self {
			columns: pairs.into_iter().map(|(k, v)| (k.into(), v)).collect(),
			time: None,
		}
	}

	pub fn at(mut self, time: Option<DateTime>) -> Self {
		self.time = time;
		self
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
						if self.rows.insert(key.clone(), row).is_some() {
							self.incoherent.push(format!(
								"update whose post row {key:?} overwrote a live row"
							));
						}
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
		let times = columns.time();
		(0..columns.row_count())
			.map(|i| {
				let number: RowNumber = columns.row_numbers()[i];
				let row = MaterializedRow::from_pairs(names.iter().cloned().zip(columns.row(i)))
					.at(times.get(i).copied());
				(OutputKey::new(vec![Value::Uint8(number.0)]), row)
			})
			.collect()
	}

	pub fn rekey(&self, key: &RowKey) -> MaterializedView {
		let mut out = MaterializedView::empty();
		out.columns = self.columns.clone();
		for row in self.rows.values() {
			let mut values: Vec<Value> = key
				.columns
				.iter()
				.map(|name| row.get(name).cloned().unwrap_or(Value::Boolean(false)))
				.collect();

			if key.include_time {
				let Some(time) = row.time else {
					out.incoherent.push(format!(
						"a published row carries no event time, so it cannot be keyed by one: {row:?}"
					));
					continue;
				};
				values.push(Value::DateTime(time));
			}

			let key = OutputKey::new(values);
			if let Some(replaced) = out.rows.insert(key.clone(), row.clone()) {
				out.incoherent.push(format!("two published rows share the key {key:?}: {replaced:?}"));
			}
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
mod fold_tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::flow::FlowNodeId,
		value::column::{ColumnWithName, buffer::ColumnBuffer},
	};
	use reifydb_value::{
		fragment::Fragment,
		value::{datetime::DateTime, value_type::ValueType},
	};

	use super::*;

	fn columns(numbers: &[u64]) -> Columns {
		let mut buffer = ColumnBuffer::with_capacity(ValueType::Int8, numbers.len());
		for number in numbers {
			buffer.push_value(Value::Int8(*number as i64));
		}
		Columns::new(vec![ColumnWithName::new(Fragment::internal("v"), buffer)])
			.with_row_numbers(numbers.iter().map(|n| RowNumber(*n)).collect())
	}

	fn change(diffs: Vec<Diff>) -> Change {
		Change::from_flow(FlowNodeId(1), CommitVersion(1), diffs, DateTime::default())
	}

	#[test]
	fn an_update_whose_post_lands_on_an_unrelated_live_row_is_incoherent() {
		// The insert branch already reports writing over a live row; the update branch did not, so
		// the same collision arriving as an update silently destroyed the row it landed on and the
		// view read as if nothing had happened. A sink cannot apply that stream either way, so the
		// two branches have to agree about what they refuse.
		let mut view = MaterializedView::empty();
		view.fold(&change(vec![Diff::insert(columns(&[1, 2]))]));
		assert!(view.incoherent.is_empty(), "precondition: two distinct rows fold cleanly");

		// Row 1 updates itself onto row 2's key, which is still live and was never retracted.
		view.fold(&change(vec![Diff::update(columns(&[1]), columns(&[2]))]));

		assert_eq!(view.rows.len(), 1);
		assert_eq!(view.incoherent.len(), 1, "overwriting a live row must be reported, not absorbed");
	}

	#[test]
	fn an_update_that_keeps_its_own_key_is_not_a_collision() {
		// The control, and the reason the check has to run after the pre keys are removed rather
		// than before: the overwhelmingly common update rewrites a row in place, and reporting that
		// as a collision would make every suite in the tree incoherent from its first update.
		let mut view = MaterializedView::empty();
		view.fold(&change(vec![Diff::insert(columns(&[1, 2]))]));

		view.fold(&change(vec![Diff::update(columns(&[1]), columns(&[1]))]));

		assert_eq!(view.rows.len(), 2);
		assert!(view.incoherent.is_empty());
	}

	#[test]
	fn a_batched_update_that_permutes_its_own_keys_is_not_a_collision() {
		// A single update diff carrying 1->2 and 2->1 removes both pre keys before inserting either
		// post key, so neither insert lands on a live row. Checking per row as it was inserted -
		// rather than after the whole pre set is retracted - would report this shape as two
		// collisions, and a swap is a legal thing for an operator to publish.
		let mut view = MaterializedView::empty();
		view.fold(&change(vec![Diff::insert(columns(&[1, 2]))]));

		view.fold(&change(vec![Diff::update(columns(&[1, 2]), columns(&[2, 1]))]));

		assert_eq!(view.rows.len(), 2);
		assert!(view.incoherent.is_empty(), "a permutation within one diff collides with nothing");
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

	#[test]
	fn rekeying_reports_two_rows_that_share_a_key_rather_than_collapsing_them() {
		// The keyed comparison strategy is the one the append and join suites use, and it can only
		// describe one row per key. An operator that publishes a second output row number for a key
		// that already has one - which is what a group reborn after reclamation does - would have the
		// duplicate silently absorbed here and the view would read as if it had published once.
		// Mutation: restore the bare `out.insert(key, row)` and this fails while every other keyed
		// suite stays green, which is exactly how the defect hid.
		let mut view = MaterializedView::empty();
		view.columns = vec!["lid".to_string(), "v".to_string()];
		for number in 1..=2u64 {
			view.insert(
				OutputKey::new(vec![Value::Uint8(number)]),
				row(&[("lid", Value::Int8(7)), ("v", Value::Int8(number as i64))]),
			);
		}

		let rekeyed = view.rekey(&RowKey::columns(["lid"]));

		assert_eq!(rekeyed.rows.len(), 1, "the collapse itself is unavoidable: one key holds one row");
		assert_eq!(
			rekeyed.incoherent.len(),
			1,
			"but it must be reported, or the collapse is indistinguishable from a single publish"
		);
	}

	#[test]
	fn rekeying_a_view_whose_keys_are_distinct_reports_nothing() {
		// The control. If rekey reported a collision for rows that genuinely differ, every keyed
		// suite would fail for a reason unrelated to what it tests, and the report above would carry
		// no information.
		let mut view = MaterializedView::empty();
		view.columns = vec!["lid".to_string()];
		for number in 1..=2u64 {
			view.insert(
				OutputKey::new(vec![Value::Uint8(number)]),
				row(&[("lid", Value::Int8(number as i64))]),
			);
		}

		let rekeyed = view.rekey(&RowKey::columns(["lid"]));

		assert_eq!(rekeyed.rows.len(), 2);
		assert!(rekeyed.incoherent.is_empty());
	}
}
