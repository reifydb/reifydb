// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What each join strategy owes its consumer, described as the table the consumer sees.
//!
//! Both oracles below claim the whole view exactly, with no gap between the bounds the driver
//! checks. A join publishes everything it owes inside the `apply` that caused it - there is no tick,
//! no horizon and nothing in flight - so a view that merely lags is already a divergence.

use std::collections::BTreeMap;

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_testing_chaos::operator::{
	compare::Tolerances,
	expectation::ViewClaim,
	model::Model,
	view::{MaterializedRow, MaterializedView, OutputKey},
};
use reifydb_value::value::{Value, value_type::ValueType};

use crate::operators::join::workload::{JoinRow, LEFT_COLUMNS, RIGHT_COLUMNS, Side};

/// The `Value::None` the join fills an unmatched right column with.
///
/// Which `None` variant that is depends on how a buffer of the column's type represents absence, so
/// this builds it the way the operator does - `ColumnBuffer::with_capacity` of the right schema's
/// type, then `push_value(Value::none())` - rather than naming a variant and hoping it matches.
/// Naming the wrong one would make every unmatched row read as divergent for a reason that has
/// nothing to do with the join.
fn absent(ty: ValueType) -> Value {
	let mut buffer = ColumnBuffer::with_capacity(ty, 1);
	buffer.push_value(Value::none());
	buffer.get_value(0)
}

fn key_value(key: Option<i32>) -> Value {
	match key {
		Some(key) => Value::Int4(key),
		None => absent(ValueType::Int4),
	}
}

fn output_columns() -> Vec<String> {
	LEFT_COLUMNS
		.iter()
		.map(|(name, _)| (*name).to_string())
		.chain(RIGHT_COLUMNS.iter().map(|(name, _)| format!("other_{name}")))
		.collect()
}

fn left_pairs(left: &JoinRow) -> Vec<(String, Value)> {
	vec![
		("lid".to_string(), Value::Int8(left.number.0 as i64)),
		("k".to_string(), key_value(left.key)),
		("lv".to_string(), Value::Int8(left.value)),
	]
}

fn joined(left: &JoinRow, right: &JoinRow) -> MaterializedRow {
	let mut pairs = left_pairs(left);
	pairs.push(("other_rid".to_string(), Value::Int8(right.number.0 as i64)));
	pairs.push(("other_k".to_string(), key_value(right.key)));
	pairs.push(("other_rv".to_string(), Value::Int8(right.value)));
	MaterializedRow::from_pairs(pairs)
}

fn unmatched(left: &JoinRow) -> MaterializedRow {
	let mut pairs = left_pairs(left);
	pairs.push(("other_rid".to_string(), absent(ValueType::Int8)));
	pairs.push(("other_k".to_string(), absent(ValueType::Int4)));
	pairs.push(("other_rv".to_string(), absent(ValueType::Int8)));
	MaterializedRow::from_pairs(pairs)
}

fn empty_view() -> MaterializedView {
	let mut view = MaterializedView::empty();
	view.columns = output_columns();
	view
}

/// The two hash strategies: every live right row that shares a live left row's key produces an
/// output row, so the view is a pure function of the two live sets and nothing about the order they
/// arrived in survives.
pub struct HashOracle {
	left_outer: bool,
	left: BTreeMap<u64, JoinRow>,
	right: BTreeMap<u64, JoinRow>,
}

impl HashOracle {
	pub fn new(left_outer: bool) -> Self {
		Self {
			left_outer,
			left: BTreeMap::new(),
			right: BTreeMap::new(),
		}
	}

	fn claim(&self) -> ViewClaim {
		let mut view = empty_view();
		for left in self.left.values() {
			let matches: Vec<&JoinRow> = match left.key {
				Some(key) => self.right.values().filter(|right| right.key == Some(key)).collect(),
				None => Vec::new(),
			};
			if matches.is_empty() {
				if self.left_outer {
					view.insert(
						OutputKey::new(vec![
							Value::Int8(left.number.0 as i64),
							absent(ValueType::Int8),
						]),
						unmatched(left),
					);
				}
				continue;
			}
			for right in matches {
				view.insert(
					OutputKey::new(vec![
						Value::Int8(left.number.0 as i64),
						Value::Int8(right.number.0 as i64),
					]),
					joined(left, right),
				);
			}
		}
		// A hash join mints one output row per (left, right) pair, and an unmatched left row is the
		// pair (left, nothing). Keying on those two columns is what lets the claim be compared
		// without predicting the row numbers the operator mints for them.
		ViewClaim::new(view, vec!["lid".to_string(), "other_rid".to_string()], Tolerances::new())
	}
}

impl Model<JoinRow> for HashOracle {
	type Expectation = ViewClaim;

	fn admit(&mut self, row: &JoinRow) -> bool {
		match row.side {
			Side::Left => self.left.insert(row.number.0, row.clone()),
			Side::Right => self.right.insert(row.number.0, row.clone()),
		};
		true
	}

	fn retract(&mut self, row: &JoinRow) {
		match row.side {
			Side::Left => self.left.remove(&row.number.0),
			Side::Right => self.right.remove(&row.number.0),
		};
	}

	fn advance_ledger(&mut self, _at_ms: u64) {}

	fn live(&self) -> ViewClaim {
		self.claim()
	}

	fn all(&self) -> ViewClaim {
		self.claim()
	}

	fn after_drain(&self) -> ViewClaim {
		self.claim()
	}
}

/// The two latest strategies: the right side is one slot per key holding whichever right row was
/// written to it last, and every live left row reads that slot.
///
/// Unlike the hash oracle this is not a function of the live sets. A right removal clears the slot
/// for its key even when the row occupying the slot is a different one that is still live, so the
/// slot is history and has to be tracked as the driver replays it.
pub struct LatestOracle {
	left_outer: bool,
	left: BTreeMap<u64, JoinRow>,
	slot: BTreeMap<i32, JoinRow>,
}

impl LatestOracle {
	pub fn new(left_outer: bool) -> Self {
		Self {
			left_outer,
			left: BTreeMap::new(),
			slot: BTreeMap::new(),
		}
	}

	fn claim(&self) -> ViewClaim {
		let mut view = empty_view();
		for left in self.left.values() {
			let key = OutputKey::new(vec![Value::Int8(left.number.0 as i64)]);
			match left.key.and_then(|key| self.slot.get(&key)) {
				Some(right) => view.insert(key, joined(left, right)),
				None if self.left_outer => view.insert(key, unmatched(left)),
				None => {}
			}
		}
		// Latest reuses the left row's own number for the row it emits rather than minting one per
		// pair, so a left row is at most one output row and its identity is the left row.
		ViewClaim::new(view, vec!["lid".to_string()], Tolerances::new())
	}
}

impl Model<JoinRow> for LatestOracle {
	type Expectation = ViewClaim;

	fn admit(&mut self, row: &JoinRow) -> bool {
		match row.side {
			Side::Left => {
				self.left.insert(row.number.0, row.clone());
			}
			// A right row with an undefined key never reaches a slot, so it is a complete no-op
			// on this side - not an occupant of some undefined-keyed slot.
			Side::Right => {
				if let Some(key) = row.key {
					self.slot.insert(key, row.clone());
				}
			}
		}
		true
	}

	fn retract(&mut self, row: &JoinRow) {
		match row.side {
			Side::Left => {
				self.left.remove(&row.number.0);
			}
			Side::Right => {
				if let Some(key) = row.key {
					self.slot.remove(&key);
				}
			}
		}
	}

	fn advance_ledger(&mut self, _at_ms: u64) {}

	fn live(&self) -> ViewClaim {
		self.claim()
	}

	fn all(&self) -> ViewClaim {
		self.claim()
	}

	fn after_drain(&self) -> ViewClaim {
		self.claim()
	}
}
