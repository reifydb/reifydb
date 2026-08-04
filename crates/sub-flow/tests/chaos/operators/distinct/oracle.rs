// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What distinct owes its consumer: exactly one row per key that still holds a live source row, and
//! that row carries the content of the highest-numbered live source row under the key.
//!
//! Both halves matter and they fail differently. Losing the first gives a view with two rows for one
//! key, which the claim's own rekey reports as rows it cannot tell apart. Losing the second gives a
//! view with the right shape and the wrong payload - a stale representative left behind because a
//! retraction did not promote its successor - which only a content comparison catches.
//!
//! Stated by picking a maximum out of a map, which is not how the operator does it: the operator
//! keeps a per-key `BTreeMap` of every colliding row and reasons incrementally about whether each
//! arrival or departure displaces the current visible row. The oracle never reasons incrementally at
//! all, which is the point.

use std::collections::BTreeMap;

use reifydb_testing_chaos::operator::{
	compare::Tolerances,
	expectation::ViewClaim,
	model::Model,
	view::{MaterializedRow, MaterializedView, OutputKey},
};
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::operators::distinct::workload::{DistinctRow, KEY_COLUMN, PAYLOAD_COLUMN};

pub struct DistinctOracle {
	live: BTreeMap<RowNumber, DistinctRow>,
}

impl DistinctOracle {
	pub fn new() -> Self {
		Self {
			live: BTreeMap::new(),
		}
	}

	/// The highest-numbered live row under each key. `live` is ordered by row number, so a later
	/// entry for the same key overwrites an earlier one and the last writer wins.
	fn representatives(&self) -> BTreeMap<i32, &DistinctRow> {
		let mut winner: BTreeMap<i32, &DistinctRow> = BTreeMap::new();
		for row in self.live.values() {
			winner.insert(row.group, row);
		}
		winner
	}

	fn claim(&self) -> ViewClaim {
		let mut view = MaterializedView::empty();
		view.columns = vec![KEY_COLUMN.to_string(), PAYLOAD_COLUMN.to_string()];
		for (group, row) in self.representatives() {
			view.insert(
				OutputKey::new(vec![Value::Int4(group)]),
				MaterializedRow::from_pairs(vec![
					(KEY_COLUMN.to_string(), Value::Int4(group)),
					(PAYLOAD_COLUMN.to_string(), Value::Int8(row.value)),
				]),
			);
		}

		// Keyed on the distinct key, which is the whole identity the operator publishes under. The
		// stable row number it mints per key is its own business, so the claim must not depend on it.
		ViewClaim::new(view, vec![KEY_COLUMN.to_string()], Tolerances::new())
	}
}

impl Model<DistinctRow> for DistinctOracle {
	type Expectation = ViewClaim;

	fn admit(&mut self, row: &DistinctRow) -> bool {
		self.live.insert(row.number, row.clone());
		true
	}

	fn retract(&mut self, row: &DistinctRow) {
		match self.live.remove(&row.number) {
			Some(held) => assert_eq!(
				(held.group, held.value),
				(row.group, row.value),
				"the driver retracts the row it last admitted for {:?}; a mismatch means the oracle and \
				 the corpus have diverged and every later comparison is meaningless",
				row.number
			),
			None => panic!(
				"the driver retracted row {:?}, which the oracle never admitted - the corpus and the \
				 model are out of step",
				row.number
			),
		}
	}

	fn advance_ledger(&mut self, _at_ms: u64) {
		// Distinct holds no seal ledger and nothing in flight, so a tick moves nothing. The sweeps
		// leave tick_pct at zero; this exists so that stays harmless.
	}

	fn live(&self) -> ViewClaim {
		self.claim()
	}

	fn all(&self) -> ViewClaim {
		self.claim()
	}

	fn after_drain(&self) -> ViewClaim {
		// Checked under `Exactly`, which is what makes a key left behind after its last row was
		// retracted a failure rather than a permitted lag.
		self.claim()
	}
}
