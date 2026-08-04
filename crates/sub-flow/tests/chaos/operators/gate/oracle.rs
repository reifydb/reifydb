// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What a gate owes its consumer: every row that has *ever* satisfied the condition since it was
//! inserted, carrying its current content.
//!
//! The word that does the work is "ever". A gate latches: the first time a row passes it is marked
//! visible, and from then on the condition is never consulted for that row again. An update to a
//! visible row is published as an update whatever its new value, and a row whose value falls back
//! below the threshold stays in the view. Only a removal takes it out, and re-inserting it starts
//! the latch over.
//!
//! That is the entire difference between a gate and a filter, and it is why this oracle tracks a
//! visibility set rather than re-deriving membership from the current values. A model that asked
//! "does this row pass right now" would agree with the operator on every row that never crosses back
//! down, and disagree on exactly the rows the latch exists for.
//!
//! The condition itself is a Rust closure, not the compiled RQL the operator evaluates, per the
//! tautology rule: the operator is handed `v > threshold` as an expression and the oracle is handed
//! the same intent as code, and a disagreement between them is the finding.

use std::collections::BTreeMap;

use reifydb_testing_chaos::operator::{expectation::KeyedMultiset, model::Model, view::RowKey};
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::operators::gate::workload::{GateRow, IDENTITY_COLUMN};

pub struct GateOracle {
	threshold: i64,

	/// Every live source row, whether or not it is visible. A row must be tracked while invisible
	/// because a later update can admit it.
	live: BTreeMap<RowNumber, GateRow>,

	/// The rows the gate has admitted. Grows on the first pass and shrinks only on removal.
	visible: BTreeMap<RowNumber, ()>,
}

impl GateOracle {
	pub fn new(threshold: i64) -> Self {
		Self {
			threshold,
			live: BTreeMap::new(),
			visible: BTreeMap::new(),
		}
	}

	/// The gate condition, stated as code rather than compiled from the same expression the operator
	/// evaluates.
	fn passes(&self, row: &GateRow) -> bool {
		row.value > self.threshold
	}

	fn claim(&self) -> KeyedMultiset {
		let mut rows: Vec<Vec<Value>> = self
			.visible
			.keys()
			.map(|number| {
				let row = self
					.live
					.get(number)
					.expect("a visible row must still be live; removal clears visibility");
				vec![Value::Int4(row.identity()), Value::Int8(row.value)]
			})
			.collect();
		rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));

		KeyedMultiset::new(RowKey::columns([IDENTITY_COLUMN]), rows)
	}
}

impl Model<GateRow> for GateOracle {
	type Expectation = KeyedMultiset;

	fn admit(&mut self, row: &GateRow) -> bool {
		self.live.insert(row.number, row.clone());
		if self.passes(row) {
			self.visible.insert(row.number, ());
		}
		true
	}

	fn retract(&mut self, row: &GateRow) {
		match self.live.remove(&row.number) {
			Some(held) => assert_eq!(
				held.value, row.value,
				"the driver retracts the value it last admitted for row {:?}",
				row.number
			),
			None => panic!("the driver retracted row {:?}, which the oracle never admitted", row.number),
		}
		// Removal is the only thing that un-latches a row. Re-inserting it starts over, which is why
		// the driver's remove-then-insert split of an update is not the same as an update here.
		self.visible.remove(&row.number);
	}

	fn update(&mut self, pre: &GateRow, post: &GateRow) {
		// Overridden because the default retract-then-admit would clear visibility and then re-derive
		// it from the new value, turning the gate into a filter. An update must leave an already-visible
		// row visible no matter what its new value is.
		assert_eq!(pre.number, post.number, "an update must not change a row's number");
		self.live.insert(post.number, post.clone());
		if self.passes(post) {
			self.visible.insert(post.number, ());
		}
	}

	fn advance_ledger(&mut self, _at_ms: u64) {
		// A gate holds no clock and nothing in flight.
	}

	fn live(&self) -> KeyedMultiset {
		self.claim()
	}

	fn all(&self) -> KeyedMultiset {
		self.claim()
	}

	fn after_drain(&self) -> KeyedMultiset {
		self.claim()
	}
}
