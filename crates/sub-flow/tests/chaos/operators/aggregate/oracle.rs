// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What an aggregate owes its consumer: one row per group that still holds a live source row, whose
//! aggregate column equals the fold of exactly those rows' values.
//!
//! The fold here is hand-written Rust over a `Vec<i64>`, and it never touches the monoid registry, the
//! expression compiler, or the accumulator. That is deliberate: an oracle that computed the answer by
//! the same route the operator does would agree with it whenever both are wrong. The RQL string handed
//! to the operator and the closure in `Agg::fold` are two independent statements of the same intent,
//! and the point of the suite is to find a corpus where they disagree.

use std::collections::BTreeMap;

use reifydb_testing_chaos::operator::{
	compare::Tolerances,
	expectation::ViewClaim,
	model::Model,
	view::{MaterializedRow, MaterializedView, OutputKey},
};
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::operators::aggregate::{Agg, GROUP_COLUMN, workload::AggregateRow};

pub struct AggregateOracle {
	agg: Agg,
	live: BTreeMap<RowNumber, AggregateRow>,
}

impl AggregateOracle {
	pub fn new(agg: Agg) -> Self {
		Self {
			agg,
			live: BTreeMap::new(),
		}
	}

	fn claim(&self) -> ViewClaim {
		let mut per_group: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
		for row in self.live.values() {
			per_group.entry(row.group).or_default().push(row.value);
		}

		let mut view = MaterializedView::empty();
		view.columns = vec![GROUP_COLUMN.to_string(), self.agg.column().to_string()];
		for (group, values) in per_group {
			view.insert(
				OutputKey::new(vec![Value::Int4(group)]),
				MaterializedRow::from_pairs(vec![
					(GROUP_COLUMN.to_string(), Value::Int4(group)),
					(self.agg.column().to_string(), self.agg.fold(&values)),
				]),
			);
		}

		// Keyed on the group rather than the emitted row number: which group a row belongs to is the
		// operator's contract, but the number it publishes the group under is the operator's to choose.
		// An exact fold means no tolerance - every aggregate in this suite is integral.
		ViewClaim::new(view, vec![GROUP_COLUMN.to_string()], Tolerances::new())
	}
}

impl Model<AggregateRow> for AggregateOracle {
	type Expectation = ViewClaim;

	fn admit(&mut self, row: &AggregateRow) -> bool {
		self.live.insert(row.number, row.clone());
		true
	}

	fn retract(&mut self, row: &AggregateRow) {
		match self.live.remove(&row.number) {
			Some(held) => assert_eq!(
				held.value, row.value,
				"the driver retracts the value it last admitted for row {:?}; a mismatch means the \
				 oracle and the corpus have diverged and every later comparison is meaningless",
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
		// An aggregate routes every row into one degenerate span and holds no seal ledger, so a tick
		// moves nothing. The sweeps leave tick_pct at zero; this exists so that stays harmless.
	}

	fn live(&self) -> ViewClaim {
		self.claim()
	}

	fn all(&self) -> ViewClaim {
		self.claim()
	}

	fn after_drain(&self) -> ViewClaim {
		// Nothing expires and nothing is held back, so ticking past every horizon changes nothing. The
		// driver checks this one under `Exactly`, which is what makes a group left behind after its last
		// row was retracted a failure rather than a permitted lag.
		self.claim()
	}
}
