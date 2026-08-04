// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What a two-stage pipeline owes its consumer: one row per group holding at least one row the first
//! stage admits, whose total is the sum of those rows' contributions.
//!
//! The oracle composes two independent statements - which rows get through, and what each contributes
//! - and never runs either operator's logic. It also never models the intermediate change stream,
//! which is the point: a pipeline is correct when its end state is right, whatever diffs crossed the
//! boundary to get there. That is what makes it able to catch a defect in those diffs that a
//! single-operator view comparison cannot see.

use std::collections::{BTreeMap, BTreeSet};

use reifydb_testing_chaos::operator::{
	compare::Tolerances,
	expectation::ViewClaim,
	model::Model,
	view::{MaterializedRow, MaterializedView, OutputKey},
};
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::operators::{aggregate::workload::AggregateRow, pipeline::Chain};

pub struct PipelineOracle {
	chain: Chain,
	live: BTreeMap<RowNumber, AggregateRow>,

	/// Rows the first stage has admitted and will not release. Only a latching stage populates this;
	/// for the others admission is re-derived from the current value on every claim.
	latched: BTreeSet<RowNumber>,
}

impl PipelineOracle {
	pub fn new(chain: Chain) -> Self {
		Self {
			chain,
			live: BTreeMap::new(),
			latched: BTreeSet::new(),
		}
	}

	fn admitted(&self, row: &AggregateRow) -> bool {
		match self.chain.latches() {
			true => self.latched.contains(&row.number),
			false => self.chain.passes(row),
		}
	}

	fn note_admission(&mut self, row: &AggregateRow) {
		if self.chain.latches() && self.chain.passes(row) {
			self.latched.insert(row.number);
		}
	}

	fn claim(&self) -> ViewClaim {
		let mut totals: BTreeMap<i32, i128> = BTreeMap::new();
		for row in self.live.values().filter(|row| self.admitted(row)) {
			*totals.entry(row.group).or_insert(0) += self.chain.contribution(row);
		}

		let mut view = MaterializedView::empty();
		view.columns = vec!["g".to_string(), "total".to_string()];
		for (group, total) in totals {
			view.insert(
				OutputKey::new(vec![Value::Int4(group)]),
				MaterializedRow::from_pairs(vec![
					("g".to_string(), Value::Int4(group)),
					("total".to_string(), Value::Int16(total)),
				]),
			);
		}

		ViewClaim::new(view, vec!["g".to_string()], Tolerances::new())
	}
}

impl Model<AggregateRow> for PipelineOracle {
	type Expectation = ViewClaim;

	fn admit(&mut self, row: &AggregateRow) -> bool {
		self.live.insert(row.number, row.clone());
		self.note_admission(row);
		true
	}

	fn retract(&mut self, row: &AggregateRow) {
		match self.live.remove(&row.number) {
			Some(held) => assert_eq!(
				held.value, row.value,
				"the driver retracts the value it last admitted for row {:?}",
				row.number
			),
			None => panic!("the driver retracted row {:?}, which the oracle never admitted", row.number),
		}
		// Removal reaches the first stage as a removal, so a latch is released here and nowhere else.
		self.latched.remove(&row.number);
	}

	fn update(&mut self, pre: &AggregateRow, post: &AggregateRow) {
		// Not retract-then-admit: for a latching stage that would release the latch and re-derive it,
		// turning the gate into a filter. Replacing the content and re-checking admission is what an
		// update actually does to every stage in the matrix.
		assert_eq!(pre.number, post.number, "an update must not change a row's number");
		self.live.insert(post.number, post.clone());
		self.note_admission(post);
	}

	fn advance_ledger(&mut self, _at_ms: u64) {
		// No stage in the matrix holds a clock or anything in flight.
	}

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
