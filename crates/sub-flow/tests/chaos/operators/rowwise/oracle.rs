// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! One oracle for the three row-wise operators, because their contracts differ only in what a single
//! live row turns into.
//!
//! Every answer here is hand-written Rust over `RowwiseRow`. None of it evaluates the RQL the operator
//! was built from, calls the expression compiler, or touches the routine registry. That is the point:
//! `Shape::rql` and `Shape::render` are two independent statements of the same intent, and a corpus
//! where they disagree is what the suite is looking for. An oracle that compiled the same expression
//! would agree with the operator precisely when both were wrong.

use std::collections::BTreeMap;

use reifydb_testing_chaos::operator::{expectation::KeyedMultiset, model::Model, view::RowKey};
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::operators::rowwise::{
	Shape,
	workload::{IDENTITY_COLUMN, RowwiseRow},
};

pub struct RowwiseOracle {
	shape: Shape,
	live: BTreeMap<RowNumber, RowwiseRow>,
}

impl RowwiseOracle {
	pub fn new(shape: Shape) -> Self {
		Self {
			shape,
			live: BTreeMap::new(),
		}
	}

	fn claim(&self) -> KeyedMultiset {
		let mut rows: Vec<Vec<Value>> = self
			.live
			.values()
			.filter(|row| self.shape.admits(row))
			.map(|row| self.shape.render(row))
			.collect();
		rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));

		// Keyed on the identity column, which every shape carries through, so two output rows claiming
		// the same source row are reported as a collision rather than satisfying the multiset by luck.
		KeyedMultiset::new(RowKey::columns([IDENTITY_COLUMN]), rows)
	}
}

impl Model<RowwiseRow> for RowwiseOracle {
	type Expectation = KeyedMultiset;

	fn admit(&mut self, row: &RowwiseRow) -> bool {
		self.live.insert(row.number, row.clone());
		true
	}

	fn retract(&mut self, row: &RowwiseRow) {
		match self.live.remove(&row.number) {
			Some(held) => assert_eq!(
				held.value, row.value,
				"the driver retracts the value it last admitted for row {:?}",
				row.number
			),
			None => panic!("the driver retracted row {:?}, which the oracle never admitted", row.number),
		}
	}

	fn update(&mut self, pre: &RowwiseRow, post: &RowwiseRow) {
		// The default retract-then-admit would be correct for all three of these, since none of them
		// hold per-row state. It is spelled out anyway so that adding a shape that DOES latch or order
		// cannot inherit the wrong semantics silently, the way it would have for gate and take.
		assert_eq!(pre.number, post.number, "an update must not change a row's number");
		self.retract(pre);
		self.admit(post);
	}

	fn advance_ledger(&mut self, _at_ms: u64) {
		// None of the row-wise operators hold a clock or anything in flight.
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
