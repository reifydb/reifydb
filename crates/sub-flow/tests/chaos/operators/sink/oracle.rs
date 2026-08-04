// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What a view sink owes: the rows it was handed, minus whatever its own capacity pushed out.
//!
//! Stated as a bounded FIFO per lane, which shares nothing with how the operator reaches the same
//! answer. A derivation that mirrored the operator's would agree with it precisely when both were
//! wrong.
//!
//! An unpartitioned sink is the one-lane case, so both layouts stay one oracle. Confusing
//! per-partition capacity for global capacity then diverges in exactly one of them.

use std::collections::{BTreeMap, VecDeque};

use reifydb_testing_chaos::operator::model::Model;
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::operators::aggregate::workload::AggregateRow;

/// The single lane an unpartitioned sink writes into. Any value works; it never reaches an assertion.
const GLOBAL_LANE: i32 = 0;

pub struct SinkOracle {
	capacity: Option<usize>,
	partitioned: bool,
	live: BTreeMap<RowNumber, AggregateRow>,
	lanes: BTreeMap<i32, VecDeque<RowNumber>>,
}

impl SinkOracle {
	pub fn new(capacity: Option<usize>, partitioned: bool) -> Self {
		Self {
			capacity,
			partitioned,
			live: BTreeMap::new(),
			lanes: BTreeMap::new(),
		}
	}

	fn lane_of(&self, row: &AggregateRow) -> i32 {
		match self.partitioned {
			true => row.group,
			false => GLOBAL_LANE,
		}
	}

	fn claim(&self) -> Vec<Vec<Value>> {
		let mut rows: Vec<Vec<Value>> =
			self.live.values().map(|row| vec![Value::Int4(row.group), Value::Int8(row.value)]).collect();
		rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
		rows
	}
}

impl Model<AggregateRow> for SinkOracle {
	type Expectation = Vec<Vec<Value>>;

	fn admit(&mut self, row: &AggregateRow) -> bool {
		let lane = self.lane_of(row);
		self.live.insert(row.number, row.clone());
		let queue = self.lanes.entry(lane).or_default();
		queue.push_back(row.number);

		// Trimmed after the arrival is queued, not before, so a capacity that cannot fit the arrival
		// drops the arrival itself.
		if let Some(capacity) = self.capacity {
			while queue.len() > capacity {
				let evicted = queue.pop_front().expect("a queue over capacity is not empty");
				self.live.remove(&evicted);
			}
		}
		true
	}

	fn retract(&mut self, row: &AggregateRow) {
		// The corpus retracts rows capacity may already have evicted, so finding nothing is expected.
		// Resurrecting the row, or disturbing another lane, is not.
		if let Some(held) = self.live.remove(&row.number) {
			assert_eq!(
				held.value, row.value,
				"the driver retracts the value it last admitted for row {:?}",
				row.number
			);
		}
		if let Some(queue) = self.lanes.get_mut(&self.lane_of(row)) {
			queue.retain(|number| *number != row.number);
		}
	}

	fn update(&mut self, pre: &AggregateRow, post: &AggregateRow) {
		assert_eq!(pre.number, post.number, "an update must not change a row's number");

		// Not retract-then-admit: that would move the row to the back of its lane, and an update must
		// not let a row outlive those that arrived after it.
		assert_eq!(
			self.lane_of(pre),
			self.lane_of(post),
			"a partitioned sink refuses an update that moves a row between partitions, so the corpus \
			 must not produce one"
		);

		if self.live.contains_key(&pre.number) {
			self.live.insert(post.number, post.clone());
		}
	}

	fn advance_ledger(&mut self, _at_ms: u64) {
		// No sink seals on the generic timer, and the sweeps hold tick_pct at 0.
	}

	fn live(&self) -> Vec<Vec<Value>> {
		self.claim()
	}

	fn all(&self) -> Vec<Vec<Value>> {
		self.claim()
	}

	fn after_drain(&self) -> Vec<Vec<Value>> {
		self.claim()
	}
}
