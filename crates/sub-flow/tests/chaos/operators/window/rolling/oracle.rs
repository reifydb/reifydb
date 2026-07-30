// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_value::value::{Value, row_number::RowNumber};

use reifydb_testing_chaos::operator::model::Model;

use crate::{framework::workload::WindowRow, operators::window::grid::render};

/// A rolling window keeps one row per GROUP, not one per window: the buffer of contributions
/// trails the seal ledger and the emitted value is the aggregate over whatever is still in it.
///
/// Two boundaries here are deliberately asymmetric and are what this oracle exists to pin:
///   admission drops a coordinate STRICTLY below `ledger - (size + grace)` (`is_sealed` compares
///   with `<`), while eviction pops everything at or below `ledger - size` (RollingEviction::Before
///   is inclusive despite its name). Getting either off by one shifts the retained set.
pub struct Oracle {
	size_ms: u64,
	grace_ms: u64,
	ledger: u64,
	contributions: Vec<Contribution>,
}

struct Contribution {
	row: RowNumber,
	group: i32,
	coord: u64,
	value: i64,
	live: bool,
}

impl Oracle {
	pub fn new(size_ms: u64, grace_ms: u64) -> Self {
		Self {
			size_ms,
			grace_ms,
			ledger: 0,
			contributions: Vec::new(),
		}
	}

	fn admission_horizon(&self) -> u64 {
		self.ledger.saturating_sub(self.size_ms.saturating_add(self.grace_ms))
	}

	fn eviction_cutoff(&self) -> u64 {
		self.ledger.saturating_sub(self.size_ms)
	}

	fn is_late(&self, coord: u64) -> bool {
		coord < self.admission_horizon()
	}

	fn is_retained(&self, coord: u64) -> bool {
		coord > self.eviction_cutoff()
	}

	fn totals(&self) -> BTreeMap<(i32, u64), i64> {
		let mut totals: BTreeMap<(i32, u64), i64> = BTreeMap::new();
		for c in self.contributions.iter().filter(|c| c.live && self.is_retained(c.coord)) {
			*totals.entry((c.group, 0)).or_insert(0) += c.value;
		}
		totals
	}
}

impl Model<WindowRow> for Oracle {
	type Expectation = Vec<Vec<Value>>;

	fn admit(&mut self, event: &WindowRow) -> bool {
		let WindowRow {
			number: row,
			group,
			coord_ms,
			value,
		} = *event;
		if self.is_late(coord_ms) {
			return false;
		}
		self.contributions.push(Contribution {
			row,
			group,
			coord: coord_ms,
			value,
			live: true,
		});
		true
	}

	fn retract(&mut self, event: &WindowRow) {
		let WindowRow {
			number: row,
			group,
			coord_ms,
			value,
		} = *event;
		if self.is_late(coord_ms) {
			return;
		}
		if let Some(c) = self.contributions.iter_mut().find(|c| c.live && c.row == row && c.group == group) {
			assert_eq!(
				c.value, value,
				"the driver retracts the value it last admitted for row {row:?}; a mismatch means \
				 the oracle and the corpus have diverged and every later comparison is meaningless"
			);
			c.live = false;
		}
	}

	fn advance_ledger(&mut self, at_ms: u64) {
		self.ledger = self.ledger.max(at_ms);
	}

	fn live(&self) -> Vec<Vec<Value>> {
		render(self.totals().into_iter())
	}

	fn all(&self) -> Vec<Vec<Value>> {
		// A rolling group's value is fully determined by the ledger, so what MUST be published and
		// what MAY be published are the same set. That collapses the driver's two containment
		// checks into an equality, which is strictly stronger than the fixed-grid case.
		self.live()
	}

	fn after_drain(&self) -> Vec<Vec<Value>> {
		Vec::new()
	}
}

/// A count-based rolling window: no seal horizon at all, and eviction is by CAPACITY rather than
/// by coordinate.
///
/// This has to simulate the buffer rather than recompute from the corpus, because capacity
/// eviction is destructive: once a row has been pushed out by newer arrivals it never comes back,
/// not even if a newer row is later retracted and leaves the buffer short. The coordinate is the
/// row number (`u64::coord` reads `row_numbers()`), so evicting the lowest key evicts the oldest
/// row.
pub struct CapacityOracle {
	capacity: usize,
	buffers: BTreeMap<i32, BTreeMap<u64, i64>>,
}

impl CapacityOracle {
	pub fn new(capacity: u64) -> Self {
		Self {
			capacity: capacity as usize,
			buffers: BTreeMap::new(),
		}
	}
}

impl Model<WindowRow> for CapacityOracle {
	type Expectation = Vec<Vec<Value>>;

	fn admit(&mut self, event: &WindowRow) -> bool {
		let WindowRow {
			number: row,
			group,
			coord_ms,
			value,
		} = *event;
		let _ = coord_ms;
		let buffer = self.buffers.entry(group).or_default();
		buffer.insert(row.0, value);
		while buffer.len() > self.capacity {
			buffer.pop_first();
		}
		true
	}

	fn retract(&mut self, event: &WindowRow) {
		let WindowRow {
			number: row,
			group,
			coord_ms,
			value,
		} = *event;
		let (_, _) = (coord_ms, value);
		// A row already pushed out of the window contributes nothing, so retracting it must change
		// nothing. It must not resurrect the row as a negative contribution.
		if let Some(buffer) = self.buffers.get_mut(&group) {
			buffer.remove(&row.0);
		}
	}

	fn advance_ledger(&mut self, _at_ms: u64) {}

	fn live(&self) -> Vec<Vec<Value>> {
		render(self
			.buffers
			.iter()
			.filter(|(_, buffer)| !buffer.is_empty())
			.map(|(group, buffer)| ((*group, 0), buffer.values().sum())))
	}

	fn all(&self) -> Vec<Vec<Value>> {
		self.live()
	}

	fn after_drain(&self) -> Vec<Vec<Value>> {
		self.live()
	}
}
