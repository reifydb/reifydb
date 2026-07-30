// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_value::value::{Value, row_number::RowNumber};

use reifydb_testing_chaos::operator::model::Model;

use crate::{framework::workload::WindowRow, operators::window::grid::render};

/// Which windows the n-th admitted row of a group belongs to.
///
/// The ordinal is 0-based and assigned by the MODEL, mirroring the operator's per-group
/// `get_and_increment_global_count`. That is the whole difference from the time-based kinds: the
/// bucketing coordinate is not carried by the row, it is handed out on arrival.
pub trait Ordinals {
	fn windows_of(&self, ordinal: u64) -> Vec<u64>;
}

pub struct CountOracle<O: Ordinals> {
	ordinals: O,
	next_ordinal: HashMap<i32, u64>,
	assigned: HashMap<RowNumber, Vec<u64>>,
	contributions: Vec<Contribution>,
}

struct Contribution {
	row: RowNumber,
	group: i32,
	window: u64,
	value: i64,
	live: bool,
}

impl<O: Ordinals> CountOracle<O> {
	pub fn new(ordinals: O) -> Self {
		Self {
			ordinals,
			next_ordinal: HashMap::new(),
			assigned: HashMap::new(),
			contributions: Vec::new(),
		}
	}

	fn totals(&self) -> BTreeMap<(i32, u64), i64> {
		let mut totals: BTreeMap<(i32, u64), i64> = BTreeMap::new();
		for c in self.contributions.iter().filter(|c| c.live) {
			*totals.entry((c.group, c.window)).or_insert(0) += c.value;
		}
		totals
	}
}

impl<O: Ordinals> Model<WindowRow> for CountOracle<O> {
	fn admit(&mut self, event: &WindowRow) -> bool {
		let WindowRow {
			number: row,
			group,
			coord_ms,
			value,
		} = *event;
		let _ = coord_ms;
		// An UPDATE reaches the model as retract-then-admit on the same row. The operator does not
		// consume a new ordinal for one - it looks the row up in its row index and reuses whatever
		// windows it already sits in - so neither may the model, or every update would shift the
		// group's remaining rows one window along.
		let windows = match self.assigned.get(&row) {
			Some(windows) => windows.clone(),
			None => {
				let next = self.next_ordinal.entry(group).or_insert(0);
				let ordinal = *next;
				*next += 1;
				let windows = self.ordinals.windows_of(ordinal);
				self.assigned.insert(row, windows.clone());
				windows
			}
		};
		for window in windows {
			self.contributions.push(Contribution {
				row,
				group,
				window,
				value,
				live: true,
			});
		}
		// A count window has no seal horizon, so nothing is ever refused as late.
		true
	}

	fn retract(&mut self, event: &WindowRow) {
		let WindowRow {
			number: row,
			group,
			coord_ms,
			value,
		} = *event;
		let _ = coord_ms;
		let Some(windows) = self.assigned.get(&row).cloned() else {
			return;
		};
		for window in windows {
			if let Some(c) = self
				.contributions
				.iter_mut()
				.find(|c| c.live && c.row == row && c.group == group && c.window == window)
			{
				assert_eq!(
					c.value, value,
					"the driver retracts the value it last admitted for row {row:?}; a mismatch \
					 means the oracle and the corpus have diverged"
				);
				c.live = false;
			}
		}
	}

	fn advance_ledger(&mut self, _at_ms: u64) {
		// A count window is never indexed for expiry and its gate returns early, so a seal tick
		// cannot move anything. The sweeps set seal_pct to 0; this exists so that stays harmless.
	}

	fn live(&self) -> Vec<Vec<Value>> {
		render(self.totals().into_iter())
	}

	fn all(&self) -> Vec<Vec<Value>> {
		self.live()
	}

	fn after_drain(&self) -> Vec<Vec<Value>> {
		// Nothing expires, so ticking past every horizon withdraws nothing. This is the assertion
		// that a count window's state is unbounded, stated as an expectation rather than a defect.
		self.live()
	}
}
