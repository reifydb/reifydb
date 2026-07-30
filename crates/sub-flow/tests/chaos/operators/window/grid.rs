// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_value::value::{Value, row_number::RowNumber};

use reifydb_testing_chaos::operator::model::Model;

use crate::framework::workload::WindowRow;

/// Which fixed-grid windows a coordinate belongs to.
///
/// This is the ONLY thing that differs between tumbling and sliding: both anchor their seal
/// horizon on the window start, both close at `start + size + grace`, and both accumulate the
/// same way. Tumbling returns exactly one window per coordinate, sliding returns every window
/// whose span covers it.
pub trait Grid {
	fn windows_of(&self, coord_ms: u64) -> Vec<u64>;
}

pub struct GridOracle<G: Grid> {
	grid: G,
	cutoff_ms: u64,
	ledger: u64,
	contributions: Vec<Contribution>,
}

struct Contribution {
	row: RowNumber,
	group: i32,
	window: u64,
	value: i64,
	live: bool,
}

impl<G: Grid> GridOracle<G> {
	pub fn new(grid: G, size_ms: u64, grace_ms: u64) -> Self {
		Self {
			grid,
			cutoff_ms: size_ms + grace_ms,
			ledger: 0,
			contributions: Vec::new(),
		}
	}

	fn is_closed(&self, window: u64) -> bool {
		window.saturating_add(self.cutoff_ms).saturating_add(1) <= self.ledger
	}

	fn totals(&self) -> BTreeMap<(i32, u64), i64> {
		let mut totals: BTreeMap<(i32, u64), i64> = BTreeMap::new();
		for c in self.contributions.iter().filter(|c| c.live) {
			*totals.entry((c.group, c.window)).or_insert(0) += c.value;
		}
		totals
	}
}

impl<G: Grid> Model<WindowRow> for GridOracle<G> {
	type Expectation = Vec<Vec<Value>>;

	fn admit(&mut self, event: &WindowRow) -> bool {
		let WindowRow {
			number: row,
			group,
			coord_ms,
			value,
		} = *event;
		let mut admitted = false;
		for window in self.grid.windows_of(coord_ms) {
			if self.is_closed(window) {
				continue;
			}
			self.contributions.push(Contribution {
				row,
				group,
				window,
				value,
				live: true,
			});
			admitted = true;
		}
		admitted
	}

	fn retract(&mut self, event: &WindowRow) {
		let WindowRow {
			number: row,
			group,
			coord_ms,
			value,
		} = *event;
		for window in self.grid.windows_of(coord_ms) {
			if self.is_closed(window) {
				continue;
			}
			if let Some(c) = self
				.contributions
				.iter_mut()
				.find(|c| c.live && c.row == row && c.group == group && c.window == window)
			{
				assert_eq!(
					c.value, value,
					"the driver retracts the value it last admitted for row {row:?}; a mismatch \
					 means the oracle and the corpus have diverged and every later comparison \
					 is meaningless"
				);
				c.live = false;
			}
		}
	}

	fn advance_ledger(&mut self, at_ms: u64) {
		self.ledger = self.ledger.max(at_ms);
	}

	fn live(&self) -> Vec<Vec<Value>> {
		// Closing a fixed-grid window stops it admitting events and lets the operator reclaim
		// its accumulator, but the aggregate it already published stays in the view. Nothing
		// evicts a grid window, so every window the oracle ever opened is required, not merely
		// permitted - if the operator withdraws a closed window's row, this bound catches it.
		self.all()
	}

	fn all(&self) -> Vec<Vec<Value>> {
		render(self.totals().into_iter())
	}

	fn after_drain(&self) -> Vec<Vec<Value>> {
		self.all()
	}
}

pub fn render(entries: impl Iterator<Item = ((i32, u64), i64)>) -> Vec<Vec<Value>> {
	let mut out: Vec<Vec<Value>> =
		entries.map(|((group, _), total)| vec![Value::Int4(group), Value::Int16(total as i128)]).collect();
	out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
	out
}
