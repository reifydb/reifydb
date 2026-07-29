// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_value::value::Value;

use crate::framework::driver::Model;

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

impl<G: Grid> Model for GridOracle<G> {
	fn admit(&mut self, group: i32, coord_ms: u64, value: i64) -> bool {
		let mut admitted = false;
		for window in self.grid.windows_of(coord_ms) {
			if self.is_closed(window) {
				continue;
			}
			self.contributions.push(Contribution {
				group,
				window,
				value,
				live: true,
			});
			admitted = true;
		}
		admitted
	}

	fn retract(&mut self, group: i32, coord_ms: u64, value: i64) {
		for window in self.grid.windows_of(coord_ms) {
			if self.is_closed(window) {
				continue;
			}
			if let Some(c) = self
				.contributions
				.iter_mut()
				.find(|c| c.live && c.group == group && c.window == window && c.value == value)
			{
				c.live = false;
			}
		}
	}

	fn advance_ledger(&mut self, at_ms: u64) {
		self.ledger = self.ledger.max(at_ms);
	}

	fn live(&self) -> Vec<Vec<Value>> {
		render(self.totals().into_iter().filter(|((_, window), _)| !self.is_closed(*window)))
	}

	fn all(&self) -> Vec<Vec<Value>> {
		render(self.totals().into_iter())
	}

	fn after_drain(&self) -> Vec<Vec<Value>> {
		Vec::new()
	}
}

pub fn render(entries: impl Iterator<Item = ((i32, u64), i64)>) -> Vec<Vec<Value>> {
	let mut out: Vec<Vec<Value>> =
		entries.map(|((group, _), total)| vec![Value::Int4(group), Value::Int16(total as i128)]).collect();
	out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
	out
}
