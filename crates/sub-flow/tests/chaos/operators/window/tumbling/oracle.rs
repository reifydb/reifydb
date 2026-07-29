// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_value::value::Value;

pub struct Oracle {
	size_ms: u64,
	cutoff_ms: u64,
	ledger: u64,
	contributions: Vec<Contribution>,
	pub dropped: u64,
}

struct Contribution {
	group: i32,
	window: u64,
	value: i64,
	live: bool,
}

impl Oracle {
	pub fn new(size_ms: u64, grace_ms: u64) -> Self {
		Self {
			size_ms,
			cutoff_ms: size_ms + grace_ms,
			ledger: 0,
			contributions: Vec::new(),
			dropped: 0,
		}
	}

	fn window_of(&self, coord_ms: u64) -> u64 {
		(coord_ms / self.size_ms) * self.size_ms
	}

	fn is_closed(&self, window: u64) -> bool {
		window.saturating_add(self.cutoff_ms).saturating_add(1) <= self.ledger
	}

	pub fn add_batch(&mut self, rows: &[(i32, u64, i64)]) -> Vec<bool> {
		self.route(rows, true)
	}

	pub fn retract_batch(&mut self, rows: &[(i32, u64, i64)]) -> Vec<bool> {
		self.route(rows, false)
	}

	fn route(&mut self, rows: &[(i32, u64, i64)], is_add: bool) -> Vec<bool> {
		let mut accepted = Vec::with_capacity(rows.len());
		for (group, coord_ms, value) in rows {
			let window = self.window_of(*coord_ms);
			if self.is_closed(window) {
				self.dropped += 1;
				accepted.push(false);
				continue;
			}
			if is_add {
				self.contributions.push(Contribution {
					group: *group,
					window,
					value: *value,
					live: true,
				});
			} else if let Some(c) = self
				.contributions
				.iter_mut()
				.find(|c| c.live && c.group == *group && c.window == window && c.value == *value)
			{
				c.live = false;
			}
			accepted.push(true);
		}
		accepted
	}

	pub fn advance_ledger(&mut self, at_ms: u64) {
		self.ledger = self.ledger.max(at_ms);
	}

	fn totals(&self) -> BTreeMap<(i32, u64), i64> {
		let mut totals: BTreeMap<(i32, u64), i64> = BTreeMap::new();
		for c in self.contributions.iter().filter(|c| c.live) {
			*totals.entry((c.group, c.window)).or_insert(0) += c.value;
		}
		totals
	}

	pub fn live(&self) -> Vec<Vec<Value>> {
		render(self.totals().into_iter().filter(|((_, window), _)| !self.is_closed(*window)))
	}

	pub fn all(&self) -> Vec<Vec<Value>> {
		render(self.totals().into_iter())
	}
}

fn render(entries: impl Iterator<Item = ((i32, u64), i64)>) -> Vec<Vec<Value>> {
	let mut out: Vec<Vec<Value>> =
		entries.map(|((group, _), total)| vec![Value::Int4(group), Value::Int16(total as i128)]).collect();
	out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
	out
}
