// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_testing_chaos::operator::model::Model;
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::{framework::workload::WindowRow, operators::window::grid::Fold};

/// A rolling group trails its own newest coordinate; every leg of a batch is judged against the state the batch began
/// with.
pub struct Oracle {
	size_ms: u64,
	lateness_ms: u64,
	retract_ms: Option<u64>,
	ledger: u64,
	groups: BTreeMap<i32, Group>,
	pending: Vec<Leg>,
	fold: Fold,
}

#[derive(Default)]
struct Group {
	newest: u64,
	rows: BTreeMap<RowNumber, (u64, i64)>,
}

enum Leg {
	Add(WindowRow),
	Remove(WindowRow),
}

impl Oracle {
	pub fn new(size_ms: u64, lateness_ms: u64) -> Self {
		Self {
			size_ms,
			lateness_ms,
			retract_ms: None,
			ledger: 0,
			groups: BTreeMap::new(),
			pending: Vec::new(),
			fold: Fold::Sum,
		}
	}

	/// Sum stays the default so the pinned rolling corpora keep the oracle they were recorded against.
	pub fn with_fold(mut self, fold: Fold) -> Self {
		self.fold = fold;
		self
	}

	/// A retraction is admitted back to immutable, an insert only back to lateness.
	pub fn with_immutable(mut self, immutable_ms: Option<u64>) -> Self {
		self.retract_ms = immutable_ms;
		self
	}

	fn clock_cutoff(&self) -> Option<u64> {
		self.ledger.checked_sub(self.size_ms)
	}

	fn admits(&self, group: i32, coord: u64, retraction: bool) -> bool {
		let Some(state) = self.groups.get(&group) else {
			return self.clock_cutoff().is_none_or(|cutoff| coord > cutoff);
		};
		let cutoff = self.clock_cutoff().max(state.newest.checked_sub(self.size_ms));
		if cutoff.is_some_and(|cutoff| coord <= cutoff) {
			return false;
		}
		let line = match retraction {
			true => self.retract_ms.unwrap_or(self.lateness_ms),
			false => self.lateness_ms,
		};
		coord >= state.newest.saturating_sub(line)
	}

	fn evict(&mut self, group: i32, cutoff: Option<u64>) {
		let clock = self.clock_cutoff();
		if let Some(state) = self.groups.get_mut(&group) {
			if let Some(cutoff) = cutoff {
				state.rows.retain(|_, (coord, _)| *coord > cutoff);
			}
			// An emptied group must keep its newest until the clock passes it, otherwise evicted rows are
			// let back in.
			if state.rows.is_empty() && clock.is_some_and(|clock| clock >= state.newest) {
				self.groups.remove(&group);
			}
		}
	}

	fn folded(&self) -> Vec<Vec<Value>> {
		let mut out: Vec<Vec<Value>> = self
			.groups
			.iter()
			.filter(|(_, state)| !state.rows.is_empty())
			.map(|(group, state)| {
				let values: Vec<i64> = state.rows.values().map(|(_, value)| *value).collect();
				vec![Value::Int4(*group), self.fold.apply(&values)]
			})
			.collect();
		out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
		out
	}
}

impl Model<WindowRow> for Oracle {
	type Expectation = Vec<Vec<Value>>;

	fn admit(&mut self, event: &WindowRow) -> bool {
		if !self.admits(event.group, event.coord_ms, false) {
			return false;
		}
		self.pending.push(Leg::Add(event.clone()));
		true
	}

	fn retract(&mut self, event: &WindowRow) {
		if self.admits(event.group, event.coord_ms, true) {
			self.pending.push(Leg::Remove(event.clone()));
		}
	}

	fn update(&mut self, pre: &WindowRow, post: &WindowRow) {
		if pre.group != post.group {
			self.retract(pre);
			self.admit(post);
			return;
		}
		if self.admits(pre.group, pre.coord_ms, true) && self.admits(post.group, post.coord_ms, false) {
			self.pending.push(Leg::Remove(pre.clone()));
			self.pending.push(Leg::Add(post.clone()));
		}
	}

	fn step_complete(&mut self) {
		let mut touched: BTreeMap<i32, u64> = BTreeMap::new();
		for leg in std::mem::take(&mut self.pending) {
			let (row, applied) = match leg {
				Leg::Add(row) => {
					let state = self.groups.entry(row.group).or_default();
					state.rows.insert(row.number, (row.coord_ms, row.value));
					(row, true)
				}
				Leg::Remove(row) => {
					let applied = self
						.groups
						.get_mut(&row.group)
						.is_some_and(|state| state.rows.remove(&row.number).is_some());
					(row, applied)
				}
			};
			if applied {
				let newest = touched.entry(row.group).or_insert(row.coord_ms);
				*newest = (*newest).max(row.coord_ms);
			}
		}
		let clock = self.clock_cutoff();
		for (group, newest) in touched {
			let Some(state) = self.groups.get_mut(&group) else {
				continue;
			};
			state.newest = state.newest.max(newest);
			let cutoff = clock.max(state.newest.checked_sub(self.size_ms));
			self.evict(group, cutoff);
		}
	}

	fn advance_ledger(&mut self, at_ms: u64) {
		self.ledger = self.ledger.max(at_ms);
		let groups: Vec<i32> = self.groups.keys().copied().collect();
		for group in groups {
			self.evict(group, self.clock_cutoff());
		}
	}

	fn live(&self) -> Vec<Vec<Value>> {
		self.folded()
	}

	fn all(&self) -> Vec<Vec<Value>> {
		// Must-publish and may-publish must be the same set, otherwise the two containment checks disagree.
		self.live()
	}

	fn after_drain(&self) -> Vec<Vec<Value>> {
		Vec::new()
	}
}
