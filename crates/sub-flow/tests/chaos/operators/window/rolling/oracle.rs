// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_testing_chaos::operator::model::Model;
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::{framework::workload::WindowRow, operators::window::grid::Fold};

/// A rolling window keeps one row per group, aggregating whatever still trails the seal ledger. The
/// boundaries are asymmetric: admission drops strictly below `ledger - size - lateness`, eviction pops at
/// or below `ledger - size`, and a ledger younger than the span evicts nothing rather than clamping.
pub struct Oracle {
	size_ms: u64,
	lateness_ms: u64,
	ledger: u64,
	contributions: Vec<Contribution>,
	fold: Fold,
}

struct Contribution {
	row: RowNumber,
	group: i32,
	coord: u64,
	value: i64,
	live: bool,
}

impl Oracle {
	pub fn new(size_ms: u64, lateness_ms: u64) -> Self {
		Self {
			size_ms,
			lateness_ms,
			ledger: 0,
			contributions: Vec::new(),
			fold: Fold::Sum,
		}
	}

	/// Sum stays the default so the pinned rolling corpora keep the oracle they were recorded against.
	/// Min and max opt in, and seal only under immutable, never by lateness alone, otherwise they stay invertible.
	pub fn with_fold(mut self, fold: Fold) -> Self {
		self.fold = fold;
		self
	}

	fn admission_horizon(&self) -> u64 {
		self.ledger.saturating_sub(self.size_ms.saturating_add(self.lateness_ms))
	}

	fn eviction_cutoff(&self) -> Option<u64> {
		self.ledger.checked_sub(self.size_ms)
	}

	fn is_late(&self, coord: u64) -> bool {
		coord < self.admission_horizon()
	}

	fn is_retained(&self, coord: u64) -> bool {
		self.eviction_cutoff().is_none_or(|cutoff| coord > cutoff)
	}

	fn folded(&self) -> Vec<Vec<Value>> {
		let mut grouped: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
		for c in self.contributions.iter().filter(|c| c.live && self.is_retained(c.coord)) {
			grouped.entry(c.group).or_default().push(c.value);
		}
		let mut out: Vec<Vec<Value>> = grouped
			.into_iter()
			.map(|(group, values)| vec![Value::Int4(group), self.fold.apply(&values)])
			.collect();
		out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
		out
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
		self.folded()
	}

	fn all(&self) -> Vec<Vec<Value>> {
		// A rolling group's value is fully determined by the ledger, so must-publish and may-publish
		// are the same set, which collapses the driver's two containment checks into an equality.
		self.live()
	}

	fn after_drain(&self) -> Vec<Vec<Value>> {
		Vec::new()
	}
}
