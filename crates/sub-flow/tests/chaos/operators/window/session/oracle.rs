// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What a session window owes: one aggregate per stretch of live rows that never went quiet for the gap.
//!
//! Restated from the rule, not from the operator: the expectation is a fresh run over the rows that are
//! live now, sorted by time and cut wherever two neighbours are at least a gap apart. Removes and moved
//! times therefore shrink, split and merge sessions exactly as a rerun from scratch would. Sealing is not
//! driven here, so no session is ever closed and no row is ever refused.
//!
//! A group with three sessions is three rows sharing a group value, each with its own end, so the
//! expectation is keyed on the group plus `window::end()`.

use std::collections::BTreeMap;

use reifydb_testing_chaos::operator::{expectation::KeyedMultiset, model::Model, view::RowKey};
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::{framework::workload::WindowRow, operators::window::grid::Fold};

struct LiveRow {
	group: i32,
	coord: u64,
	value: i64,
}

pub struct SessionOracle {
	gap_ms: u64,
	fold: Fold,
	rows: BTreeMap<RowNumber, LiveRow>,
}

impl SessionOracle {
	pub fn new(gap_ms: u64, fold: Fold) -> Self {
		Self {
			gap_ms,
			fold,
			rows: BTreeMap::new(),
		}
	}

	fn folded(&self) -> Vec<Vec<Value>> {
		let mut by_group: BTreeMap<i32, Vec<(u64, i64)>> = BTreeMap::new();
		for row in self.rows.values() {
			by_group.entry(row.group).or_default().push((row.coord, row.value));
		}
		let mut out: Vec<Vec<Value>> = Vec::new();
		for (group, mut points) in by_group {
			points.sort();
			let mut session: Vec<i64> = Vec::new();
			let mut previous: Option<u64> = None;
			for (coord, value) in points {
				if previous.is_some_and(|previous| coord > previous && coord - previous >= self.gap_ms)
				{
					out.push(vec![Value::Int4(group), self.fold.apply(&session)]);
					session.clear();
				}
				session.push(value);
				previous = Some(coord);
			}
			out.push(vec![Value::Int4(group), self.fold.apply(&session)]);
		}
		out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
		out
	}
}

/// The group plus the published `window::end()`: a group's session ends strictly increase. Keying on the
/// group alone would let a group's sessions satisfy each other's totals.
fn session_row_key() -> RowKey {
	RowKey::columns(["g", "e"])
}

impl Model<WindowRow> for SessionOracle {
	type Expectation = KeyedMultiset;

	fn admit(&mut self, event: &WindowRow) -> bool {
		let WindowRow {
			number,
			group,
			coord_ms,
			value,
		} = *event;
		let previous = self.rows.insert(
			number,
			LiveRow {
				group,
				coord: coord_ms,
				value,
			},
		);
		assert!(previous.is_none(), "the driver admits row {number:?} while it is still live");
		true
	}

	fn retract(&mut self, event: &WindowRow) {
		// Withdrawing a row must let its session shrink or split, since a fresh run never saw it.
		let removed = self.rows.remove(&event.number);
		let removed = removed
			.unwrap_or_else(|| panic!("the driver retracts row {:?} it never admitted", event.number));
		assert_eq!(
			removed.value, event.value,
			"the driver retracts the value it last admitted for row {:?}; a mismatch means the oracle and \
			 the corpus have diverged",
			event.number
		);
	}

	fn update(&mut self, pre: &WindowRow, post: &WindowRow) {
		// A moved time or group leaves and rejoins like a fresh row; the sessions are recomputed from scratch.
		assert_eq!(pre.number, post.number, "an update must not change a row's number");
		let row = self
			.rows
			.get_mut(&pre.number)
			.unwrap_or_else(|| panic!("the driver updates row {:?} it never admitted", pre.number));
		assert_eq!(row.value, pre.value, "the driver updates from the value it last admitted");
		*row = LiveRow {
			group: post.group,
			coord: post.coord_ms,
			value: post.value,
		};
	}

	fn advance_ledger(&mut self, _at_ms: u64) {
		// The sweeps hold seal_pct at 0, so no session closes and closed-means-final never applies here.
	}

	fn live(&self) -> KeyedMultiset {
		self.all()
	}

	fn all(&self) -> KeyedMultiset {
		KeyedMultiset::new(session_row_key(), self.folded())
	}

	fn after_drain(&self) -> KeyedMultiset {
		self.all()
	}
}
