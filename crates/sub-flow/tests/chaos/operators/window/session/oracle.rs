// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What a session window owes: one aggregate per stretch of activity that never went quiet for
//! longer than the gap.
//!
//! The assignment rule is restated here rather than called, but `SessionKind::assign` is not what
//! this is aiming at - that has its own unit tests. What no test covers is the threading: one tracker
//! per group, shared across a batch and reloaded between them, with removes that must not move a
//! boundary and updates that must not reassign a row. A rule this short is worth restating to get at
//! that.
//!
//! Sessions are numbered per group from zero and the operator publishes each under an ordinal span,
//! so a group with three sessions is three rows sharing a group value. That is why the expectation is
//! keyed on the group plus the published time.

use std::collections::BTreeMap;

use reifydb_testing_chaos::operator::{expectation::KeyedMultiset, model::Model, view::RowKey};
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::{framework::workload::WindowRow, operators::window::grid::Fold};

/// Mirrors `SessionTracker`: `opened` is carried rather than inferred from `last == 0`, because the
/// epoch is a real coordinate a corpus does draw.
#[derive(Debug, Clone, Copy, Default)]
struct Tracker {
	session: u64,
	last: u64,
	start: u64,
	opened: bool,
}

struct Contribution {
	row: RowNumber,
	group: i32,
	session: u64,
	value: i64,
	live: bool,
}

pub struct SessionOracle {
	gap_ms: u64,
	fold: Fold,
	trackers: BTreeMap<i32, Tracker>,
	contributions: Vec<Contribution>,
}

impl SessionOracle {
	pub fn new(gap_ms: u64, fold: Fold) -> Self {
		Self {
			gap_ms,
			fold,
			trackers: BTreeMap::new(),
			contributions: Vec::new(),
		}
	}

	/// None where the row belongs to a session that has already moved on: it is dropped rather than
	/// misfiled, so it must not reach the corpus as live either.
	fn assign(&mut self, group: i32, coord: u64) -> Option<u64> {
		let gap = self.gap_ms;
		let tracker = self.trackers.entry(group).or_default();

		if !tracker.opened {
			tracker.last = coord;
			tracker.start = coord;
			tracker.opened = true;
			return Some(tracker.session);
		}
		if coord > tracker.last && coord - tracker.last > gap {
			tracker.session += 1;
			tracker.last = coord;
			tracker.start = coord;
			return Some(tracker.session);
		}
		if coord < tracker.start && tracker.start - coord > gap {
			return None;
		}
		tracker.last = tracker.last.max(coord);
		tracker.start = tracker.start.min(coord);
		Some(tracker.session)
	}

	fn folded(&self) -> Vec<Vec<Value>> {
		let mut grouped: BTreeMap<(i32, u64), Vec<i64>> = BTreeMap::new();
		for c in self.contributions.iter().filter(|c| c.live) {
			grouped.entry((c.group, c.session)).or_default().push(c.value);
		}
		let mut out: Vec<Vec<Value>> = grouped
			.into_iter()
			.map(|((group, _), values)| vec![Value::Int4(group), self.fold.apply(&values)])
			.collect();
		out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
		out
	}
}

/// The group plus the published time, which carries the session ordinal. Keying on the group alone
/// would let a group's sessions satisfy each other's totals.
fn session_row_key() -> RowKey {
	RowKey::columns(["g"]).with_time()
}

impl Model<WindowRow> for SessionOracle {
	type Expectation = KeyedMultiset;

	fn admit(&mut self, event: &WindowRow) -> bool {
		let WindowRow {
			number: row,
			group,
			coord_ms,
			value,
		} = *event;
		match self.assign(group, coord_ms) {
			Some(session) => {
				self.contributions.push(Contribution {
					row,
					group,
					session,
					value,
					live: true,
				});
				true
			}
			None => false,
		}
	}

	fn retract(&mut self, event: &WindowRow) {
		// Deliberately leaves the tracker alone. A session's boundaries are a record of what arrived,
		// not of what is still live, so withdrawing the row that opened a session must not let the
		// next arrival rejoin the one before it.
		let WindowRow {
			number: row,
			value,
			..
		} = *event;

		// A refused row has no contribution to withdraw, and the corpus still holds it because the
		// driver commits a row to its live set before the model can decline it. Tolerated for that
		// reason only: a row that WAS filed must still be found, or a divergence would go quiet.
		if let Some(c) = self.contributions.iter_mut().find(|c| c.live && c.row == row) {
			assert_eq!(
				c.value, value,
				"the driver retracts the value it last admitted for row {row:?}; a mismatch means \
				 the oracle and the corpus have diverged"
			);
			c.live = false;
		}
	}

	fn update(&mut self, pre: &WindowRow, post: &WindowRow) {
		// Not retract-then-admit. A row already filed into a session has its contribution swapped in
		// place and keeps that session; re-running the assignment would let an update rotate a
		// session, which the operator never does for a row it can still find.
		assert_eq!(pre.number, post.number, "an update must not change a row's number");
		assert_eq!(
			pre.coord_ms, post.coord_ms,
			"this oracle holds a filed row's session fixed across an update, which is only sound while \
			 the workload leaves the coordinate alone"
		);

		// `c.live` is load-bearing: a row the corpus retracted and re-admitted has more than one
		// contribution under the same number, and the dead one comes first.
		if let Some(c) = self.contributions.iter_mut().find(|c| c.live && c.row == pre.number) {
			assert_eq!(c.value, pre.value, "the driver updates from the value it last admitted");
			c.value = post.value;
			return;
		}

		// Only reachable for a row that was refused on arrival. The operator looks a row up before
		// deciding, and finding nothing sends it back through the assignment - so an update is a
		// second chance at a session, and the tracker may well have moved on enough to grant one.
		if let Some(session) = self.assign(post.group, post.coord_ms) {
			self.contributions.push(Contribution {
				row: post.number,
				group: post.group,
				session,
				value: post.value,
				live: true,
			});
		}
	}

	fn advance_ledger(&mut self, _at_ms: u64) {
		// The sweeps hold seal_pct at 0: sealing a session is the same machinery a grid window uses,
		// and modelling it here would restate that rather than the assignment this suite is for.
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
