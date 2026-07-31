// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What each join strategy owes its consumer, described as the table the consumer sees.
//!
//! Both oracles below claim the whole view exactly, with no gap between the bounds the driver
//! checks. A join publishes everything it owes inside the `apply` that caused it - there is no tick,
//! no horizon and nothing in flight - so a view that merely lags is already a divergence.

use std::collections::{BTreeMap, BTreeSet};

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_testing_chaos::operator::{
	compare::Tolerances,
	expectation::ViewClaim,
	model::Model,
	reclaim::Reclaimed,
	view::{MaterializedRow, MaterializedView, OutputKey},
};
use reifydb_value::value::{Value, value_type::ValueType};

use crate::operators::join::workload::{JoinRow, LEFT_COLUMNS, RIGHT_COLUMNS, Side};

/// The `Value::None` the join fills an unmatched right column with.
///
/// Which `None` variant that is depends on how a buffer of the column's type represents absence, so
/// this builds it the way the operator does - `ColumnBuffer::with_capacity` of the right schema's
/// type, then `push_value(Value::none())` - rather than naming a variant and hoping it matches.
/// Naming the wrong one would make every unmatched row read as divergent for a reason that has
/// nothing to do with the join.
fn absent(ty: ValueType) -> Value {
	let mut buffer = ColumnBuffer::with_capacity(ty, 1);
	buffer.push_value(Value::none());
	buffer.get_value(0)
}

fn key_value(key: Option<i32>) -> Value {
	match key {
		Some(key) => Value::Int4(key),
		None => absent(ValueType::Int4),
	}
}

fn output_columns() -> Vec<String> {
	LEFT_COLUMNS
		.iter()
		.map(|(name, _)| (*name).to_string())
		.chain(RIGHT_COLUMNS.iter().map(|(name, _)| format!("other_{name}")))
		.collect()
}

fn left_pairs(left: &JoinRow) -> Vec<(String, Value)> {
	vec![
		("lid".to_string(), Value::Int8(left.number.0 as i64)),
		("k".to_string(), key_value(left.key)),
		("lv".to_string(), Value::Int8(left.value)),
	]
}

fn joined(left: &JoinRow, right: &JoinRow) -> MaterializedRow {
	let mut pairs = left_pairs(left);
	pairs.push(("other_rid".to_string(), Value::Int8(right.number.0 as i64)));
	pairs.push(("other_k".to_string(), key_value(right.key)));
	pairs.push(("other_rv".to_string(), Value::Int8(right.value)));
	MaterializedRow::from_pairs(pairs)
}

fn unmatched(left: &JoinRow) -> MaterializedRow {
	let mut pairs = left_pairs(left);
	pairs.push(("other_rid".to_string(), absent(ValueType::Int8)));
	pairs.push(("other_k".to_string(), absent(ValueType::Int4)));
	pairs.push(("other_rv".to_string(), absent(ValueType::Int8)));
	MaterializedRow::from_pairs(pairs)
}

fn empty_view() -> MaterializedView {
	let mut view = MaterializedView::empty();
	view.columns = output_columns();
	view
}

/// What a reclaiming run reached, measured at the end of it.
///
/// `reached` is cumulative over the whole run and `unconstrained`/`pinned` describe the view as it
/// finally stands, so the three are not interchangeable: a run can reach dozens of keys over sixty
/// steps and still end with a small view, and comparing the running total against the final snapshot
/// would fail a perfectly good run.
#[derive(Debug, Clone, Copy)]
pub struct Envelope {
	/// Output keys the sweep put beyond the claim at any point in the run.
	pub reached: usize,

	/// Of the view as it stands, how many keys the claim no longer constrains.
	pub unconstrained: usize,

	/// Of the view as it stands, how many keys the claim still pins exactly.
	pub pinned: usize,
}

/// The two hash strategies: every live right row that shares a live left row's key produces an
/// output row, so the view is a pure function of the two live sets and nothing about the order they
/// arrived in survives.
pub struct HashOracle {
	left_outer: bool,
	left: BTreeMap<u64, JoinRow>,
	right: BTreeMap<u64, JoinRow>,

	/// Row numbers whose stored side state a sweep may already have erased.
	///
	/// Monotone: a later write does not clear it, because the only path that writes an existing row
	/// number again is an update, and an update to a row the sweep erased updates nothing - the store
	/// has no row under that number to update - so the state stays gone.
	exposed: BTreeSet<u64>,

	/// The newest event position ever written to each (join key, side), live or since removed.
	///
	/// The sweep's unit is the interned group, and a join interns one group per join key - but each
	/// SIDE of that group carries its own activity stamp and ages on its own declared ttl, so one
	/// side can be retired while the other stays live. Folding the two into one number per key
	/// leaves a quiet right row constrained because busy left traffic kept the key looking fresh,
	/// and the claim then requires a pair whose right state is already gone.
	///
	/// Exposing per ROW would be wrong in the other direction: a key is retired whole, so a freshly
	/// written row is erased along with the older ones it shares a side with. Removals count too - a
	/// remove writes to the group and stamps it.
	key_high: BTreeMap<(i32, Side), u64>,

	/// The furthest back the mapping phase has reported reaching.
	///
	/// A pair's row-number mapping is what a withdrawal is published under, and the operator resolves
	/// it rather than minting a replacement - so once the mapping is gone the pair can no longer be
	/// retracted and whatever it published stays. That makes the mapping phase a second, independent
	/// source of stranding, on its own cutoff.
	mapping_cutoff: u64,

	/// Output keys the claim has stopped constraining, because at some point in the run one of the
	/// two rows behind them was exposed.
	///
	/// Monotone, and that is not conservatism for its own sake. A left row whose state is gone can
	/// never withdraw what it published: the operator withdraws by reading its own stored row, so a
	/// pair stranded once is stranded for the rest of the run even if the left row is later written
	/// back. A set that forgot would start requiring rows the operator has no way to produce.
	unconstrained: BTreeSet<OutputKey>,
}

impl HashOracle {
	pub fn new(left_outer: bool) -> Self {
		Self {
			left_outer,
			left: BTreeMap::new(),
			right: BTreeMap::new(),
			exposed: BTreeSet::new(),
			key_high: BTreeMap::new(),
			mapping_cutoff: 0,
			unconstrained: BTreeSet::new(),
		}
	}

	fn stamp(&mut self, row: &JoinRow) {
		if let Some(key) = row.key {
			let high = self.key_high.entry((key, row.side)).or_default();
			*high = (*high).max(row.coord_ms);
		}
	}

	/// How much of the view the sweep has put beyond the claim's reach, against how much it still
	/// pins exactly.
	///
	/// A reclaim suite needs both ends. Nothing reached means the sweep never touched a published row
	/// and the run proves nothing about reclamation; nothing pinned means the claim has stopped
	/// saying anything and the run proves nothing about the join.
	pub fn envelope(&self) -> Envelope {
		let (unconstrained, pinned) = self.pairs().into_iter().partition::<Vec<_>, _>(|(_, _, gone)| *gone);
		Envelope {
			reached: self.unconstrained.len(),
			unconstrained: unconstrained.len(),
			pinned: pinned.len(),
		}
	}

	/// Every output row the operator owes, each flagged with whether the sweep has put it beyond the
	/// claim's reach.
	///
	/// Two independent reasons a pair leaves the claim, and they are not the same cutoff. Either side
	/// having lost its stored row means the pair can no longer be computed at all; the pair having
	/// lost its row-number mapping means it can no longer be WITHDRAWN, because a retraction resolves
	/// the mapping rather than minting a replacement.
	///
	/// The mapping test is on the LOWER of the two positions. A mapping is stamped at the change
	/// coordinate of the batch that published the pair, which is at least the arriving row's own
	/// position - and the arriving row is whichever of the two came second, so its position is at
	/// least the minimum. If the minimum is past the cutoff the mapping cannot have been reached.
	fn pairs(&self) -> Vec<(OutputKey, MaterializedRow, bool)> {
		let mut out = Vec::new();
		for left in self.left.values() {
			let matches: Vec<&JoinRow> = match left.key {
				Some(key) => self.right.values().filter(|right| right.key == Some(key)).collect(),
				None => Vec::new(),
			};
			let gone = |rows: [&JoinRow; 2]| {
				rows.iter().any(|row| self.exposed.contains(&row.number.0))
					|| rows.iter().map(|row| row.coord_ms).min().unwrap_or(0) <= self.mapping_cutoff
			};
			if matches.is_empty() {
				if self.left_outer {
					out.push((
						OutputKey::new(vec![
							Value::Int8(left.number.0 as i64),
							absent(ValueType::Int8),
						]),
						unmatched(left),
						gone([left, left]),
					));
				}
				continue;
			}
			for right in matches {
				out.push((
					OutputKey::new(vec![
						Value::Int8(left.number.0 as i64),
						Value::Int8(right.number.0 as i64),
					]),
					joined(left, right),
					gone([left, right]),
				));
			}
		}
		out
	}

	fn claim(&self) -> ViewClaim {
		let mut view = empty_view();
		for (key, row, _) in self.pairs() {
			view.insert(key, row);
		}
		// A hash join mints one output row per (left, right) pair, and an unmatched left row is the
		// pair (left, nothing). Keying on those two columns is what lets the claim be compared
		// without predicting the row numbers the operator mints for them.
		ViewClaim::new(view, vec!["lid".to_string(), "other_rid".to_string()], Tolerances::new())
			.with_unconstrained(self.unconstrained.clone())
	}
}

impl Model<JoinRow> for HashOracle {
	type Expectation = ViewClaim;

	fn admit(&mut self, row: &JoinRow) -> bool {
		self.stamp(row);
		match row.side {
			Side::Left => self.left.insert(row.number.0, row.clone()),
			Side::Right => self.right.insert(row.number.0, row.clone()),
		};
		true
	}

	fn retract(&mut self, row: &JoinRow) {
		self.stamp(row);
		match row.side {
			Side::Left => self.left.remove(&row.number.0),
			Side::Right => self.right.remove(&row.number.0),
		};
	}

	fn advance_ledger(&mut self, _at_ms: u64) {}

	fn reclaimed(&mut self, swept: &Reclaimed) {
		if let Some(mapping) = swept.cutoffs.mapping {
			self.mapping_cutoff = self.mapping_cutoff.max(mapping);
		}
		// The keyspace phase is what ages a join: each side's rows live in their own keyspace and
		// retire on that side's declared ttl. The data phase is folded in through `state_cutoff_ms`
		// because a join declares one when either ttl is set, and it reaches the same group range.
		let Some(cutoff) = swept.state_cutoff_ms() else {
			return;
		};
		let retired: BTreeSet<(i32, Side)> =
			self.key_high.iter().filter(|(_, high)| **high <= cutoff).map(|(side, _)| *side).collect();
		for row in self.left.values().chain(self.right.values()) {
			if row.key.is_some_and(|key| retired.contains(&(key, row.side))) {
				self.exposed.insert(row.number.0);
			}
		}
	}

	fn step_complete(&mut self) {
		// Folded here rather than inside `reclaimed` for two reasons, and both are needed. A key can be
		// formed AFTER the sweep that stranded one of its rows - a right row arriving to match a left
		// row whose state is already gone. And a key must be recorded BEFORE the row that carries it is
		// removed, because that removal is exactly when the stranding becomes visible and the pair
		// leaves the model's own claim.
		if self.exposed.is_empty() && self.mapping_cutoff == 0 {
			return;
		}
		let reached: Vec<OutputKey> =
			self.pairs().into_iter().filter(|(_, _, gone)| *gone).map(|(key, _, _)| key).collect();
		self.unconstrained.extend(reached);

		// An exposed left row owns a second output key the pair list cannot show: its UNMATCHED form.
		// A left join publishes `(lid, none)` while a key has no right rows and `(lid, rid)` once it
		// does, so reclaiming either side moves a row between the two. Marking only the form the row
		// happens to be in right now leaves the other one constrained, and the claim then requires a
		// row whose supporting state is already gone.
		let stranded: Vec<OutputKey> = self
			.left
			.values()
			.filter(|left| self.exposed.contains(&left.number.0))
			.map(|left| OutputKey::new(vec![Value::Int8(left.number.0 as i64), absent(ValueType::Int8)]))
			.collect();
		self.unconstrained.extend(stranded);
	}

	fn live(&self) -> ViewClaim {
		self.claim()
	}

	fn all(&self) -> ViewClaim {
		self.claim()
	}

	fn after_drain(&self) -> ViewClaim {
		self.claim()
	}
}

/// The two latest strategies: the right side is one slot per key holding whichever right row was
/// written to it last, and every live left row reads that slot.
///
/// Unlike the hash oracle this is not a function of the live sets. A right removal clears the slot
/// for its key even when the row occupying the slot is a different one that is still live, so the
/// slot is history and has to be tracked as the driver replays it.
pub struct LatestOracle {
	left_outer: bool,
	left: BTreeMap<u64, JoinRow>,
	slot: BTreeMap<i32, JoinRow>,
}

impl LatestOracle {
	pub fn new(left_outer: bool) -> Self {
		Self {
			left_outer,
			left: BTreeMap::new(),
			slot: BTreeMap::new(),
		}
	}

	fn claim(&self) -> ViewClaim {
		let mut view = empty_view();
		for left in self.left.values() {
			let key = OutputKey::new(vec![Value::Int8(left.number.0 as i64)]);
			match left.key.and_then(|key| self.slot.get(&key)) {
				Some(right) => view.insert(key, joined(left, right)),
				None if self.left_outer => view.insert(key, unmatched(left)),
				None => {}
			}
		}
		// Latest reuses the left row's own number for the row it emits rather than minting one per
		// pair, so a left row is at most one output row and its identity is the left row.
		ViewClaim::new(view, vec!["lid".to_string()], Tolerances::new())
	}
}

impl Model<JoinRow> for LatestOracle {
	type Expectation = ViewClaim;

	fn admit(&mut self, row: &JoinRow) -> bool {
		match row.side {
			Side::Left => {
				self.left.insert(row.number.0, row.clone());
			}
			// A right row with an undefined key never reaches a slot, so it is a complete no-op
			// on this side - not an occupant of some undefined-keyed slot.
			Side::Right => {
				if let Some(key) = row.key {
					self.slot.insert(key, row.clone());
				}
			}
		}
		true
	}

	fn retract(&mut self, row: &JoinRow) {
		match row.side {
			Side::Left => {
				self.left.remove(&row.number.0);
			}
			Side::Right => {
				if let Some(key) = row.key {
					self.slot.remove(&key);
				}
			}
		}
	}

	fn advance_ledger(&mut self, _at_ms: u64) {}

	fn live(&self) -> ViewClaim {
		self.claim()
	}

	fn all(&self) -> ViewClaim {
		self.claim()
	}

	fn after_drain(&self) -> ViewClaim {
		self.claim()
	}
}

/// A snapshot join: a left row is joined against the right side as it stands when that left row is
/// touched, and is never revisited when the right side later moves on.
///
/// Unlike the other two this tracks the published table directly rather than deriving it, because
/// under snapshot the table is not a function of any current state - it is a function of what the
/// right side happened to hold at each left row's last touch. Two runs with identical live sets can
/// legitimately hold different tables.
pub struct SnapshotOracle {
	left_outer: bool,
	latest: bool,
	right: BTreeMap<u64, JoinRow>,
	slot: BTreeMap<i32, JoinRow>,
	published: BTreeMap<OutputKey, MaterializedRow>,
}

impl SnapshotOracle {
	pub fn new(left_outer: bool, latest: bool) -> Self {
		Self {
			left_outer,
			latest,
			right: BTreeMap::new(),
			slot: BTreeMap::new(),
			published: BTreeMap::new(),
		}
	}

	fn key_columns(&self) -> Vec<String> {
		match self.latest {
			true => vec!["lid".to_string()],
			false => vec!["lid".to_string(), "other_rid".to_string()],
		}
	}

	fn output_key(&self, left: &JoinRow, right: Option<&JoinRow>) -> OutputKey {
		let lid = Value::Int8(left.number.0 as i64);
		match self.latest {
			true => OutputKey::new(vec![lid]),
			false => OutputKey::new(vec![
				lid,
				match right {
					Some(right) => Value::Int8(right.number.0 as i64),
					None => absent(ValueType::Int8),
				},
			]),
		}
	}

	fn matches(&self, left: &JoinRow) -> Vec<&JoinRow> {
		let Some(key) = left.key else {
			return Vec::new();
		};
		match self.latest {
			true => self.slot.get(&key).into_iter().collect(),
			false => self.right.values().filter(|right| right.key == Some(key)).collect(),
		}
	}

	fn withdraw(&mut self, left: &JoinRow) {
		let lid = Value::Int8(left.number.0 as i64);
		self.published.retain(|key, _| key.as_slice().first() != Some(&lid));
	}

	fn republish(&mut self, left: &JoinRow) {
		self.withdraw(left);
		let rows: Vec<(OutputKey, MaterializedRow)> = match self.matches(left).as_slice() {
			[] if self.left_outer => vec![(self.output_key(left, None), unmatched(left))],
			[] => Vec::new(),
			matched => matched
				.iter()
				.map(|right| (self.output_key(left, Some(right)), joined(left, right)))
				.collect(),
		};
		self.published.extend(rows);
	}
}

impl Model<JoinRow> for SnapshotOracle {
	type Expectation = ViewClaim;

	fn admit(&mut self, row: &JoinRow) -> bool {
		match row.side {
			// A right arrival moves the state the NEXT left touch will read, and nothing else. Not
			// republishing here is the whole of what `snapshot` means.
			Side::Right => match (self.latest, row.key) {
				(true, Some(key)) => {
					self.slot.insert(key, row.clone());
				}
				(false, _) => {
					self.right.insert(row.number.0, row.clone());
				}
				(true, None) => {}
			},
			Side::Left => self.republish(row),
		}
		true
	}

	fn retract(&mut self, row: &JoinRow) {
		match row.side {
			Side::Right => match (self.latest, row.key) {
				(true, Some(key)) => {
					self.slot.remove(&key);
				}
				(false, _) => {
					self.right.remove(&row.number.0);
				}
				(true, None) => {}
			},
			// A left row takes exactly what it published with it, whatever the right side has done
			// since. Recomputing the withdrawal from the current right side is what leaves rows
			// stranded in the view.
			Side::Left => self.withdraw(row),
		}
	}

	fn advance_ledger(&mut self, _at_ms: u64) {}

	fn live(&self) -> ViewClaim {
		self.all()
	}

	fn all(&self) -> ViewClaim {
		let mut view = empty_view();
		for (key, row) in &self.published {
			view.insert(key.clone(), row.clone());
		}
		ViewClaim::new(view, self.key_columns(), Tolerances::new())
	}

	fn after_drain(&self) -> ViewClaim {
		self.all()
	}
}
