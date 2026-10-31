// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	mem::take,
};

use reifydb_flow_async::operator::state::seal::coord::Coord;
use reifydb_value::value::row_number::RowNumber;

pub struct RollingRules<C: Coord> {
	pub size: C::Span,
	pub lateness: C::Span,
	pub retract: C::Span,
}

struct RollingModelGroup<C, V> {
	newest: C,
	rows: BTreeMap<RowNumber, (C, V)>,
	leftovers: Vec<(C, V)>,
}

impl<C, V> RollingModelGroup<C, V> {
	fn is_empty(&self) -> bool {
		self.rows.is_empty() && self.leftovers.is_empty()
	}
}

pub enum RollingApplied<G, C, V> {
	Add(G, C, V),
	Remove(G, C, V),
	Cut(G, C),
}

enum RollingLeg<C, V> {
	Add(C, V),
	Remove,
}

pub struct RollingModel<G, C: Coord, V> {
	rules: RollingRules<C>,
	cutoff: Option<C>,
	groups: BTreeMap<G, RollingModelGroup<C, V>>,
	pending: Vec<(G, RowNumber, RollingLeg<C, V>)>,
}

impl<G: Ord + Clone, C: Coord, V: Clone> RollingModel<G, C, V> {
	pub fn new(rules: RollingRules<C>) -> Self {
		Self {
			rules,
			cutoff: None,
			groups: BTreeMap::new(),
			pending: Vec::new(),
		}
	}

	fn admits_at(rules: &RollingRules<C>, clock: Option<C>, newest: Option<C>, pane: C, retraction: bool) -> bool {
		let own = newest.and_then(|newest| newest.checked_sub_span(rules.size));
		let cutoff = match (clock, own) {
			(Some(clock), Some(own)) => Some(clock.max(own)),
			(clock, own) => clock.or(own),
		};
		if cutoff.is_some_and(|cutoff| pane <= cutoff) {
			return false;
		}
		let Some(newest) = newest else {
			return true;
		};
		let line = if retraction {
			rules.retract
		} else {
			rules.lateness
		};
		pane >= newest.saturating_sub_span(line)
	}

	fn admits(&self, group: &G, pane: C, retraction: bool) -> bool {
		Self::admits_at(
			&self.rules,
			self.cutoff,
			self.groups.get(group).map(|state| state.newest),
			pane,
			retraction,
		)
	}

	pub fn insert(&mut self, group: G, row: RowNumber, pane: C, value: V) -> bool {
		if !self.admits(&group, pane, false) {
			return false;
		}
		self.pending.push((group, row, RollingLeg::Add(pane, value)));
		true
	}

	pub fn remove(&mut self, group: G, row: RowNumber, pane: C) {
		if self.admits(&group, pane, true) {
			self.pending.push((group, row, RollingLeg::Remove));
		}
	}

	pub fn update(&mut self, pre: (G, RowNumber, C), post: (G, RowNumber, C, V)) {
		let (pre_group, pre_row, pre_pane) = pre;
		let (post_group, post_row, post_pane, value) = post;
		if pre_group != post_group {
			self.remove(pre_group, pre_row, pre_pane);
			self.insert(post_group, post_row, post_pane, value);
			return;
		}
		if self.admits(&pre_group, pre_pane, true) && self.admits(&post_group, post_pane, false) {
			self.pending.push((pre_group, pre_row, RollingLeg::Remove));
			self.pending.push((post_group, post_row, RollingLeg::Add(post_pane, value)));
		}
	}

	pub fn step(&mut self) -> Vec<RollingApplied<G, C, V>> {
		let mut applied = Vec::new();
		let start: BTreeMap<G, C> =
			self.groups.iter().map(|(group, state)| (group.clone(), state.newest)).collect();
		let mut touched = BTreeSet::new();
		for (group, row, leg) in take(&mut self.pending) {
			let changed = match leg {
				RollingLeg::Add(pane, value) => {
					let state =
						self.groups.entry(group.clone()).or_insert_with(|| RollingModelGroup {
							newest: pane,
							rows: BTreeMap::new(),
							leftovers: Vec::new(),
						});
					if let Some(old) = state.rows.insert(row, (pane, value.clone())) {
						state.leftovers.push(old);
					}
					applied.push(RollingApplied::Add(group.clone(), pane, value));
					Some(pane)
				}
				RollingLeg::Remove => {
					let newest = start.get(&group).copied();
					match self
						.groups
						.get_mut(&group)
						.and_then(|state| state.rows.remove(&row).map(|held| (state, held)))
					{
						Some((_, (pane, value)))
							if Self::admits_at(
								&self.rules,
								self.cutoff,
								newest,
								pane,
								true,
							) =>
						{
							applied.push(RollingApplied::Remove(
								group.clone(),
								pane,
								value,
							));
							Some(pane)
						}
						Some((state, held)) => {
							state.leftovers.push(held);
							None
						}
						None => None,
					}
				}
			};
			if let Some(pane) = changed
				&& let Some(state) = self.groups.get_mut(&group)
			{
				state.newest = state.newest.max(pane);
				touched.insert(group);
			}
		}
		for group in touched {
			let own = self
				.groups
				.get(&group)
				.and_then(|state| state.newest.checked_sub_span(self.rules.size));
			let cutoff = match (self.cutoff, own) {
				(Some(clock), Some(own)) => Some(clock.max(own)),
				(clock, own) => clock.or(own),
			};
			self.evict(&group, cutoff, &mut applied);
		}
		applied
	}

	pub fn advance_cutoff(&mut self, cutoff: C) -> Vec<RollingApplied<G, C, V>> {
		let mut applied = Vec::new();
		self.cutoff = Some(self.cutoff.map_or(cutoff, |current| current.max(cutoff)));
		let groups: Vec<G> = self.groups.keys().cloned().collect();
		for group in groups {
			self.evict(&group, self.cutoff, &mut applied);
		}
		applied
	}

	fn evict(&mut self, group: &G, cutoff: Option<C>, applied: &mut Vec<RollingApplied<G, C, V>>) {
		let clock = self.cutoff;
		let Some(state) = self.groups.get_mut(group) else {
			return;
		};
		if let Some(cutoff) = cutoff {
			state.rows.retain(|_, (pane, _)| *pane > cutoff);
			state.leftovers.retain(|(pane, _)| *pane > cutoff);
			applied.push(RollingApplied::Cut(group.clone(), cutoff));
		}
		if state.is_empty() && clock.is_some_and(|clock| clock >= state.newest) {
			self.groups.remove(group);
		}
	}

	pub fn groups(&self) -> impl Iterator<Item = (&G, Vec<(&C, &V)>)> {
		self.groups.iter().filter(|(_, state)| !state.is_empty()).map(|(group, state)| {
			let held = state
				.rows
				.values()
				.chain(state.leftovers.iter())
				.map(|(pane, value)| (pane, value))
				.collect();
			(group, held)
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

	use super::{RollingModel, RollingRules};

	fn at(ms: i64) -> DateTime {
		DateTime::from_epoch_millis(ms).unwrap()
	}

	fn model(size_ms: i64, lateness_ms: i64, retract_ms: i64) -> RollingModel<i32, DateTime, i64> {
		RollingModel::new(RollingRules {
			size: Duration::from_milliseconds(size_ms).unwrap(),
			lateness: Duration::from_milliseconds(lateness_ms).unwrap(),
			retract: Duration::from_milliseconds(retract_ms).unwrap(),
		})
	}

	fn held(model: &RollingModel<i32, DateTime, i64>) -> Vec<(i32, Vec<i64>)> {
		let mut out: Vec<(i32, Vec<i64>)> = model
			.groups()
			.map(|(group, rows)| {
				let mut values: Vec<i64> = rows.into_iter().map(|(_, value)| *value).collect();
				values.sort();
				(*group, values)
			})
			.collect();
		out.sort();
		out
	}

	#[test]
	fn a_remove_takes_the_value_the_row_holds_not_the_one_the_event_carries() {
		// A dropped update leaves 30 held at 16s; the remove names 19s and must still take the 30 out.
		let mut model = model(30_000, 0, 5_000);
		model.insert(1, RowNumber(2), at(16_000), 30);
		model.insert(1, RowNumber(3), at(16_000), 5);
		model.step();
		model.insert(1, RowNumber(1), at(20_000), 10);
		model.step();
		model.update((1, RowNumber(2), at(16_000)), (1, RowNumber(2), at(19_000), 50));
		model.step();
		assert_eq!(held(&model), vec![(1, vec![5, 10, 30])], "the update past the seal line must be dropped");

		model.remove(1, RowNumber(2), at(19_000));
		model.step();
		assert_eq!(held(&model), vec![(1, vec![5, 10])]);
	}

	#[test]
	fn a_remove_of_a_row_held_in_a_locked_pane_keeps_its_value() {
		// The row sits at 10s, below the retract line 15s; a remove naming an open pane must not change it.
		let mut model = model(30_000, 0, 5_000);
		model.insert(1, RowNumber(2), at(10_000), 30);
		model.step();
		model.insert(1, RowNumber(1), at(20_000), 10);
		model.step();
		model.update((1, RowNumber(2), at(10_000)), (1, RowNumber(2), at(20_000), 50));
		model.step();

		model.remove(1, RowNumber(2), at(20_000));
		model.step();
		assert_eq!(held(&model), vec![(1, vec![10, 30])]);
	}

	#[test]
	fn the_clock_cutoff_rejects_an_insert_the_group_line_alone_admits() {
		// Group 1 is young (newest 5s, own cutoff none) but the clock cutoff at 10s must still reject 8s.
		let mut model = model(30_000, 10_000, 10_000);
		model.insert(1, RowNumber(1), at(5_000), 1);
		model.step();
		model.advance_cutoff(at(10_000));

		assert!(!model.insert(1, RowNumber(2), at(8_000), 2), "8s is at or below the clock cutoff");
		assert!(model.insert(1, RowNumber(3), at(11_000), 3));
	}

	#[test]
	fn an_emptied_group_is_forgotten_only_once_the_clock_passes_its_newest() {
		// While the group is remembered its seal line drops 15s; once forgotten the same insert is admitted.
		let mut model = model(30_000, 0, 0);
		model.insert(1, RowNumber(1), at(20_000), 1);
		model.step();
		model.remove(1, RowNumber(1), at(20_000));
		model.step();

		model.advance_cutoff(at(10_000));
		assert!(!model.insert(1, RowNumber(2), at(15_000), 2), "an emptied group must keep its seal line");

		model.advance_cutoff(at(20_000));
		assert!(
			model.insert(1, RowNumber(3), at(25_000), 3),
			"a forgotten group must admit from the clock alone"
		);
		assert!(!model.insert(1, RowNumber(4), at(20_000), 4), "the clock cutoff still applies");
	}

	#[test]
	fn a_remove_moves_newest_only_by_the_pane_it_changed_not_the_pane_it_named() {
		// The remove names 25s but changes nothing; bumping newest to 25s would seal 17s out.
		let mut model = model(30_000, 5_000, 5_000);
		model.insert(1, RowNumber(2), at(10_000), 30);
		model.step();
		model.insert(1, RowNumber(1), at(20_000), 10);
		model.step();
		model.update((1, RowNumber(2), at(10_000)), (1, RowNumber(2), at(25_000), 50));
		model.step();

		model.remove(1, RowNumber(2), at(25_000));
		model.step();
		assert!(
			model.insert(1, RowNumber(5), at(17_000), 7),
			"newest must stay at 20s, so 17s passes the seal line"
		);
	}

	#[test]
	fn a_reused_row_number_keeps_the_old_value_until_its_pane_is_cut() {
		// The first life's remove is stale, so its 30 stays in 80ms while the second life lives at 90ms.
		let mut model = model(30, 0, 0);
		model.insert(1, RowNumber(26), at(80), 30);
		model.step();
		model.remove(1, RowNumber(26), at(0));
		model.step();
		model.insert(1, RowNumber(26), at(90), 50);
		model.step();
		assert_eq!(held(&model), vec![(1, vec![30, 50])]);

		model.remove(1, RowNumber(26), at(90));
		model.step();
		assert_eq!(held(&model), vec![(1, vec![30])], "the remove takes the second life only");

		model.advance_cutoff(at(80));
		assert!(held(&model).is_empty(), "the leftover dies with its pane");
	}

	#[test]
	fn a_remove_of_a_row_whose_insert_was_dropped_changes_nothing() {
		// The insert at 17s is under the seal line 20s; the remove passes the retract line 15s but holds
		// nothing.
		let mut model = model(30_000, 0, 5_000);
		model.insert(1, RowNumber(3), at(17_000), 5);
		model.step();
		model.insert(1, RowNumber(1), at(20_000), 10);
		model.step();
		assert!(!model.insert(1, RowNumber(2), at(17_000), 30));
		model.step();

		model.remove(1, RowNumber(2), at(17_000));
		model.step();
		assert_eq!(held(&model), vec![(1, vec![5, 10])]);
	}
}
