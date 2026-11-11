// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, mem};

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::{
	operator::state::seal::coord::Coord,
	window::span::{Slot, SlotSpan},
};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct ImmutableBase<S: Slot, V> {
	immutable: Option<SlotSpan<S>>,
	high_water: Option<S>,
	immutable_high: Option<S>,
	immutable_count: u64,
	tail: BTreeMap<S, V>,
}

impl<S: Slot, V> Default for ImmutableBase<S, V> {
	fn default() -> Self {
		Self {
			immutable: None,
			high_water: None,
			immutable_high: None,
			immutable_count: 0,
			tail: BTreeMap::new(),
		}
	}
}

impl<S: Slot, V> ImmutableBase<S, V> {
	pub fn maybe_immutable(immutable: Option<SlotSpan<S>>) -> Self {
		Self {
			immutable,
			high_water: None,
			immutable_high: None,
			immutable_count: 0,
			tail: BTreeMap::new(),
		}
	}

	pub fn push(&mut self, slot: S, value: V) -> Vec<(S, V)> {
		if matches!(self.immutable_high, Some(immutable) if slot <= immutable) {
			return Vec::new();
		}
		self.high_water = Some(match self.high_water {
			Some(hw) if hw >= slot => hw,
			_ => slot,
		});
		self.tail.insert(slot, value);
		let mut aged = Vec::new();
		let (Some(hw), Some(l)) = (self.high_water, self.immutable) else {
			return aged;
		};
		while let Some((&c, _)) = self.tail.first_key_value() {
			if hw.order_key().span_since(c.order_key()) > l {
				self.immutable_high = Some(c);
				self.immutable_count += 1;
				aged.push(self.tail.pop_first().expect("non-empty"));
			} else {
				break;
			}
		}
		aged
	}

	pub fn absorb<F>(&mut self, other: &Self, combine: F) -> Vec<(S, V)>
	where
		V: Clone,
		F: Fn(&V, &V) -> V,
	{
		self.immutable_count += other.immutable_count;
		let mut aged = Vec::new();
		for (slot, value) in &other.tail {
			let merged = match self.tail.get(slot) {
				Some(mine) => combine(mine, value),
				None => value.clone(),
			};
			aged.extend(self.absorb_push(*slot, merged));
		}
		aged
	}

	fn absorb_push(&mut self, slot: S, value: V) -> Vec<(S, V)> {
		if matches!(self.immutable_high, Some(immutable) if slot <= immutable) {
			self.immutable_count += 1;
			return vec![(slot, value)];
		}
		self.push(slot, value)
	}

	pub fn remove(&mut self, slot: &S) {
		self.tail.remove(slot);
	}

	pub fn drain_tail(&mut self) -> BTreeMap<S, V> {
		self.immutable_count += self.tail.len() as u64;
		mem::take(&mut self.tail)
	}

	pub fn tail(&self) -> &BTreeMap<S, V> {
		&self.tail
	}

	pub fn len(&self) -> u64 {
		self.immutable_count + self.tail.len() as u64
	}

	pub fn is_empty(&self) -> bool {
		self.immutable_count == 0 && self.tail.is_empty()
	}

	pub fn immutable_count(&self) -> u64 {
		self.immutable_count
	}

	pub fn is_tail_empty(&self) -> bool {
		self.tail.is_empty()
	}

	pub fn immutable(&self) -> Option<SlotSpan<S>> {
		self.immutable
	}
}

impl<S: Slot + HeapSize, V: HeapSize> HeapSize for ImmutableBase<S, V> {
	fn heap_size(&self) -> usize {
		self.high_water.heap_size() + self.immutable_high.heap_size() + self.tail.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_value::factory::time::millis;

	use super::*;
	use crate::{factory::coord::event_coord_at_millis, window::coord::DateTimeCoord};

	#[test]
	fn immutable_a_tail_matches_the_btreemap_it_replaced() {
		// a container that orders or ages differently silently changes the aggregate a window emits
		let immutable = millis(20);
		let mut base: ImmutableBase<DateTimeCoord, i64> = ImmutableBase::maybe_immutable(Some(immutable));
		let mut tail: BTreeMap<DateTimeCoord, i64> = BTreeMap::new();
		let mut high_water: Option<DateTimeCoord> = None;
		let mut immutable_high: Option<DateTimeCoord> = None;
		let mut immutable_count: u64 = 0;
		let mut aged_apart = false;

		let mut state = 0x2545_F491_4F6C_DD1Du64;
		for step in 0..600u64 {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			let slot = event_coord_at_millis(
				i64::try_from(step / 4 * 5 + state % 40)
					.expect("test slot offset fits in i64 milliseconds"),
			);
			let value = step as i64;

			if state % 5 == 4 {
				base.remove(&slot);
				tail.remove(&slot);
				assert_eq!(base.tail().len(), tail.len(), "tail length after a remove at step {step}");
				continue;
			}

			let aged = base.push(slot, value);
			let mut expected_aged: Vec<(DateTimeCoord, i64)> = Vec::new();
			if !matches!(immutable_high, Some(immutable) if slot <= immutable) {
				high_water = Some(match high_water {
					Some(hw) if hw >= slot => hw,
					_ => slot,
				});
				tail.insert(slot, value);
				if let (Some(hw), Some(l)) = (high_water, Some(immutable)) {
					while let Some((&c, _)) = tail.iter().next() {
						if hw.order_key().span_since(c.order_key()) > l {
							immutable_high = Some(c);
							immutable_count += 1;
							expected_aged.push(tail.pop_first().expect("non-empty"));
						} else {
							break;
						}
					}
				}
			}

			aged_apart |= !aged.is_empty();
			assert_eq!(aged, expected_aged, "aged coordinates at step {step}");
			assert_eq!(base.immutable_count(), immutable_count, "immutable count at step {step}");
			assert_eq!(base.len(), immutable_count + tail.len() as u64, "total at step {step}");
			assert_eq!(
				base.tail().iter().map(|(c, v)| (*c, *v)).collect::<Vec<_>>(),
				tail.iter().map(|(c, v)| (*c, *v)).collect::<Vec<_>>(),
				"live tail at step {step}"
			);
		}

		assert!(aged_apart, "the run must actually fold something, or it proves nothing about aging");
		assert!(!tail.is_empty(), "the run must leave live coordinates behind to compare");
	}

	#[test]
	fn a_coordinate_exactly_one_immutable_span_behind_the_high_water_stays_live() {
		// The boundary is load-bearing: folding it would discard a retraction that is still allowed.
		let mut base: ImmutableBase<DateTimeCoord, i64> = ImmutableBase::maybe_immutable(Some(millis(10)));
		base.push(event_coord_at_millis(0), 1);
		base.push(event_coord_at_millis(10), 2);
		assert_eq!(base.immutable_count(), 0, "a coordinate exactly at the bound is still immutable");
		assert!(base.tail().contains_key(&event_coord_at_millis(0)));

		base.push(event_coord_at_millis(11), 3);
		assert_eq!(base.immutable_count(), 1, "one millisecond past the bound folds");
		assert!(!base.tail().contains_key(&event_coord_at_millis(0)));
	}

	#[test]
	fn a_corrected_coordinate_that_folds_twice_is_still_one_observation() {
		// A repeat of an already immutable slot is that row arriving again, never a second one.
		let mut base: ImmutableBase<DateTimeCoord, i64> = ImmutableBase::maybe_immutable(Some(millis(1)));
		base.push(event_coord_at_millis(0), 10);
		base.push(event_coord_at_millis(2), 20);
		assert_eq!(base.len(), 2);

		base.push(event_coord_at_millis(0), 99);
		assert_eq!(base.len(), 2, "re-folding a corrected coordinate must not count it twice");
	}

	#[test]
	fn a_row_below_the_immutable_line_is_dropped_while_one_just_above_it_still_counts() {
		// The fast path assumes ordered arrival, so a row at or under the highest immutable slot is lost.
		let mut base: ImmutableBase<DateTimeCoord, i64> = ImmutableBase::maybe_immutable(Some(millis(1)));
		base.push(event_coord_at_millis(0), 1);
		base.push(event_coord_at_millis(10), 2);
		assert_eq!(base.immutable_count(), 1);
		assert_eq!(base.len(), 2);

		base.push(event_coord_at_millis(0), 99);
		assert_eq!(base.len(), 2, "a row at the immutable line never reaches the tail");

		base.push(event_coord_at_millis(1), 3);
		assert_eq!(
			base.immutable_count(),
			2,
			"a row one tick above the immutable line is admitted and then folded"
		);
		assert_eq!(base.len(), 3);
	}

	#[test]
	fn removing_the_row_that_set_the_high_water_does_not_lower_the_immutable_line() {
		// The immutable line is monotonic: a retraction of the newest row must never un-fold what it aged out.
		let mut base: ImmutableBase<DateTimeCoord, i64> = ImmutableBase::maybe_immutable(Some(millis(10)));
		base.push(event_coord_at_millis(0), 1);
		base.push(event_coord_at_millis(50), 2);
		assert_eq!(base.immutable_count(), 1);

		base.remove(&event_coord_at_millis(50));
		assert!(base.tail().is_empty());

		base.push(event_coord_at_millis(30), 3);
		assert_eq!(base.immutable_count(), 2, "the retracted high water still holds the immutable line");
		assert!(base.tail().is_empty());
	}

	#[test]
	fn absorbing_a_disjoint_branch_keeps_every_observation_from_both_sides() {
		// absorb takes slot-disjoint branches, so dropping the other side's immutable total undercounts it.
		let mut left: ImmutableBase<DateTimeCoord, i64> = ImmutableBase::maybe_immutable(Some(millis(10)));
		left.push(event_coord_at_millis(0), 1);
		left.push(event_coord_at_millis(50), 2);
		assert_eq!(left.immutable_count(), 1);
		assert_eq!(left.len(), 2);

		let mut right: ImmutableBase<DateTimeCoord, i64> = ImmutableBase::maybe_immutable(Some(millis(10)));
		right.push(event_coord_at_millis(100), 3);
		right.push(event_coord_at_millis(200), 4);
		assert_eq!(right.immutable_count(), 1);
		assert_eq!(right.len(), 2);

		let mut left_first = left.clone();
		left_first.absorb(&right, |mine, _| *mine);
		let mut right_first = right.clone();
		right_first.absorb(&left, |mine, _| *mine);

		assert_eq!(left_first.len(), 4);
		assert_eq!(right_first.len(), 4, "the merged total must not depend on which branch receives");
	}

	#[test]
	fn absorb_resolves_a_shared_live_coordinate_through_the_combine_rule() {
		// A slot both branches hold live is one row, so it must collapse to a single counted entry.
		let mut left: ImmutableBase<DateTimeCoord, i64> = ImmutableBase::default();
		left.push(event_coord_at_millis(0), 1);
		let mut right: ImmutableBase<DateTimeCoord, i64> = ImmutableBase::default();
		right.push(event_coord_at_millis(0), 9);

		left.absorb(&right, |mine, theirs| *mine.max(theirs));

		assert_eq!(left.len(), 1, "a shared coordinate must never count twice");
		assert_eq!(left.tail().get(&event_coord_at_millis(0)), Some(&9));
	}

	#[test]
	fn without_an_immutable_span_no_distance_is_far_enough_to_fold() {
		// Every differential test compares against this arm, so it must retain every slot it is given.
		let mut base: ImmutableBase<DateTimeCoord, i64> = ImmutableBase::default();
		base.push(event_coord_at_millis(0), 1);
		base.push(event_coord_at_millis(1_000_000), 2);
		base.push(event_coord_at_millis(0), 3);

		assert_eq!(base.immutable_count(), 0);
		assert_eq!(base.tail().len(), 2, "a repeat of a live coordinate corrects it in place");
		assert_eq!(base.tail().get(&event_coord_at_millis(0)), Some(&3));
	}
}
