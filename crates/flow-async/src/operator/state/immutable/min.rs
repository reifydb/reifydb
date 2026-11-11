// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash};

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use super::base::ImmutableBase;
use crate::window::{
	accumulator::{MergeAccumulator, WindowAccumulator},
	span::{Slot, SlotSpan},
};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct ImmutableMin<S: Slot, V: Ord> {
	base: ImmutableBase<S, V>,
	immutable: Option<V>,
}

impl<S: Slot, V: Ord> Default for ImmutableMin<S, V> {
	fn default() -> Self {
		Self {
			base: ImmutableBase::default(),
			immutable: None,
		}
	}
}

impl<S: Slot, V: Ord + Clone> ImmutableMin<S, V> {
	pub fn immutable(immutable: SlotSpan<S>) -> Self {
		Self::maybe_immutable(Some(immutable))
	}

	pub fn maybe_immutable(immutable: Option<SlotSpan<S>>) -> Self {
		Self {
			base: ImmutableBase::maybe_immutable(immutable),
			immutable: None,
		}
	}

	pub fn min(&self) -> Option<V> {
		let tail_min = self.base.tail().values().min().cloned();
		match (self.immutable.clone(), tail_min) {
			(Some(s), Some(t)) => Some(s.min(t)),
			(Some(s), None) => Some(s),
			(None, Some(t)) => Some(t),
			(None, None) => None,
		}
	}

	fn fold(&mut self, v: V) {
		self.immutable = Some(match self.immutable.take() {
			Some(s) => s.min(v),
			None => v,
		});
	}
}

impl<S, V> MergeAccumulator for ImmutableMin<S, V>
where
	S: Slot + Hash,
	V: Ord + Clone + Debug,
	ImmutableMin<S, V>: OperatorState + StateCodec + HeapSize,
{
	fn merge(&mut self, other: &Self) {
		if let Some(s) = other.immutable.clone() {
			self.fold(s);
		}
		for (_, aged) in self.base.absorb(&other.base, |mine, theirs| mine.clone().min(theirs.clone())) {
			self.fold(aged);
		}
	}

	fn compacts(&self) -> bool {
		self.base.immutable().is_some()
	}
}

impl<S, V> WindowAccumulator for ImmutableMin<S, V>
where
	S: Slot + Hash,
	V: Ord + Clone + Debug,
	ImmutableMin<S, V>: OperatorState + StateCodec + HeapSize,
{
	type Contributions = Vec<(S, V)>;
	type Output = V;

	fn add(&mut self, contributions: &[(S, V)]) {
		for contribution in contributions {
			for (_, v) in self.base.push(contribution.0, contribution.1.clone()) {
				self.immutable = Some(match self.immutable.take() {
					Some(s) => s.min(v),
					None => v,
				});
			}
		}
	}

	fn remove(&mut self, contributions: &[(S, V)]) {
		for contribution in contributions {
			self.base.remove(&contribution.0);
		}
	}

	fn finalize(&self) -> Option<V> {
		self.min()
	}
}

impl<S: Slot + HeapSize, V: Ord + HeapSize> HeapSize for ImmutableMin<S, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.immutable.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::factory::time::millis;

	use super::*;
	use crate::{
		factory::coord::event_coord_at_millis,
		window::{
			accumulator::{
				CountedAccumulator,
				testkit::{Op, assert_add_remove_is_inverse, assert_arms_agree, drive},
			},
			coord::DateTimeCoord,
		},
	};

	#[test]
	fn immutable_min_folds_aged_extreme() {
		let mut accumulator: ImmutableMin<DateTimeCoord, i64> = ImmutableMin::immutable(millis(10));
		accumulator.add(&[(event_coord_at_millis(0), 2)]);
		accumulator.add(&[(event_coord_at_millis(5), 9)]);
		accumulator.add(&[(event_coord_at_millis(12), 7)]);
		assert_eq!(accumulator.min(), Some(2));
		accumulator.remove(&[(event_coord_at_millis(5), 9)]);
		assert_eq!(accumulator.min(), Some(2), "immutable min 2 survives removal of a live event");
	}

	#[test]
	fn immutable_min_default_never_folds_and_is_fully_invertible() {
		// Without an immutable span the minimum is never frozen, so retracting it must reveal the next one.
		assert_add_remove_is_inverse::<ImmutableMin<DateTimeCoord, i64>>(
			&[(event_coord_at_millis(1), 10i64), (event_coord_at_millis(2), 20)],
			(event_coord_at_millis(3), 30i64),
		);
		let mut accumulator: ImmutableMin<DateTimeCoord, i64> = ImmutableMin::default();
		accumulator.add(&[(event_coord_at_millis(0), 5)]);
		accumulator.add(&[(event_coord_at_millis(1_000_000), 8)]);
		accumulator.remove(&[(event_coord_at_millis(0), 5)]);
		assert_eq!(accumulator.min(), Some(8), "removing the min reveals the next min (no folding)");
	}

	#[test]
	fn immutable_min_merge_keeps_the_smaller_value_at_a_shared_coordinate() {
		// Two branches holding the same slot must never let the incoming value replace a smaller one.
		let mut accumulator: ImmutableMin<DateTimeCoord, i64> = ImmutableMin::default();
		accumulator.add(&[(event_coord_at_millis(0), 1)]);
		let mut other: ImmutableMin<DateTimeCoord, i64> = ImmutableMin::default();
		other.add(&[(event_coord_at_millis(0), 9)]);

		accumulator.merge(&other);
		assert_eq!(
			accumulator.min(),
			Some(1),
			"merging a larger value at the same coordinate must not raise the min"
		);
	}

	#[test]
	fn immutable_min_default_merge_leaves_nothing_folded() {
		// merge is the only path that can fold without an immutable span, and an immutable minimum never
		// retracts.
		let mut left: CountedAccumulator<ImmutableMin<DateTimeCoord, i64>> =
			CountedAccumulator::new(ImmutableMin::default());
		left.add(&[(event_coord_at_millis(0), 5)]);
		let mut right: CountedAccumulator<ImmutableMin<DateTimeCoord, i64>> =
			CountedAccumulator::new(ImmutableMin::default());
		right.add(&[(event_coord_at_millis(1_000_000), 8)]);

		left.merge(&right);
		assert_eq!(left.inner().min(), Some(5));

		left.remove(&[(event_coord_at_millis(0), 5)]);
		left.remove(&[(event_coord_at_millis(1_000_000), 8)]);
		assert!(left.is_empty(), "every merged row stays retractable while nothing folds");
		assert_eq!(left.finalize(), None);
	}

	#[test]
	fn immutable_min_with_an_immutable_span_beyond_the_data_matches_the_plain_arm() {
		// Nothing ages in either arm, so any divergence is an aging rule that fired when it must not.
		let mut immutable: ImmutableMin<DateTimeCoord, i64> = ImmutableMin::immutable(millis(1_000));
		let mut plain: ImmutableMin<DateTimeCoord, i64> = ImmutableMin::default();
		for (slot, value) in [(0, 5i64), (10, 2), (20, 7)] {
			immutable.add(&[(event_coord_at_millis(slot), value)]);
			plain.add(&[(event_coord_at_millis(slot), value)]);
		}

		immutable.remove(&[(event_coord_at_millis(10), 2)]);
		plain.remove(&[(event_coord_at_millis(10), 2)]);
		assert_eq!(immutable.min(), plain.min(), "with nothing folded the two arms must agree");
		assert_eq!(immutable.min(), Some(5));
	}

	#[test]
	fn immutable_min_matches_the_plain_arm_for_in_order_adds() {
		// Folding is a memory optimisation over adds; without a retraction it must not move the answer.
		assert_arms_agree(
			ImmutableMin::<DateTimeCoord, i64>::immutable(millis(5)),
			ImmutableMin::<DateTimeCoord, i64>::default(),
			&[
				Op::Add((event_coord_at_millis(0), 9)),
				Op::Add((event_coord_at_millis(10), 2)),
				Op::Add((event_coord_at_millis(20), 7)),
				Op::Add((event_coord_at_millis(30), 4)),
			],
			"an immutable span must not change the minimum when every row arrives in order",
		);
	}

	#[test]
	fn immutable_min_never_drains_to_empty_once_a_row_has_folded() {
		// An immutable minimum cannot be retracted, so the immutable arm's state is never reclaimable.
		let ops = [
			Op::Add((event_coord_at_millis(0), 5i64)),
			Op::Add((event_coord_at_millis(50), 8)),
			Op::Remove((event_coord_at_millis(0), 5)),
			Op::Remove((event_coord_at_millis(50), 8)),
		];
		let mut immutable: CountedAccumulator<ImmutableMin<DateTimeCoord, i64>> =
			CountedAccumulator::new(ImmutableMin::immutable(millis(10)));
		drive(&mut immutable, &ops);
		let mut plain: CountedAccumulator<ImmutableMin<DateTimeCoord, i64>> =
			CountedAccumulator::new(ImmutableMin::default());
		drive(&mut plain, &ops);

		assert!(plain.is_empty(), "with nothing folded every row stays retractable");
		assert!(!immutable.is_empty(), "the immutable minimum outlives both retractions");
		assert_eq!(immutable.finalize(), Some(5));
	}

	#[test]
	fn immutable_min_merge_keeps_a_branch_minimum_that_predates_the_immutable_line() {
		// merge combines two parallel histories, never late arrivals, so the receiver's immutable line must not
		// swallow the other branch.
		let mut left: ImmutableMin<DateTimeCoord, i64> = ImmutableMin::immutable(millis(10));
		left.add(&[(event_coord_at_millis(0), 5)]);

		let mut right: ImmutableMin<DateTimeCoord, i64> = ImmutableMin::immutable(millis(10));
		right.add(&[(event_coord_at_millis(50), 7)]);
		right.add(&[(event_coord_at_millis(70), 9)]);

		let mut left_first = left.clone();
		left_first.merge(&right);
		let mut right_first = right.clone();
		right_first.merge(&left);

		assert_eq!(left_first.min(), Some(5));
		assert_eq!(right_first.min(), left_first.min(), "merge must not depend on which branch receives");
	}

	#[test]
	fn immutable_min_compacts_only_with_an_immutable_span() {
		// without an immutable span nothing folds, so the tail must count as growing with every sample
		assert!(!ImmutableMin::<DateTimeCoord, i64>::default().compacts());
		assert!(!ImmutableMin::<DateTimeCoord, i64>::maybe_immutable(None).compacts());
		assert!(ImmutableMin::<DateTimeCoord, i64>::immutable(millis(10)).compacts());
	}
}
