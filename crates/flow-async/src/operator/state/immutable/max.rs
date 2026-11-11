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
pub struct ImmutableMax<S: Slot, V: Ord> {
	base: ImmutableBase<S, V>,
	immutable: Option<V>,
}

impl<S: Slot, V: Ord> Default for ImmutableMax<S, V> {
	fn default() -> Self {
		Self {
			base: ImmutableBase::default(),
			immutable: None,
		}
	}
}

impl<S: Slot, V: Ord + Clone> ImmutableMax<S, V> {
	pub fn immutable(immutable: SlotSpan<S>) -> Self {
		Self::maybe_immutable(Some(immutable))
	}

	pub fn maybe_immutable(immutable: Option<SlotSpan<S>>) -> Self {
		Self {
			base: ImmutableBase::maybe_immutable(immutable),
			immutable: None,
		}
	}

	pub fn max(&self) -> Option<V> {
		let tail_max = self.base.tail().values().max().cloned();
		match (self.immutable.clone(), tail_max) {
			(Some(s), Some(t)) => Some(s.max(t)),
			(Some(s), None) => Some(s),
			(None, Some(t)) => Some(t),
			(None, None) => None,
		}
	}

	fn fold(&mut self, v: V) {
		self.immutable = Some(match self.immutable.take() {
			Some(s) => s.max(v),
			None => v,
		});
	}
}

impl<S, V> MergeAccumulator for ImmutableMax<S, V>
where
	S: Slot + Hash,
	V: Ord + Clone + Debug,
	ImmutableMax<S, V>: OperatorState + StateCodec + HeapSize,
{
	fn merge(&mut self, other: &Self) {
		if let Some(s) = other.immutable.clone() {
			self.fold(s);
		}
		for (_, aged) in self.base.absorb(&other.base, |mine, theirs| mine.clone().max(theirs.clone())) {
			self.fold(aged);
		}
	}

	fn compacts(&self) -> bool {
		self.base.immutable().is_some()
	}
}

impl<S, V> WindowAccumulator for ImmutableMax<S, V>
where
	S: Slot + Hash,
	V: Ord + Clone + Debug,
	ImmutableMax<S, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (S, V);
	type Output = V;

	fn add(&mut self, contribution: &(S, V)) {
		for (_, v) in self.base.push(contribution.0, contribution.1.clone()) {
			self.immutable = Some(match self.immutable.take() {
				Some(s) => s.max(v),
				None => v,
			});
		}
	}

	fn remove(&mut self, contribution: &(S, V)) {
		self.base.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<V> {
		self.max()
	}

	fn is_empty(&self) -> bool {
		self.immutable.is_none() && self.base.is_tail_empty()
	}
}

impl<S: Slot + HeapSize, V: Ord + HeapSize> HeapSize for ImmutableMax<S, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.immutable.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::state::decode;
	use reifydb_value::factory::time::millis;

	use super::*;
	use crate::{
		factory::coord::event_coord_at_millis,
		window::{
			accumulator::testkit::{Op, assert_add_remove_is_inverse, assert_arms_agree},
			coord::DateTimeCoord,
		},
	};

	#[test]
	fn immutable_max_folds_aged_and_keeps_recent_tail_removal_safe() {
		let mut accumulator: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::immutable(millis(10));
		accumulator.add(&(event_coord_at_millis(0), 5));
		accumulator.add(&(event_coord_at_millis(5), 8));
		accumulator.add(&(event_coord_at_millis(12), 3));
		assert_eq!(accumulator.max(), Some(8));

		accumulator.remove(&(event_coord_at_millis(0), 5));
		assert_eq!(accumulator.max(), Some(8), "aged removal does not disturb the immutable max");

		accumulator.remove(&(event_coord_at_millis(5), 8));
		assert_eq!(accumulator.max(), Some(5), "tail max 8 removed; falls back to immutable 5");
	}

	#[test]
	fn immutable_max_default_never_folds_and_is_fully_invertible() {
		assert_add_remove_is_inverse::<ImmutableMax<DateTimeCoord, i64>>(
			&[(event_coord_at_millis(1), 10i64), (event_coord_at_millis(2), 20)],
			(event_coord_at_millis(3), 30i64),
		);
		let mut accumulator: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::default();
		accumulator.add(&(event_coord_at_millis(0), 5));
		accumulator.add(&(event_coord_at_millis(100), 8));
		accumulator.remove(&(event_coord_at_millis(100), 8));
		assert_eq!(accumulator.max(), Some(5), "removing the max reveals the prior max (no folding)");
	}

	#[test]
	fn immutable_max_merge_keeps_the_larger_value_at_a_shared_coordinate() {
		// Two branches holding the same slot must never let the incoming value replace a larger one.
		let mut accumulator: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::default();
		accumulator.add(&(event_coord_at_millis(0), 9));
		let mut other: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::default();
		other.add(&(event_coord_at_millis(0), 1));

		accumulator.merge(&other);
		assert_eq!(
			accumulator.max(),
			Some(9),
			"merging a smaller value at the same coordinate must not lower the max"
		);
	}

	#[test]
	fn immutable_max_default_merge_leaves_nothing_folded() {
		// merge is the only path that can fold without an immutable span, and an immutable maximum never
		// retracts.
		let mut left: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::default();
		left.add(&(event_coord_at_millis(0), 9));
		let mut right: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::default();
		right.add(&(event_coord_at_millis(1_000_000), 3));

		left.merge(&right);
		assert_eq!(left.max(), Some(9));

		left.remove(&(event_coord_at_millis(0), 9));
		left.remove(&(event_coord_at_millis(1_000_000), 3));
		assert!(left.is_empty(), "every merged row stays retractable while nothing folds");
		assert_eq!(left.finalize(), None);
	}

	#[test]
	fn immutable_max_with_an_immutable_span_beyond_the_data_matches_the_plain_arm() {
		// Nothing ages in either arm, so any divergence is an aging rule that fired when it must not.
		let mut immutable: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::immutable(millis(1_000));
		let mut plain: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::default();
		for (slot, value) in [(0, 5i64), (10, 9), (20, 7)] {
			immutable.add(&(event_coord_at_millis(slot), value));
			plain.add(&(event_coord_at_millis(slot), value));
		}

		immutable.remove(&(event_coord_at_millis(10), 9));
		plain.remove(&(event_coord_at_millis(10), 9));
		assert_eq!(immutable.max(), plain.max(), "with nothing folded the two arms must agree");
		assert_eq!(immutable.max(), Some(7));
	}

	#[test]
	fn immutable_max_matches_the_plain_arm_for_in_order_adds() {
		// Folding is a memory optimisation over adds; without a retraction it must not move the answer.
		assert_arms_agree(
			ImmutableMax::<DateTimeCoord, i64>::immutable(millis(5)),
			ImmutableMax::<DateTimeCoord, i64>::default(),
			&[
				Op::Add((event_coord_at_millis(0), 9)),
				Op::Add((event_coord_at_millis(10), 2)),
				Op::Add((event_coord_at_millis(20), 7)),
				Op::Add((event_coord_at_millis(30), 4)),
			],
			"an immutable span must not change the maximum when every row arrives in order",
		);
	}

	#[test]
	fn immutable_max_merge_keeps_a_branch_maximum_that_predates_the_immutable_line() {
		// merge combines two parallel histories, never late arrivals, so the receiver's immutable line must not
		// swallow the other branch.
		let mut left: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::immutable(millis(10));
		left.add(&(event_coord_at_millis(0), 9));

		let mut right: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::immutable(millis(10));
		right.add(&(event_coord_at_millis(50), 1));
		right.add(&(event_coord_at_millis(70), 3));

		let mut left_first = left.clone();
		left_first.merge(&right);
		let mut right_first = right.clone();
		right_first.merge(&left);

		assert_eq!(left_first.max(), Some(9));
		assert_eq!(right_first.max(), left_first.max(), "merge must not depend on which branch receives");
	}

	#[test]
	fn immutable_max_roundtrip() {
		let mut mx: ImmutableMax<DateTimeCoord, i64> = ImmutableMax::immutable(millis(10));
		mx.add(&(event_coord_at_millis(0), 5));
		mx.add(&(event_coord_at_millis(12), 8));
		let bytes = mx.encode_state().expect("encode");
		let restored: ImmutableMax<DateTimeCoord, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, mx);
	}

	#[test]
	fn immutable_max_compacts_only_with_an_immutable_span() {
		// without an immutable span nothing folds, so the tail must count as growing with every sample
		assert!(!ImmutableMax::<DateTimeCoord, i64>::default().compacts());
		assert!(!ImmutableMax::<DateTimeCoord, i64>::maybe_immutable(None).compacts());
		assert!(ImmutableMax::<DateTimeCoord, i64>::immutable(millis(10)).compacts());
	}
}
