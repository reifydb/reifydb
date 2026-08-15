// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash};

use reifydb_codec::row::operator::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::{
	accumulator::{WindowAccumulator, sealing::base::SealingBase},
	span::{Slot, SlotSpan},
};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct SealingMin<C: Slot, V: Ord> {
	base: SealingBase<C, V>,
	sealed: Option<V>,
}

impl<C: Slot, V: Ord> Default for SealingMin<C, V> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			sealed: None,
		}
	}
}

impl<C: Slot, V: Ord + Clone> SealingMin<C, V> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			base: SealingBase::amendable(amendable),
			sealed: None,
		}
	}

	pub fn min(&self) -> Option<V> {
		let tail_min = self.base.tail().values().min().cloned();
		match (self.sealed.clone(), tail_min) {
			(Some(s), Some(t)) => Some(s.min(t)),
			(Some(s), None) => Some(s),
			(None, Some(t)) => Some(t),
			(None, None) => None,
		}
	}

	pub fn absorb(&mut self, other: &Self) {
		if let Some(s) = other.sealed.clone() {
			self.seal(s);
		}
		for (_, aged) in self.base.absorb(&other.base, |mine, theirs| mine.clone().min(theirs.clone())) {
			self.seal(aged);
		}
	}

	fn seal(&mut self, v: V) {
		self.sealed = Some(match self.sealed.take() {
			Some(s) => s.min(v),
			None => v,
		});
	}
}

impl<C, V> WindowAccumulator for SealingMin<C, V>
where
	C: Slot + Hash,
	V: Ord + Clone + Debug,
	SealingMin<C, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (C, V);
	type Output = V;

	fn add(&mut self, contribution: &(C, V)) {
		for (_, v) in self.base.push(contribution.0, contribution.1.clone()) {
			self.sealed = Some(match self.sealed.take() {
				Some(s) => s.min(v),
				None => v,
			});
		}
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.base.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<V> {
		self.min()
	}

	fn is_empty(&self) -> bool {
		self.sealed.is_none() && self.base.is_tail_empty()
	}
}

impl<C: Slot + HeapSize, V: Ord + HeapSize> HeapSize for SealingMin<C, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.sealed.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::{
		factory::time::{at_millis, millis},
		value::datetime::DateTime,
	};

	use super::*;
	use crate::window::accumulator::testkit::{Op, assert_arms_agree, drive};

	#[test]
	fn sealing_min_seals_aged_extreme() {
		let mut accumulator: SealingMin<DateTime, i64> = SealingMin::amendable(millis(10));
		accumulator.add(&(at_millis(0), 2));
		accumulator.add(&(at_millis(5), 9));
		accumulator.add(&(at_millis(12), 7));
		assert_eq!(accumulator.min(), Some(2));
		accumulator.remove(&(at_millis(5), 9));
		assert_eq!(accumulator.min(), Some(2), "sealed min 2 survives removal of a live event");
	}

	#[test]
	fn sealing_min_absorb_keeps_the_smaller_value_at_a_shared_coordinate() {
		// Two branches holding the same coordinate must never let the incoming value replace a smaller one.
		let mut accumulator: SealingMin<DateTime, i64> = SealingMin::default();
		accumulator.add(&(at_millis(0), 1));
		let mut other: SealingMin<DateTime, i64> = SealingMin::default();
		other.add(&(at_millis(0), 9));

		accumulator.absorb(&other);
		assert_eq!(
			accumulator.min(),
			Some(1),
			"absorbing a larger value at the same coordinate must not raise the min"
		);
	}

	#[test]
	fn sealing_min_with_an_amendable_span_beyond_the_data_matches_the_unsealed_arm() {
		// Nothing ages in either arm, so any divergence is an aging rule that fired when it must not.
		let mut sealed: SealingMin<DateTime, i64> = SealingMin::amendable(millis(1_000));
		let mut unsealed: SealingMin<DateTime, i64> = SealingMin::default();
		for (coord, value) in [(0, 5i64), (10, 2), (20, 7)] {
			sealed.add(&(at_millis(coord), value));
			unsealed.add(&(at_millis(coord), value));
		}

		sealed.remove(&(at_millis(10), 2));
		unsealed.remove(&(at_millis(10), 2));
		assert_eq!(sealed.min(), unsealed.min(), "with nothing sealed the two arms must agree");
		assert_eq!(sealed.min(), Some(5));
	}

	#[test]
	fn sealing_min_matches_the_unsealed_arm_for_in_order_adds() {
		// Sealing is a memory optimisation over adds; without a retraction it must not move the answer.
		assert_arms_agree(
			SealingMin::<DateTime, i64>::amendable(millis(5)),
			SealingMin::<DateTime, i64>::default(),
			&[
				Op::Add((at_millis(0), 9)),
				Op::Add((at_millis(10), 2)),
				Op::Add((at_millis(20), 7)),
				Op::Add((at_millis(30), 4)),
			],
			"an amendable span must not change the minimum when every row arrives in order",
		);
	}

	#[test]
	fn sealing_min_never_drains_to_empty_once_a_row_has_sealed() {
		// A sealed minimum cannot be retracted, so the sealed arm's state is never reclaimable.
		let ops = [
			Op::Add((at_millis(0), 5i64)),
			Op::Add((at_millis(50), 8)),
			Op::Remove((at_millis(0), 5)),
			Op::Remove((at_millis(50), 8)),
		];
		let mut sealed: SealingMin<DateTime, i64> = SealingMin::amendable(millis(10));
		drive(&mut sealed, &ops);
		let mut unsealed: SealingMin<DateTime, i64> = SealingMin::default();
		drive(&mut unsealed, &ops);

		assert!(unsealed.is_empty(), "with nothing sealed every row stays retractable");
		assert!(!sealed.is_empty(), "the sealed minimum outlives both retractions");
		assert_eq!(sealed.finalize(), Some(5));
	}

	#[test]
	fn sealing_min_absorb_keeps_a_branch_minimum_that_predates_the_seal_line() {
		// absorb combines two parallel histories, never late arrivals, so the receiver's seal line must not
		// swallow the other branch.
		let mut left: SealingMin<DateTime, i64> = SealingMin::amendable(millis(10));
		left.add(&(at_millis(0), 5));

		let mut right: SealingMin<DateTime, i64> = SealingMin::amendable(millis(10));
		right.add(&(at_millis(50), 7));
		right.add(&(at_millis(70), 9));

		let mut left_first = left.clone();
		left_first.absorb(&right);
		let mut right_first = right.clone();
		right_first.absorb(&left);

		assert_eq!(left_first.min(), Some(5));
		assert_eq!(right_first.min(), left_first.min(), "absorb must not depend on which branch receives");
	}
}
