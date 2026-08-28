// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash};

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::{
	accumulator::{WindowAccumulator, sealing::base::SealingBase},
	span::{Slot, SlotSpan},
};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct SealingMax<S: Slot, V: Ord> {
	base: SealingBase<S, V>,
	sealed: Option<V>,
}

impl<S: Slot, V: Ord> Default for SealingMax<S, V> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			sealed: None,
		}
	}
}

impl<S: Slot, V: Ord + Clone> SealingMax<S, V> {
	pub fn immutable(immutable: SlotSpan<S>) -> Self {
		Self::maybe_immutable(Some(immutable))
	}

	pub fn maybe_immutable(immutable: Option<SlotSpan<S>>) -> Self {
		Self {
			base: SealingBase::maybe_immutable(immutable),
			sealed: None,
		}
	}

	pub fn max(&self) -> Option<V> {
		let tail_max = self.base.tail().values().max().cloned();
		match (self.sealed.clone(), tail_max) {
			(Some(s), Some(t)) => Some(s.max(t)),
			(Some(s), None) => Some(s),
			(None, Some(t)) => Some(t),
			(None, None) => None,
		}
	}

	pub fn absorb(&mut self, other: &Self) {
		if let Some(s) = other.sealed.clone() {
			self.seal(s);
		}
		for (_, aged) in self.base.absorb(&other.base, |mine, theirs| mine.clone().max(theirs.clone())) {
			self.seal(aged);
		}
	}

	fn seal(&mut self, v: V) {
		self.sealed = Some(match self.sealed.take() {
			Some(s) => s.max(v),
			None => v,
		});
	}
}

impl<S, V> WindowAccumulator for SealingMax<S, V>
where
	S: Slot + Hash,
	V: Ord + Clone + Debug,
	SealingMax<S, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (S, V);
	type Output = V;

	fn add(&mut self, contribution: &(S, V)) {
		for (_, v) in self.base.push(contribution.0, contribution.1.clone()) {
			self.sealed = Some(match self.sealed.take() {
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
		self.sealed.is_none() && self.base.is_tail_empty()
	}
}

impl<S: Slot + HeapSize, V: Ord + HeapSize> HeapSize for SealingMax<S, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.sealed.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::state::decode;
	use reifydb_value::{
		factory::time::{at_millis, millis},
		value::datetime::DateTime,
	};

	use super::*;
	use crate::window::accumulator::testkit::{Op, assert_add_remove_is_inverse, assert_arms_agree};

	#[test]
	fn sealing_max_seals_aged_and_keeps_recent_tail_removal_safe() {
		let mut accumulator: SealingMax<DateTime, i64> = SealingMax::immutable(millis(10));
		accumulator.add(&(at_millis(0), 5));
		accumulator.add(&(at_millis(5), 8));
		accumulator.add(&(at_millis(12), 3));
		assert_eq!(accumulator.max(), Some(8));

		accumulator.remove(&(at_millis(0), 5));
		assert_eq!(accumulator.max(), Some(8), "aged removal does not disturb the sealed max");

		accumulator.remove(&(at_millis(5), 8));
		assert_eq!(accumulator.max(), Some(5), "tail max 8 removed; falls back to sealed 5");
	}

	#[test]
	fn sealing_max_default_never_seals_and_is_fully_invertible() {
		assert_add_remove_is_inverse::<SealingMax<DateTime, i64>>(
			&[(at_millis(1), 10i64), (at_millis(2), 20)],
			(at_millis(3), 30i64),
		);
		let mut accumulator: SealingMax<DateTime, i64> = SealingMax::default();
		accumulator.add(&(at_millis(0), 5));
		accumulator.add(&(at_millis(100), 8));
		accumulator.remove(&(at_millis(100), 8));
		assert_eq!(accumulator.max(), Some(5), "removing the max reveals the prior max (no sealing)");
	}

	#[test]
	fn sealing_max_absorb_keeps_the_larger_value_at_a_shared_coordinate() {
		// Two branches holding the same slot must never let the incoming value replace a larger one.
		let mut accumulator: SealingMax<DateTime, i64> = SealingMax::default();
		accumulator.add(&(at_millis(0), 9));
		let mut other: SealingMax<DateTime, i64> = SealingMax::default();
		other.add(&(at_millis(0), 1));

		accumulator.absorb(&other);
		assert_eq!(
			accumulator.max(),
			Some(9),
			"absorbing a smaller value at the same coordinate must not lower the max"
		);
	}

	#[test]
	fn sealing_max_default_absorb_leaves_nothing_sealed() {
		// absorb is the only path that can seal without an immutable span, and a sealed maximum never retracts.
		let mut left: SealingMax<DateTime, i64> = SealingMax::default();
		left.add(&(at_millis(0), 9));
		let mut right: SealingMax<DateTime, i64> = SealingMax::default();
		right.add(&(at_millis(1_000_000), 3));

		left.absorb(&right);
		assert_eq!(left.max(), Some(9));

		left.remove(&(at_millis(0), 9));
		left.remove(&(at_millis(1_000_000), 3));
		assert!(left.is_empty(), "every absorbed row stays retractable while nothing seals");
		assert_eq!(left.finalize(), None);
	}

	#[test]
	fn sealing_max_with_an_immutable_span_beyond_the_data_matches_the_unsealed_arm() {
		// Nothing ages in either arm, so any divergence is an aging rule that fired when it must not.
		let mut sealed: SealingMax<DateTime, i64> = SealingMax::immutable(millis(1_000));
		let mut unsealed: SealingMax<DateTime, i64> = SealingMax::default();
		for (slot, value) in [(0, 5i64), (10, 9), (20, 7)] {
			sealed.add(&(at_millis(slot), value));
			unsealed.add(&(at_millis(slot), value));
		}

		sealed.remove(&(at_millis(10), 9));
		unsealed.remove(&(at_millis(10), 9));
		assert_eq!(sealed.max(), unsealed.max(), "with nothing sealed the two arms must agree");
		assert_eq!(sealed.max(), Some(7));
	}

	#[test]
	fn sealing_max_matches_the_unsealed_arm_for_in_order_adds() {
		// Sealing is a memory optimisation over adds; without a retraction it must not move the answer.
		assert_arms_agree(
			SealingMax::<DateTime, i64>::immutable(millis(5)),
			SealingMax::<DateTime, i64>::default(),
			&[
				Op::Add((at_millis(0), 9)),
				Op::Add((at_millis(10), 2)),
				Op::Add((at_millis(20), 7)),
				Op::Add((at_millis(30), 4)),
			],
			"an immutable span must not change the maximum when every row arrives in order",
		);
	}

	#[test]
	fn sealing_max_absorb_keeps_a_branch_maximum_that_predates_the_seal_line() {
		// absorb combines two parallel histories, never late arrivals, so the receiver's seal line must not
		// swallow the other branch.
		let mut left: SealingMax<DateTime, i64> = SealingMax::immutable(millis(10));
		left.add(&(at_millis(0), 9));

		let mut right: SealingMax<DateTime, i64> = SealingMax::immutable(millis(10));
		right.add(&(at_millis(50), 1));
		right.add(&(at_millis(70), 3));

		let mut left_first = left.clone();
		left_first.absorb(&right);
		let mut right_first = right.clone();
		right_first.absorb(&left);

		assert_eq!(left_first.max(), Some(9));
		assert_eq!(right_first.max(), left_first.max(), "absorb must not depend on which branch receives");
	}

	#[test]
	fn sealing_max_roundtrip() {
		let mut mx: SealingMax<DateTime, i64> = SealingMax::immutable(millis(10));
		mx.add(&(at_millis(0), 5));
		mx.add(&(at_millis(12), 8));
		let bytes = mx.encode_state().expect("encode");
		let restored: SealingMax<DateTime, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, mx);
	}
}
