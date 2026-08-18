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
pub struct SealingEndpoint<C: Slot, V> {
	base: SealingBase<C, V>,
	sealed_open: Option<(C, V)>,
	sealed_close: Option<(C, V)>,
}

impl<C: Slot, V> Default for SealingEndpoint<C, V> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			sealed_open: None,
			sealed_close: None,
		}
	}
}

impl<C: Slot, V: Clone> SealingEndpoint<C, V> {
	pub fn immutable(immutable: SlotSpan<C>) -> Self {
		Self::maybe_immutable(Some(immutable))
	}

	pub fn maybe_immutable(immutable: Option<SlotSpan<C>>) -> Self {
		Self {
			base: SealingBase::maybe_immutable(immutable),
			sealed_open: None,
			sealed_close: None,
		}
	}

	pub fn open(&self) -> Option<&V> {
		match &self.sealed_open {
			Some((_, v)) => Some(v),
			None => self.base.tail().values().next(),
		}
	}

	pub fn close(&self) -> Option<&V> {
		match self.base.tail().values().next_back() {
			Some(v) => Some(v),
			None => self.sealed_close.as_ref().map(|(_, v)| v),
		}
	}

	pub fn absorb(&mut self, other: &Self) {
		if let Some((c, v)) = other.sealed_open.clone() {
			self.seal(c, v);
		}
		if let Some((c, v)) = other.sealed_close.clone() {
			self.seal(c, v);
		}
		for (c, v) in self.base.absorb(&other.base, |_mine, theirs| theirs.clone()) {
			self.seal(c, v);
		}
	}

	fn seal(&mut self, c: C, v: V) {
		self.sealed_open = Some(match self.sealed_open.take() {
			Some((sc, sv)) if sc <= c => (sc, sv),
			_ => (c, v.clone()),
		});
		self.sealed_close = Some(match self.sealed_close.take() {
			Some((sc, sv)) if sc >= c => (sc, sv),
			_ => (c, v),
		});
	}
}

impl<C, V> WindowAccumulator for SealingEndpoint<C, V>
where
	C: Slot + Hash,
	V: Clone + Debug + PartialEq,
	SealingEndpoint<C, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (C, V);
	type Output = (V, V);

	fn add(&mut self, contribution: &(C, V)) {
		for (c, v) in self.base.push(contribution.0, contribution.1.clone()) {
			self.seal(c, v);
		}
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.base.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<(V, V)> {
		match (self.open(), self.close()) {
			(Some(o), Some(c)) => Some((o.clone(), c.clone())),
			_ => None,
		}
	}

	fn is_empty(&self) -> bool {
		self.sealed_open.is_none() && self.base.is_tail_empty()
	}
}

impl<C: Slot + HeapSize, V: HeapSize> HeapSize for SealingEndpoint<C, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.sealed_open.heap_size() + self.sealed_close.heap_size()
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
	fn sealing_endpoint_freezes_open_and_tracks_live_close() {
		let mut accumulator: SealingEndpoint<DateTime, i64> = SealingEndpoint::immutable(millis(10));
		accumulator.add(&(at_millis(0), 100));
		accumulator.add(&(at_millis(5), 200));
		accumulator.add(&(at_millis(12), 300));
		assert_eq!(accumulator.open(), Some(&100), "open frozen to the earliest observation");
		assert_eq!(accumulator.close(), Some(&300), "close is the latest live observation");

		accumulator.remove(&(at_millis(0), 100));
		assert_eq!(accumulator.open(), Some(&100), "aged open removal is a dropped no-op (frozen)");

		accumulator.remove(&(at_millis(12), 300));
		assert_eq!(accumulator.close(), Some(&200), "removing the latest reveals the prior latest in the tail");

		accumulator.add(&(at_millis(20), 400));
		assert_eq!(accumulator.open(), Some(&100));
		assert_eq!(accumulator.close(), Some(&400));
	}

	#[test]
	fn sealing_endpoint_default_is_fully_invertible() {
		assert_add_remove_is_inverse::<SealingEndpoint<DateTime, i64>>(
			&[(at_millis(1), 10i64), (at_millis(3), 30)],
			(at_millis(2), 20i64),
		);
	}

	#[test]
	fn sealing_endpoint_default_open_moves_when_the_earliest_row_is_retracted() {
		// Without an immutable span the open is never frozen, so retracting the earliest row must promote the
		// next.
		let mut accumulator: SealingEndpoint<DateTime, i64> = SealingEndpoint::default();
		accumulator.add(&(at_millis(0), 100));
		accumulator.add(&(at_millis(5), 200));
		accumulator.add(&(at_millis(1_000_000), 300));
		assert_eq!(accumulator.finalize(), Some((100, 300)));

		accumulator.remove(&(at_millis(0), 100));
		assert_eq!(accumulator.open(), Some(&200), "the open is retractable while nothing is sealed");
		assert_eq!(accumulator.finalize(), Some((200, 300)));

		accumulator.remove(&(at_millis(5), 200));
		accumulator.remove(&(at_millis(1_000_000), 300));
		assert!(accumulator.is_empty());
		assert_eq!(accumulator.finalize(), None);
	}

	#[test]
	fn sealing_endpoint_close_is_the_latest_surviving_row_not_the_open() {
		// Only the earliest sealed coordinate is kept, so retracting the live tail must never make close report
		// the window's first value as its last.
		let mut accumulator: SealingEndpoint<DateTime, i64> = SealingEndpoint::immutable(millis(10));
		accumulator.add(&(at_millis(0), 1));
		accumulator.add(&(at_millis(5), 2));
		accumulator.add(&(at_millis(20), 3));

		accumulator.remove(&(at_millis(20), 3));
		assert_eq!(accumulator.close(), Some(&2), "close is the latest surviving observation");
		assert_eq!(accumulator.finalize(), Some((1, 2)));
	}

	#[test]
	fn sealing_endpoint_matches_the_unsealed_arm_for_in_order_adds() {
		// Freezing the open and replaying the tail must reproduce exactly what retaining everything gives.
		assert_arms_agree(
			SealingEndpoint::<DateTime, i64>::immutable(millis(5)),
			SealingEndpoint::<DateTime, i64>::default(),
			&[Op::Add((at_millis(0), 1)), Op::Add((at_millis(10), 2)), Op::Add((at_millis(20), 3))],
			"an immutable span must not change the endpoints when every row arrives in order",
		);
	}

	#[test]
	fn sealing_endpoint_absorb_keeps_a_branch_open_that_predates_the_seal_line() {
		// absorb combines two parallel histories, never late arrivals, so the receiver's seal line must not
		// swallow the other branch.
		let mut left: SealingEndpoint<DateTime, i64> = SealingEndpoint::immutable(millis(10));
		left.add(&(at_millis(0), 1));

		let mut right: SealingEndpoint<DateTime, i64> = SealingEndpoint::immutable(millis(10));
		right.add(&(at_millis(50), 5));
		right.add(&(at_millis(70), 7));

		let mut left_first = left.clone();
		left_first.absorb(&right);
		let mut right_first = right.clone();
		right_first.absorb(&left);

		assert_eq!(left_first.open(), Some(&1));
		assert_eq!(right_first.open(), left_first.open(), "absorb must not depend on which branch receives");
	}

	#[test]
	fn sealing_endpoint_roundtrip() {
		let mut ep: SealingEndpoint<DateTime, i64> = SealingEndpoint::immutable(millis(10));
		ep.add(&(at_millis(0), 100));
		ep.add(&(at_millis(12), 300));
		let bytes = ep.encode_state().expect("encode");
		let restored: SealingEndpoint<DateTime, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, ep);
	}
}
