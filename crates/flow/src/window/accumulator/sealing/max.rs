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
pub struct SealingMax<C: Slot, V: Ord> {
	base: SealingBase<C, V>,
	sealed: Option<V>,
}

impl<C: Slot, V: Ord> Default for SealingMax<C, V> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			sealed: None,
		}
	}
}

impl<C: Slot, V: Ord + Clone> SealingMax<C, V> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			base: SealingBase::amendable(amendable),
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
		for (coord, value) in other.base.tail() {
			for (_, aged) in self.base.push(*coord, value.clone()) {
				self.seal(aged);
			}
		}
	}

	fn seal(&mut self, v: V) {
		self.sealed = Some(match self.sealed.take() {
			Some(s) => s.max(v),
			None => v,
		});
	}
}

impl<C, V> WindowAccumulator for SealingMax<C, V>
where
	C: Slot + Hash,
	V: Ord + Clone + Debug,
	SealingMax<C, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (C, V);
	type Output = V;

	fn add(&mut self, contribution: &(C, V)) {
		for (_, v) in self.base.push(contribution.0, contribution.1.clone()) {
			self.sealed = Some(match self.sealed.take() {
				Some(s) => s.max(v),
				None => v,
			});
		}
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.base.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<V> {
		self.max()
	}

	fn is_empty(&self) -> bool {
		self.sealed.is_none() && self.base.is_tail_empty()
	}
}

impl<C: Slot + HeapSize, V: Ord + HeapSize> HeapSize for SealingMax<C, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size() + self.sealed.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::decode;
	use reifydb_value::{
		factory::time::{at_millis, millis},
		value::datetime::DateTime,
	};

	use super::*;
	use crate::window::accumulator::testkit::assert_add_remove_is_inverse;

	#[test]
	fn sealing_max_seals_aged_and_keeps_recent_tail_removal_safe() {
		let mut accumulator: SealingMax<DateTime, i64> = SealingMax::amendable(millis(10));
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
	fn sealing_max_roundtrip() {
		let mut mx: SealingMax<DateTime, i64> = SealingMax::amendable(millis(10));
		mx.add(&(at_millis(0), 5));
		mx.add(&(at_millis(12), 8));
		let bytes = mx.encode_state(DateTime::EPOCH).expect("encode");
		let restored: SealingMax<DateTime, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, mx);
	}
}
