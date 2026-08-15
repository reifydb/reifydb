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
		for (coord, value) in other.base.tail() {
			for (_, aged) in self.base.push(*coord, value.clone()) {
				self.seal(aged);
			}
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
}
