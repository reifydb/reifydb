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
pub struct SealingEndpoint<C: Slot, V> {
	base: SealingBase<C, V>,
	sealed_open: Option<(C, V)>,
}

impl<C: Slot, V> Default for SealingEndpoint<C, V> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
			sealed_open: None,
		}
	}
}

impl<C: Slot, V: Clone> SealingEndpoint<C, V> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			base: SealingBase::amendable(amendable),
			sealed_open: None,
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
			None => self.sealed_open.as_ref().map(|(_, v)| v),
		}
	}

	pub fn absorb(&mut self, other: &Self) {
		if let Some((c, v)) = other.sealed_open.clone() {
			self.seal_open(c, v);
		}
		for (coord, value) in other.base.tail() {
			for (c, v) in self.base.push(*coord, value.clone()) {
				self.seal_open(c, v);
			}
		}
	}

	fn seal_open(&mut self, c: C, v: V) {
		self.sealed_open = Some(match self.sealed_open.take() {
			Some((sc, sv)) if sc <= c => (sc, sv),
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
			self.sealed_open = Some(match self.sealed_open.take() {
				Some((sc, sv)) if sc <= c => (sc, sv),
				_ => (c, v),
			});
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
		self.base.heap_size() + self.sealed_open.heap_size()
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
	fn sealing_endpoint_freezes_open_and_tracks_live_close() {
		let mut accumulator: SealingEndpoint<DateTime, i64> = SealingEndpoint::amendable(millis(10));
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
	fn sealing_endpoint_roundtrip() {
		let mut ep: SealingEndpoint<DateTime, i64> = SealingEndpoint::amendable(millis(10));
		ep.add(&(at_millis(0), 100));
		ep.add(&(at_millis(12), 300));
		let bytes = ep.encode_state(DateTime::EPOCH).expect("encode");
		let restored: SealingEndpoint<DateTime, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, ep);
	}
}
