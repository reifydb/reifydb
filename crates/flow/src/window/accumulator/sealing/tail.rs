// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug};

use reifydb_codec::row::operator::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::{
	accumulator::{WindowAccumulator, sealing::base::SealingBase},
	span::{Slot, SlotSpan},
};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct SealingTail<C: Slot, V> {
	base: SealingBase<C, V>,
}

impl<C: Slot, V> Default for SealingTail<C, V> {
	fn default() -> Self {
		Self {
			base: SealingBase::default(),
		}
	}
}

impl<C: Slot, V: Clone> SealingTail<C, V> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			base: SealingBase::amendable(amendable),
		}
	}

	pub fn add(&mut self, coord: C, value: V) {
		self.base.push(coord, value);
	}

	pub fn remove(&mut self, coord: &C) {
		self.base.remove(coord);
	}

	pub fn tail(&self) -> &BTreeMap<C, V> {
		self.base.tail()
	}

	pub fn is_empty(&self) -> bool {
		self.base.is_tail_empty()
	}
}

impl<C: Slot + HeapSize, V: HeapSize> HeapSize for SealingTail<C, V> {
	fn heap_size(&self) -> usize {
		self.base.heap_size()
	}
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct TailAccumulator<C: Slot, V> {
	events: SealingTail<C, V>,
}

impl<C: Slot, V> Default for TailAccumulator<C, V> {
	fn default() -> Self {
		Self {
			events: SealingTail::default(),
		}
	}
}

impl<C: Slot, V: Clone> TailAccumulator<C, V> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			events: SealingTail::amendable(amendable),
		}
	}
}

impl<C, V> WindowAccumulator for TailAccumulator<C, V>
where
	C: Slot,
	V: Clone + Debug + PartialEq,
	TailAccumulator<C, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (C, V);
	type Output = BTreeMap<C, V>;

	fn add(&mut self, contribution: &(C, V)) {
		self.events.add(contribution.0, contribution.1.clone());
	}

	fn remove(&mut self, contribution: &(C, V)) {
		self.events.remove(&contribution.0);
	}

	fn finalize(&self) -> Option<BTreeMap<C, V>> {
		(!self.events.is_empty()).then(|| self.events.tail().clone())
	}

	fn is_empty(&self) -> bool {
		self.events.is_empty()
	}
}

impl<C: Slot + HeapSize, V: HeapSize> HeapSize for TailAccumulator<C, V> {
	fn heap_size(&self) -> usize {
		self.events.heap_size()
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
	use crate::window::accumulator::testkit::{Op, assert_add_remove_is_inverse, assert_arms_agree, drive};

	#[test]
	fn sealing_tail_drops_aged_keeps_recent() {
		let mut tail: SealingTail<DateTime, i64> = SealingTail::amendable(millis(10));
		tail.add(at_millis(0), 1);
		tail.add(at_millis(5), 2);
		tail.add(at_millis(12), 3);
		let keys: Vec<DateTime> = tail.tail().keys().copied().collect();
		assert_eq!(keys, vec![at_millis(5), at_millis(12)], "aged prefix dropped, recent tail kept in order");
		tail.remove(&at_millis(5));
		let keys: Vec<DateTime> = tail.tail().keys().copied().collect();
		assert_eq!(keys, vec![at_millis(12)], "live tail entry removable");
	}

	#[test]
	fn sealing_tail_default_never_drops() {
		let mut tail: SealingTail<DateTime, i64> = SealingTail::default();
		tail.add(at_millis(0), 1);
		tail.add(at_millis(100), 2);
		assert_eq!(tail.tail().len(), 2, "with no amendable bound nothing is dropped");
	}

	#[test]
	fn sealing_tail_roundtrip() {
		let mut tail: SealingTail<DateTime, i64> = SealingTail::amendable(millis(10));
		tail.add(at_millis(0), 1);
		tail.add(at_millis(12), 3);
		let bytes = tail.encode_state(DateTime::EPOCH).expect("encode");
		let restored: SealingTail<DateTime, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, tail);
	}

	#[test]
	fn tail_acc_no_seal_retains_whole_window_like_retained_acc() {
		let mut accumulator: TailAccumulator<DateTime, i64> = TailAccumulator::default();
		accumulator.add(&(at_millis(0), 10));
		accumulator.add(&(at_millis(100), 20));
		let map = accumulator.finalize().expect("non-empty");
		assert_eq!(map.len(), 2);
		assert_eq!(map.get(&at_millis(0)), Some(&10));
		assert_eq!(map.get(&at_millis(100)), Some(&20));
	}

	#[test]
	fn tail_acc_default_add_remove_is_inverse() {
		assert_add_remove_is_inverse::<TailAccumulator<DateTime, i64>>(
			&[(at_millis(0), 10i64), (at_millis(1), 20)],
			(at_millis(2), 30i64),
		);
	}

	#[test]
	fn tail_acc_default_drains_to_empty_and_finalizes_to_none() {
		// The sealed arm can never drain once a row ages out; with no span the window must be reclaimable again.
		let mut accumulator: TailAccumulator<DateTime, i64> = TailAccumulator::default();
		accumulator.add(&(at_millis(0), 10));
		accumulator.add(&(at_millis(1_000_000), 20));
		assert_eq!(accumulator.finalize().expect("non-empty").len(), 2);

		accumulator.remove(&(at_millis(0), 10));
		accumulator.remove(&(at_millis(1_000_000), 20));
		assert!(accumulator.is_empty(), "every row stays retractable while nothing seals");
		assert_eq!(accumulator.finalize(), None);
	}

	#[test]
	fn tail_acc_with_amendable_drops_aged_from_finalize() {
		let mut accumulator: TailAccumulator<DateTime, i64> = TailAccumulator::amendable(millis(10));
		accumulator.add(&(at_millis(0), 10));
		accumulator.add(&(at_millis(5), 20));
		accumulator.add(&(at_millis(12), 30));
		let map = accumulator.finalize().expect("non-empty");
		assert_eq!(
			map.keys().copied().collect::<Vec<_>>(),
			vec![at_millis(5), at_millis(12)],
			"aged prefix dropped from the emitted map"
		);
	}

	#[test]
	fn tail_acc_emits_a_suffix_of_what_the_unsealed_arm_holds() {
		// The sealed arm may drop an aged prefix but must never rewrite or reorder a surviving value.
		let ops = [
			Op::Add((at_millis(0), 10i64)),
			Op::Add((at_millis(5), 20)),
			Op::Add((at_millis(12), 30)),
			Op::Add((at_millis(14), 40)),
		];
		let mut sealed: TailAccumulator<DateTime, i64> = TailAccumulator::amendable(millis(10));
		drive(&mut sealed, &ops);
		let mut unsealed: TailAccumulator<DateTime, i64> = TailAccumulator::default();
		drive(&mut unsealed, &ops);

		let sealed_map = sealed.finalize().expect("non-empty");
		let unsealed_map = unsealed.finalize().expect("non-empty");
		assert_eq!(unsealed_map.len(), 4);
		let surviving: BTreeMap<DateTime, i64> =
			unsealed_map.iter().filter(|(k, _)| **k >= at_millis(5)).map(|(k, v)| (*k, *v)).collect();
		assert_eq!(sealed_map, surviving);
	}

	#[test]
	fn tail_acc_with_an_amendable_span_beyond_the_data_matches_the_unsealed_arm() {
		// Nothing ages in either arm, so any divergence is an aging rule that fired when it must not.
		assert_arms_agree(
			TailAccumulator::<DateTime, i64>::amendable(millis(1_000)),
			TailAccumulator::<DateTime, i64>::default(),
			&[
				Op::Add((at_millis(0), 10)),
				Op::Add((at_millis(5), 20)),
				Op::Remove((at_millis(5), 20)),
				Op::Add((at_millis(9), 30)),
			],
			"with nothing sealed the two arms must hold the same map",
		);
	}

	#[test]
	fn tail_acc_roundtrip() {
		let mut accumulator: TailAccumulator<DateTime, i64> = TailAccumulator::amendable(millis(10));
		accumulator.add(&(at_millis(0), 1));
		accumulator.add(&(at_millis(12), 3));
		let bytes = accumulator.encode_state(DateTime::EPOCH).expect("encode");
		let restored: TailAccumulator<DateTime, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, accumulator);
	}
}
