// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug};

use reifydb_codec::row::operator::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::accumulator::WindowAccumulator;

#[operator_state]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedMap<K: Ord, V> {
	entries: BTreeMap<K, V>,
}

impl<K: Ord, V> Default for RetainedMap<K, V> {
	fn default() -> Self {
		Self {
			entries: BTreeMap::new(),
		}
	}
}

impl<K: Ord, V> RetainedMap<K, V> {
	pub fn insert(&mut self, key: K, value: V) {
		self.entries.insert(key, value);
	}

	pub fn remove(&mut self, key: &K) {
		self.entries.remove(key);
	}

	pub fn entries(&self) -> &BTreeMap<K, V> {
		&self.entries
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}
}

impl<K: Ord + HeapSize, V: HeapSize> HeapSize for RetainedMap<K, V> {
	fn heap_size(&self) -> usize {
		self.entries.heap_size()
	}
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct RetainedAccumulator<K: Ord, V> {
	map: RetainedMap<K, V>,
}

impl<K: Ord, V> Default for RetainedAccumulator<K, V> {
	fn default() -> Self {
		Self {
			map: RetainedMap::default(),
		}
	}
}

impl<K, V> WindowAccumulator for RetainedAccumulator<K, V>
where
	K: Ord + Clone + Debug,
	V: Clone + Debug + PartialEq,
	RetainedAccumulator<K, V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (K, V);
	type Output = BTreeMap<K, V>;

	fn add(&mut self, contribution: &(K, V)) {
		self.map.insert(contribution.0.clone(), contribution.1.clone());
	}

	fn remove(&mut self, contribution: &(K, V)) {
		if self.map.entries().get(&contribution.0) == Some(&contribution.1) {
			self.map.remove(&contribution.0);
		}
	}

	fn finalize(&self) -> Option<BTreeMap<K, V>> {
		(!self.map.is_empty()).then(|| self.map.entries().clone())
	}

	fn is_empty(&self) -> bool {
		self.map.is_empty()
	}
}

impl<K: Ord + HeapSize, V: HeapSize> HeapSize for RetainedAccumulator<K, V> {
	fn heap_size(&self) -> usize {
		self.map.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::decode;
	use reifydb_macro::operator_state;
	use reifydb_value::value::datetime::DateTime;

	use super::*;
	use crate::window::accumulator::testkit::{assert_add_remove_is_inverse, assert_order_independent};

	#[operator_state]
	#[derive(Clone, Debug, Default)]
	struct LastAccumulator {
		retained: RetainedMap<u64, i64>,
	}

	impl HeapSize for LastAccumulator {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for LastAccumulator {
		type Contribution = (u64, i64);
		type Output = i64;

		fn add(&mut self, contribution: &(u64, i64)) {
			self.retained.insert(contribution.0, contribution.1);
		}

		fn remove(&mut self, contribution: &(u64, i64)) {
			self.retained.remove(&contribution.0);
		}

		fn finalize(&self) -> Option<i64> {
			self.retained.entries().last_key_value().map(|(_, v)| *v)
		}

		fn is_empty(&self) -> bool {
			self.retained.is_empty()
		}
	}

	#[test]
	fn retained_add_remove_is_inverse_for_fresh_key() {
		assert_add_remove_is_inverse::<LastAccumulator>(&[(1u64, 10i64), (2, 20)], (3u64, 30i64));
	}

	#[test]
	fn retained_is_order_independent_for_distinct_keys() {
		assert_order_independent::<LastAccumulator>(&[(1u64, 10i64), (2, 20), (3, 30)]);
	}

	#[test]
	fn retained_add_over_existing_key_then_remove_deletes() {
		let mut accumulator = LastAccumulator::default();
		accumulator.add(&(1u64, 10i64));
		accumulator.add(&(1u64, 99i64));
		accumulator.remove(&(1u64, 99i64));
		assert!(accumulator.is_empty());
		assert_eq!(accumulator.finalize(), None);
	}

	#[test]
	fn retained_map_roundtrip() {
		let mut rm: RetainedMap<u64, i64> = RetainedMap::default();
		rm.insert(1, 10);
		rm.insert(2, 20);
		let bytes = rm.encode_state(DateTime::EPOCH).expect("encode");
		let restored: RetainedMap<u64, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, rm);
		assert_eq!(restored.len(), 2);
	}

	#[test]
	fn retained_acc_add_remove_is_inverse_for_fresh_key() {
		assert_add_remove_is_inverse::<RetainedAccumulator<u64, i64>>(&[(1u64, 10i64), (2, 20)], (3u64, 30i64));
	}

	#[test]
	fn retained_acc_is_order_independent_for_distinct_keys() {
		assert_order_independent::<RetainedAccumulator<u64, i64>>(&[(1u64, 10i64), (2, 20), (3, 30)]);
	}

	#[test]
	fn retained_acc_finalize_returns_whole_map() {
		let mut accumulator: RetainedAccumulator<u64, i64> = RetainedAccumulator::default();
		assert!(accumulator.is_empty());
		assert_eq!(accumulator.finalize(), None);
		accumulator.add(&(2, 20));
		accumulator.add(&(1, 10));
		let map = accumulator.finalize().expect("non-empty");
		assert_eq!(map.len(), 2);
		assert_eq!(map.get(&1), Some(&10));
		assert_eq!(map.get(&2), Some(&20));
	}

	#[test]
	fn retained_acc_add_over_existing_key_then_remove_deletes() {
		let mut accumulator: RetainedAccumulator<u64, i64> = RetainedAccumulator::default();
		accumulator.add(&(1, 10));
		accumulator.add(&(1, 99));
		accumulator.remove(&(1, 99));
		assert!(accumulator.is_empty());
		assert_eq!(accumulator.finalize(), None);
	}

	#[test]
	fn retained_acc_remove_of_a_superseded_value_keeps_the_current_one() {
		// An update fans out as remove(pre) then add(post); a reordered pair must not delete the live value.
		let mut accumulator: RetainedAccumulator<u64, i64> = RetainedAccumulator::default();
		accumulator.add(&(1, 10));
		accumulator.add(&(1, 99));
		accumulator.remove(&(1, 10));
		let map = accumulator.finalize().expect("key 1 survives");
		assert_eq!(map.get(&1), Some(&99), "removing a superseded value must not delete the key");
	}

	#[test]
	fn retained_acc_roundtrip() {
		let mut accumulator: RetainedAccumulator<u64, i64> = RetainedAccumulator::default();
		accumulator.add(&(1, 10));
		accumulator.add(&(2, 20));
		let bytes = accumulator.encode_state(DateTime::EPOCH).expect("encode");
		let restored: RetainedAccumulator<u64, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, accumulator);
	}
}
