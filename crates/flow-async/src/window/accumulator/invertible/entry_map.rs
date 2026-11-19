// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug};

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::accumulator::{MergeAccumulator, WindowAccumulator};

#[operator_state]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryMap<K: Ord, V> {
	entries: BTreeMap<K, V>,
}

impl<K: Ord, V> Default for EntryMap<K, V> {
	fn default() -> Self {
		Self {
			entries: BTreeMap::new(),
		}
	}
}

impl<K: Ord, V> EntryMap<K, V> {
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

impl<K: Ord + HeapSize, V: HeapSize> HeapSize for EntryMap<K, V> {
	fn heap_size(&self) -> usize {
		self.entries.heap_size()
	}
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct EntryMapAccumulator<K: Ord, V> {
	map: EntryMap<K, V>,
}

impl<K: Ord, V> Default for EntryMapAccumulator<K, V> {
	fn default() -> Self {
		Self {
			map: EntryMap::default(),
		}
	}
}

impl<K, V> WindowAccumulator for EntryMapAccumulator<K, V>
where
	K: Ord + Clone + Debug,
	V: Clone + Debug + PartialEq,
	EntryMapAccumulator<K, V>: OperatorState + StateCodec + HeapSize,
{
	type Contributions = Vec<(K, V)>;
	type Output = BTreeMap<K, V>;

	fn add(&mut self, contributions: &[(K, V)]) {
		for (key, value) in contributions {
			self.map.insert(key.clone(), value.clone());
		}
	}

	fn remove(&mut self, contributions: &[(K, V)]) {
		for (key, value) in contributions {
			if self.map.entries().get(key) == Some(value) {
				self.map.remove(key);
			}
		}
	}

	fn finalize(&self) -> Option<BTreeMap<K, V>> {
		(!self.map.is_empty()).then(|| self.map.entries().clone())
	}

	fn rows(&self) -> Option<u64> {
		Some(self.map.len() as u64)
	}
}

impl<K, V> MergeAccumulator for EntryMapAccumulator<K, V>
where
	K: Ord + Clone + Debug,
	V: Clone + Debug + PartialEq,
	EntryMapAccumulator<K, V>: OperatorState + StateCodec + HeapSize,
{
	fn merge(&mut self, other: &Self) {
		for (key, value) in other.map.entries() {
			self.map.insert(key.clone(), value.clone());
		}
	}

	fn compacts(&self) -> bool {
		false
	}
}

impl<K: Ord + HeapSize, V: HeapSize> HeapSize for EntryMapAccumulator<K, V> {
	fn heap_size(&self) -> usize {
		self.map.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::state::decode;
	use reifydb_macro::operator_state;

	use super::*;
	use crate::window::accumulator::{
		CountedAccumulator,
		testkit::{assert_add_remove_is_inverse, assert_order_independent},
	};

	#[operator_state]
	#[derive(Clone, Debug, Default)]
	struct LastAccumulator {
		entry_map: EntryMap<u64, i64>,
	}

	impl HeapSize for LastAccumulator {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for LastAccumulator {
		type Contributions = Vec<(u64, i64)>;
		type Output = i64;

		fn add(&mut self, contributions: &[(u64, i64)]) {
			for contribution in contributions {
				self.entry_map.insert(contribution.0, contribution.1);
			}
		}

		fn remove(&mut self, contributions: &[(u64, i64)]) {
			for contribution in contributions {
				self.entry_map.remove(&contribution.0);
			}
		}

		fn finalize(&self) -> Option<i64> {
			self.entry_map.entries().last_key_value().map(|(_, v)| *v)
		}

		fn rows(&self) -> Option<u64> {
			Some(self.entry_map.len() as u64)
		}
	}

	#[test]
	fn entry_map_add_remove_is_inverse_for_fresh_key() {
		assert_add_remove_is_inverse::<LastAccumulator>(&[(1u64, 10i64), (2, 20)], (3u64, 30i64));
	}

	#[test]
	fn entry_map_is_order_independent_for_distinct_keys() {
		assert_order_independent::<LastAccumulator>(&[(1u64, 10i64), (2, 20), (3, 30)]);
	}

	#[test]
	fn entry_map_add_over_existing_key_then_remove_deletes() {
		let mut accumulator = CountedAccumulator::new(LastAccumulator::default());
		accumulator.add(&[(1u64, 10i64)]);
		accumulator.add(&[(1u64, 99i64)]);
		accumulator.remove(&[(1u64, 99i64)]);
		assert!(accumulator.is_empty());
		assert_eq!(accumulator.finalize(), None);
	}

	#[test]
	fn entry_map_roundtrip() {
		let mut rm: EntryMap<u64, i64> = EntryMap::default();
		rm.insert(1, 10);
		rm.insert(2, 20);
		let bytes = rm.encode_state().expect("encode");
		let restored: EntryMap<u64, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, rm);
		assert_eq!(restored.len(), 2);
	}

	#[test]
	fn entry_map_acc_add_remove_is_inverse_for_fresh_key() {
		assert_add_remove_is_inverse::<EntryMapAccumulator<u64, i64>>(&[(1u64, 10i64), (2, 20)], (3u64, 30i64));
	}

	#[test]
	fn entry_map_acc_is_order_independent_for_distinct_keys() {
		assert_order_independent::<EntryMapAccumulator<u64, i64>>(&[(1u64, 10i64), (2, 20), (3, 30)]);
	}

	#[test]
	fn entry_map_acc_finalize_returns_whole_map() {
		let mut accumulator: CountedAccumulator<EntryMapAccumulator<u64, i64>> =
			CountedAccumulator::new(EntryMapAccumulator::default());
		assert!(accumulator.is_empty());
		assert_eq!(accumulator.finalize(), None);
		accumulator.add(&[(2, 20)]);
		accumulator.add(&[(1, 10)]);
		let map = accumulator.finalize().expect("non-empty");
		assert_eq!(map.len(), 2);
		assert_eq!(map.get(&1), Some(&10));
		assert_eq!(map.get(&2), Some(&20));
	}

	#[test]
	fn entry_map_acc_add_over_existing_key_then_remove_deletes() {
		let mut accumulator: CountedAccumulator<EntryMapAccumulator<u64, i64>> =
			CountedAccumulator::new(EntryMapAccumulator::default());
		accumulator.add(&[(1, 10)]);
		accumulator.add(&[(1, 99)]);
		accumulator.remove(&[(1, 99)]);
		assert!(accumulator.is_empty());
		assert_eq!(accumulator.finalize(), None);
	}

	#[test]
	fn entry_map_acc_remove_of_a_superseded_value_keeps_the_current_one() {
		// An update fans out as remove(pre) then add(post); a reordered pair must not delete the live value.
		let mut accumulator: EntryMapAccumulator<u64, i64> = EntryMapAccumulator::default();
		accumulator.add(&[(1, 10)]);
		accumulator.add(&[(1, 99)]);
		accumulator.remove(&[(1, 10)]);
		let map = accumulator.finalize().expect("key 1 survives");
		assert_eq!(map.get(&1), Some(&99), "removing a superseded value must not delete the key");
	}

	#[test]
	fn a_merge_unions_the_panes_and_the_later_pane_wins_a_shared_key() {
		// Panes merge oldest first, so a key both hold must keep the later pane's value, never the older one.
		let mut older: EntryMapAccumulator<u64, i64> = EntryMapAccumulator::default();
		older.add(&[(1, 10)]);
		older.add(&[(2, 20)]);
		let mut later: EntryMapAccumulator<u64, i64> = EntryMapAccumulator::default();
		later.add(&[(2, 99)]);
		later.add(&[(3, 30)]);
		older.merge(&later);
		let map = older.finalize().expect("merged panes hold entries");
		assert_eq!(map, BTreeMap::from([(1, 10), (2, 99), (3, 30)]));
	}

	#[test]
	fn entry_map_acc_roundtrip() {
		let mut accumulator: EntryMapAccumulator<u64, i64> = EntryMapAccumulator::default();
		accumulator.add(&[(1, 10)]);
		accumulator.add(&[(2, 20)]);
		let bytes = accumulator.encode_state().expect("encode");
		let restored: EntryMapAccumulator<u64, i64> = decode(&bytes).expect("decode");
		assert_eq!(restored, accumulator);
	}

	#[test]
	fn entry_map_does_not_compact() {
		// an entry map keeps every sample, so a rolling window over it must never be accepted
		let accumulator: EntryMapAccumulator<u64, f64> = EntryMapAccumulator::default();
		assert!(!accumulator.compacts());
	}
}
