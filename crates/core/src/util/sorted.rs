// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	mem::replace,
	ops::{Bound, RangeBounds},
};

use crate::metrics::heap::HeapSize;

pub struct SortedVecMap<K, V> {
	slots: Vec<(K, V)>,
}

impl<K, V> SortedVecMap<K, V> {
	pub fn new() -> Self {
		Self {
			slots: Vec::new(),
		}
	}

	pub fn len(&self) -> usize {
		self.slots.len()
	}

	pub fn is_empty(&self) -> bool {
		self.slots.is_empty()
	}

	pub fn clear(&mut self) {
		self.slots.clear();
	}

	pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
		self.slots.iter().map(|(key, value)| (key, value))
	}

	pub fn values(&self) -> impl Iterator<Item = &V> {
		self.slots.iter().map(|(_, value)| value)
	}
}

impl<K: Ord, V> SortedVecMap<K, V> {
	fn seek(&self, key: &K) -> Result<usize, usize> {
		self.slots.binary_search_by(|(resident, _)| resident.cmp(key))
	}

	pub fn get(&self, key: &K) -> Option<&V> {
		self.seek(key).ok().map(|position| &self.slots[position].1)
	}

	pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
		self.seek(key).ok().map(|position| &mut self.slots[position].1)
	}

	pub fn contains_key(&self, key: &K) -> bool {
		self.seek(key).is_ok()
	}

	pub fn insert(&mut self, key: K, value: V) -> Option<V> {
		if self.slots.last().is_some_and(|(last, _)| *last < key) {
			self.slots.push((key, value));
			return None;
		}
		match self.seek(&key) {
			Ok(position) => Some(replace(&mut self.slots[position].1, value)),
			Err(position) => {
				self.slots.insert(position, (key, value));
				None
			}
		}
	}

	pub fn remove(&mut self, key: &K) -> Option<V> {
		self.seek(key).ok().map(|position| self.slots.remove(position).1)
	}

	pub fn retain(&mut self, mut keep: impl FnMut(&K, &mut V) -> bool) {
		self.slots.retain_mut(|(key, value)| keep(key, value));
		let target = self.slots.len() * 2;
		if self.slots.capacity() > target {
			self.slots.shrink_to(target);
		}
	}

	pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> impl Iterator<Item = (&K, &V)> {
		let lo = match bounds.start_bound() {
			Bound::Unbounded => 0,
			Bound::Included(key) => self.slots.partition_point(|(resident, _)| resident < key),
			Bound::Excluded(key) => self.slots.partition_point(|(resident, _)| resident <= key),
		};
		let hi = match bounds.end_bound() {
			Bound::Unbounded => self.slots.len(),
			Bound::Included(key) => self.slots.partition_point(|(resident, _)| resident <= key),
			Bound::Excluded(key) => self.slots.partition_point(|(resident, _)| resident < key),
		};
		self.slots[lo..hi.max(lo)].iter().map(|(key, value)| (key, value))
	}
}

impl<K, V> Default for SortedVecMap<K, V> {
	fn default() -> Self {
		Self::new()
	}
}

impl<K: Clone, V: Clone> Clone for SortedVecMap<K, V> {
	fn clone(&self) -> Self {
		Self {
			slots: self.slots.clone(),
		}
	}
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for SortedVecMap<K, V> {
	fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		out.debug_map().entries(self.iter()).finish()
	}
}

impl<K: PartialEq, V: PartialEq> PartialEq for SortedVecMap<K, V> {
	fn eq(&self, other: &Self) -> bool {
		self.slots == other.slots
	}
}

impl<K: Eq, V: Eq> Eq for SortedVecMap<K, V> {}

impl<K, V> IntoIterator for SortedVecMap<K, V> {
	type Item = (K, V);
	type IntoIter = std::vec::IntoIter<(K, V)>;

	fn into_iter(self) -> Self::IntoIter {
		self.slots.into_iter()
	}
}

impl<'a, K, V> IntoIterator for &'a SortedVecMap<K, V> {
	type Item = (&'a K, &'a V);
	type IntoIter = std::iter::Map<std::slice::Iter<'a, (K, V)>, fn(&'a (K, V)) -> (&'a K, &'a V)>;

	fn into_iter(self) -> Self::IntoIter {
		self.slots.iter().map(|(key, value)| (key, value))
	}
}

impl<K: Ord, V> FromIterator<(K, V)> for SortedVecMap<K, V> {
	fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
		let mut sorted: Vec<(K, V)> = iter.into_iter().collect();
		sorted.sort_by(|left, right| left.0.cmp(&right.0));
		let mut slots: Vec<(K, V)> = Vec::with_capacity(sorted.len());
		for slot in sorted {
			if slots.last().is_some_and(|(key, _)| *key == slot.0) {
				slots.pop();
			}
			slots.push(slot);
		}
		Self {
			slots,
		}
	}
}

impl<K: Ord, V> Extend<(K, V)> for SortedVecMap<K, V> {
	fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
		for (key, value) in iter {
			self.insert(key, value);
		}
	}
}

impl<K: HeapSize, V: HeapSize> HeapSize for SortedVecMap<K, V> {
	fn heap_size(&self) -> usize {
		self.slots.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::BTreeMap,
		ops::Bound::{Excluded, Included, Unbounded},
	};

	use super::SortedVecMap;
	use crate::metrics::heap::HeapSize;

	fn scatter(count: u64) -> Vec<u64> {
		let mut state = 0x9E37_79B9_7F4A_7C15u64;
		(0..count)
			.map(|_| {
				state ^= state << 13;
				state ^= state >> 7;
				state ^= state << 17;
				state % 500
			})
			.collect()
	}

	#[test]
	fn a_tail_insert_and_a_scattered_insert_reach_the_same_order() {
		// the push fast path skips the binary search, so it must land a key exactly where a search
		// would have; a divergence here silently corrupts every range and every lookup after it
		let ascending: SortedVecMap<u64, u64> = (0..64u64).map(|key| (key, key * 10)).collect();
		let mut scattered = SortedVecMap::new();
		for key in [31u64, 0, 63, 17, 48, 2] {
			scattered.insert(key, key * 10);
		}
		for key in 0..64u64 {
			scattered.insert(key, key * 10);
		}
		assert_eq!(ascending.iter().collect::<Vec<_>>(), scattered.iter().collect::<Vec<_>>());
	}

	#[test]
	fn every_keyed_operation_agrees_with_a_btreemap() {
		// the whole point of the type is to stand in for a BTreeMap, so any divergence in ordering,
		// replacement or removal is a defect even when the map's own invariants still hold
		let mut sorted = SortedVecMap::new();
		let mut expected = BTreeMap::new();
		for (step, key) in scatter(4000).into_iter().enumerate() {
			if step % 4 == 3 {
				assert_eq!(sorted.remove(&key), expected.remove(&key), "removal at step {step}");
			} else {
				assert_eq!(sorted.insert(key, step), expected.insert(key, step), "insert at {step}");
			}
			assert_eq!(sorted.len(), expected.len(), "length at step {step}");
		}
		assert_eq!(sorted.iter().collect::<Vec<_>>(), expected.iter().collect::<Vec<_>>());
		for key in 0..500u64 {
			assert_eq!(sorted.get(&key), expected.get(&key), "lookup of {key}");
			assert_eq!(sorted.contains_key(&key), expected.contains_key(&key), "membership of {key}");
		}
	}

	#[test]
	fn every_bound_pairing_selects_the_same_span_as_a_btreemap() {
		// the tier only ever asks for an included lower and an excluded or unbounded upper, so the
		// other pairings are unexercised by its tests and an off-by-one here would ship unseen
		let sorted: SortedVecMap<u64, u64> = (0..20u64).map(|key| (key * 2, key)).collect();
		let expected: BTreeMap<u64, u64> = (0..20u64).map(|key| (key * 2, key)).collect();
		for low in 0..12u64 {
			for high in 0..12u64 {
				for lower in [Included(low), Excluded(low), Unbounded] {
					for upper in [Included(high), Excluded(high), Unbounded] {
						let unanswerable = matches!(
							(lower, upper),
							(Included(l) | Excluded(l), Included(h) | Excluded(h)) if l > h
						) || matches!((lower, upper), (Excluded(l), Excluded(h)) if l == h);
						if unanswerable {
							continue;
						}
						let bounds = (lower, upper);
						assert_eq!(
							sorted.range(bounds).collect::<Vec<_>>(),
							expected.range(bounds).collect::<Vec<_>>(),
							"span {bounds:?}"
						);
					}
				}
			}
		}
	}

	#[test]
	fn an_inverted_span_yields_nothing_rather_than_panicking() {
		// slicing with a start past the end panics, and a caller composing bounds from two separate
		// observations can hand us an inverted pair
		let sorted: SortedVecMap<u64, u64> = (0..10u64).map(|key| (key, key)).collect();
		assert_eq!(sorted.range((Included(8), Excluded(2))).count(), 0);
		assert_eq!(sorted.range((Excluded(9), Excluded(0))).count(), 0);
		assert_eq!(sorted.range((Excluded(4), Excluded(4))).count(), 0, "a BTreeMap panics here instead");
	}

	#[test]
	fn collecting_duplicate_keys_keeps_the_last_value() {
		// a BTreeMap built from an iterator keeps the last value for a repeated key, and a caller
		// swapping the types must not silently get the first instead
		let pairs = [(3u64, "a"), (1, "b"), (3, "c"), (1, "d"), (2, "e")];
		let sorted: SortedVecMap<u64, &str> = pairs.into_iter().collect();
		let expected: BTreeMap<u64, &str> = pairs.into_iter().collect();
		assert_eq!(sorted.iter().collect::<Vec<_>>(), expected.iter().collect::<Vec<_>>());
		assert_eq!(sorted.get(&3), Some(&"c"));
		assert_eq!(sorted.get(&1), Some(&"d"));
	}

	#[test]
	fn retain_keeps_order_and_gives_back_the_slack_it_no_longer_needs() {
		// eviction drops most of a partition at once, and a container that keeps the whole capacity
		// leaves the freed bytes charged to nothing
		let mut sorted: SortedVecMap<u64, u64> = (0..1000u64).map(|key| (key, key)).collect();
		let before = sorted.heap_size();
		sorted.retain(|key, _| key % 100 == 0);
		assert_eq!(sorted.len(), 10);
		assert_eq!(
			sorted.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
			(0..10).map(|n| n * 100).collect::<Vec<_>>()
		);
		assert!(sorted.heap_size() < before / 10, "retain must release the capacity it stopped using");
	}

	#[test]
	fn heap_size_counts_the_slots_the_map_actually_holds() {
		// the tier replaced a modelled BTreeMap node cost with this, so it must report allocation
		// rather than another estimate
		let sorted: SortedVecMap<u64, u64> = (0..37u64).map(|key| (key, key)).collect();
		assert_eq!(sorted.heap_size(), 37 * size_of::<(u64, u64)>(), "a collected map is sized to its slots");

		let mut grown: SortedVecMap<u64, u64> = SortedVecMap::new();
		for key in 0..37u64 {
			grown.insert(key, key);
		}
		assert!(
			grown.heap_size() > 37 * size_of::<(u64, u64)>(),
			"a map grown past a doubling holds slack, and reporting only its length hides that"
		);
	}

	#[test]
	fn get_mut_edits_in_place_and_extend_overwrites() {
		let mut sorted: SortedVecMap<u64, u64> = (0..5u64).map(|key| (key, key)).collect();
		*sorted.get_mut(&3).expect("the key was just collected") = 99;
		assert_eq!(sorted.get(&3), Some(&99));
		sorted.extend([(3u64, 7u64), (9, 9)]);
		assert_eq!(sorted.get(&3), Some(&7));
		assert_eq!(sorted.get(&9), Some(&9));
		assert_eq!(sorted.len(), 6);
	}
}
