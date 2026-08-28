// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	marker::PhantomData,
	mem::replace,
	ops::{Bound, RangeBounds},
};

use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{SeqAccess, Visitor},
	ser::SerializeSeq,
};

use crate::metrics::heap::HeapSize;

const DECODE_CAPACITY_CAP: usize = 1024;

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

	pub fn pop_first(&mut self) -> Option<(K, V)> {
		(!self.slots.is_empty()).then(|| self.slots.remove(0))
	}

	pub fn first_key_value(&self) -> Option<(&K, &V)> {
		self.slots.first().map(|(key, value)| (key, value))
	}

	pub fn last_key_value(&self) -> Option<(&K, &V)> {
		self.slots.last().map(|(key, value)| (key, value))
	}

	pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&K, &V)> {
		self.slots.iter().map(|(key, value)| (key, value))
	}

	pub fn keys(&self) -> impl DoubleEndedIterator<Item = &K> {
		self.slots.iter().map(|(key, _)| key)
	}

	pub fn values(&self) -> impl DoubleEndedIterator<Item = &V> {
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

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for SortedVecMap<K, V> {
	fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl<K: Serialize, V: Serialize> Serialize for SortedVecMap<K, V> {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let mut seq = serializer.serialize_seq(Some(self.slots.len()))?;
		for slot in &self.slots {
			seq.serialize_element(slot)?;
		}
		seq.end()
	}
}

struct SlotVisitor<K, V>(PhantomData<(K, V)>);

impl<'de, K: Deserialize<'de> + Ord, V: Deserialize<'de>> Visitor<'de> for SlotVisitor<K, V> {
	type Value = SortedVecMap<K, V>;

	fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
		out.write_str("a sequence of key value pairs")
	}

	fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
		let mut slots: Vec<(K, V)> =
			Vec::with_capacity(seq.size_hint().unwrap_or_default().min(DECODE_CAPACITY_CAP));
		while let Some(slot) = seq.next_element()? {
			slots.push(slot);
		}
		Ok(slots.into_iter().collect())
	}
}

impl<'de, K: Deserialize<'de> + Ord, V: Deserialize<'de>> Deserialize<'de> for SortedVecMap<K, V> {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		deserializer.deserialize_seq(SlotVisitor(PhantomData))
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

	use postcard::{from_bytes, to_allocvec};

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
	fn pop_first_drains_in_the_same_order_as_a_btreemap() {
		// a pop that hands back anything but the lowest key seals a coordinate that was still live
		let mut sorted: SortedVecMap<u64, usize> = SortedVecMap::new();
		let mut expected: BTreeMap<u64, usize> = BTreeMap::new();
		for (step, key) in scatter(200).into_iter().enumerate() {
			sorted.insert(key, step);
			expected.insert(key, step);
		}
		assert!(!expected.is_empty(), "the drain must be exercised against a populated map");

		let mut step = 0;
		loop {
			let popped = sorted.pop_first();
			assert_eq!(popped, expected.pop_first(), "pop at step {step}");
			if popped.is_none() {
				break;
			}
			step += 1;
		}
		assert!(sorted.is_empty());
		assert_eq!(sorted.len(), 0, "a fully drained map must report no slots, not stale capacity");
		assert_eq!(sorted.pop_first(), None, "the aging loop relies on an empty pop being None, not a panic");
	}

	#[test]
	fn last_key_value_and_the_reverse_walk_agree_with_a_btreemap() {
		// the newest live slot is read from the back, so a wrong tail silently changes what it carries
		let empty: SortedVecMap<u64, u64> = SortedVecMap::new();
		assert_eq!(empty.last_key_value(), None);
		assert_eq!(empty.values().next_back(), None);
		assert_eq!(empty.keys().next_back(), None);

		let sorted: SortedVecMap<u64, u64> = scatter(200).into_iter().map(|key| (key, key * 3)).collect();
		let expected: BTreeMap<u64, u64> = scatter(200).into_iter().map(|key| (key, key * 3)).collect();
		assert_eq!(sorted.last_key_value(), expected.last_key_value());
		assert_eq!(sorted.values().next_back(), expected.values().next_back());
		assert_eq!(sorted.keys().next_back(), expected.keys().next_back());

		assert_eq!(
			sorted.iter().rev().collect::<Vec<_>>(),
			expected.iter().rev().collect::<Vec<_>>(),
			"a reverse walk must visit the same pairs in the same order"
		);
		assert_eq!(sorted.keys().rev().count(), sorted.len(), "walking back must visit every slot once");
	}

	#[test]
	fn a_postcard_round_trip_reproduces_the_bytes_a_btreemap_would_have_written() {
		// persisted operator state: an encoding that differs from a BTreeMap orphans every stored window
		for count in [0u64, 1, 37, 300] {
			let sorted: SortedVecMap<u64, u64> = (0..count).map(|key| (key, key * 7)).collect();
			let expected: BTreeMap<u64, u64> = (0..count).map(|key| (key, key * 7)).collect();
			let encoded = to_allocvec(&sorted).expect("a map of plain integers must encode");
			assert_eq!(
				encoded,
				to_allocvec(&expected).expect("a map of plain integers must encode"),
				"{count} slots must encode to the bytes a BTreeMap writes"
			);
			assert_eq!(
				from_bytes::<SortedVecMap<u64, u64>>(&encoded).expect("its own bytes must decode"),
				sorted,
				"{count} slots must survive a round trip"
			);
		}
	}

	#[test]
	fn decoding_an_out_of_order_stream_restores_the_search_invariant() {
		// binary search answers wrongly on an unsorted slice instead of failing, so decode must repair
		let scrambled: Vec<(u64, &str)> = vec![(9, "i"), (2, "b"), (5, "e"), (2, "z"), (0, "a"), (5, "y")];
		let encoded = to_allocvec(&scrambled).expect("a vec of pairs must encode");

		let decoded: SortedVecMap<u64, String> = from_bytes(&encoded).expect("a seq of pairs must decode");

		assert_eq!(
			decoded.keys().copied().collect::<Vec<_>>(),
			vec![0, 2, 5, 9],
			"a decoded map must be sorted and deduped whatever order it arrived in"
		);
		assert_eq!(decoded.get(&2), Some(&"z".to_string()), "a repeated key keeps the last value it carried");
		assert_eq!(decoded.get(&5), Some(&"y".to_string()));
		for key in [0u64, 2, 5, 9] {
			assert!(decoded.contains_key(&key), "every decoded key must be findable by search");
		}
		assert_eq!(decoded.get(&7), None, "a key that was never present must not be invented");
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
