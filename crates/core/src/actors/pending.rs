// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, iter::once, mem::take, ops::RangeBounds};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};

use crate::delta::RemoveVisibility;

#[derive(Debug, Clone)]
pub enum PendingWrite {
	Set(EncodedBytes),
	Remove {
		announce: RemoveVisibility,
	},
}

#[derive(Debug, Default, Clone)]
pub struct Pending {
	entries: Vec<(EncodedKey, PendingWrite)>,
	index: BTreeMap<EncodedKey, usize>,
}

impl Pending {
	pub fn new() -> Self {
		Self {
			entries: Vec::new(),
			index: BTreeMap::new(),
		}
	}

	fn put(&mut self, key: EncodedKey, write: PendingWrite) {
		if let Some(&slot) = self.index.get(&key) {
			self.entries[slot].1 = write;
			return;
		}
		self.index.insert(key.clone(), self.entries.len());
		self.entries.push((key, write));
	}

	fn write_at(&self, key: &EncodedKey) -> Option<&PendingWrite> {
		self.index.get(key).map(|slot| &self.entries[*slot].1)
	}

	pub fn insert(&mut self, key: EncodedKey, value: EncodedBytes) {
		self.put(key, PendingWrite::Set(value));
	}

	pub fn remove(&mut self, key: EncodedKey) {
		self.put(
			key,
			PendingWrite::Remove {
				announce: RemoveVisibility::Announced,
			},
		);
	}

	pub fn insert_batch(&mut self, keys: &[EncodedKey], values: &[EncodedBytes]) {
		assert_eq!(keys.len(), values.len(), "Pending::insert_batch keys/values length mismatch");
		for (k, v) in keys.iter().zip(values.iter()) {
			self.put(k.clone(), PendingWrite::Set(v.clone()));
		}
	}

	pub fn remove_batch(&mut self, keys: &[EncodedKey]) {
		for k in keys {
			self.put(
				k.clone(),
				PendingWrite::Remove {
					announce: RemoveVisibility::Announced,
				},
			);
		}
	}

	pub fn remove_silent(&mut self, key: EncodedKey) {
		self.put(
			key,
			PendingWrite::Remove {
				announce: RemoveVisibility::Silent,
			},
		);
	}

	pub fn remove_unobserved(&mut self, key: EncodedKey) {
		self.put(
			key,
			PendingWrite::Remove {
				announce: RemoveVisibility::Unobserved,
			},
		);
	}

	pub fn get(&self, key: &EncodedKey) -> Option<&EncodedBytes> {
		match self.write_at(key) {
			Some(PendingWrite::Set(value)) => Some(value),
			_ => None,
		}
	}

	pub fn is_removed(&self, key: &EncodedKey) -> bool {
		matches!(self.write_at(key), Some(PendingWrite::Remove { .. }))
	}

	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.index.contains_key(key)
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}

	pub fn extend_from(&mut self, other: &Pending) {
		for (k, w) in other.iter_ordered() {
			self.put(k.clone(), w.clone());
		}
	}

	pub fn iter_ordered(&self) -> impl DoubleEndedIterator<Item = (&EncodedKey, &PendingWrite)> + '_ {
		self.entries.iter().map(|(k, w)| (k, w))
	}

	pub fn iter_sorted(&self) -> impl DoubleEndedIterator<Item = (&EncodedKey, &PendingWrite)> + '_ {
		self.index.iter().map(|(k, slot)| (k, &self.entries[*slot].1))
	}

	pub fn range<R>(&self, range: R) -> impl DoubleEndedIterator<Item = (&EncodedKey, &PendingWrite)> + '_
	where
		R: RangeBounds<EncodedKey>,
	{
		self.index.range(range).map(|(k, slot)| (k, &self.entries[*slot].1))
	}
}

#[derive(Debug, Default, Clone)]
pub struct PendingLayers {
	layers: Vec<Pending>,
	top: Pending,
}

impl PendingLayers {
	pub fn empty() -> Self {
		Self {
			layers: Vec::new(),
			top: Pending::new(),
		}
	}

	pub fn over(layers: Vec<Pending>) -> Self {
		Self {
			layers,
			top: Pending::new(),
		}
	}

	pub fn with_top(top: Pending) -> Self {
		Self {
			layers: Vec::new(),
			top,
		}
	}

	pub fn depth(&self) -> usize {
		self.layers.len()
	}

	pub fn len(&self) -> usize {
		self.layers.iter().map(|layer| layer.len()).sum::<usize>() + self.top.len()
	}

	pub fn is_empty(&self) -> bool {
		self.top.is_empty() && self.layers.iter().all(|layer| layer.is_empty())
	}

	pub fn top(&self) -> &Pending {
		&self.top
	}

	pub fn take_top(&mut self) -> Pending {
		take(&mut self.top)
	}

	pub fn insert(&mut self, key: EncodedKey, value: EncodedBytes) {
		self.top.insert(key, value);
	}

	pub fn remove(&mut self, key: EncodedKey) {
		self.top.remove(key);
	}

	pub fn remove_silent(&mut self, key: EncodedKey) {
		self.top.remove_silent(key);
	}

	pub fn remove_unobserved(&mut self, key: EncodedKey) {
		self.top.remove_unobserved(key);
	}

	pub fn insert_batch(&mut self, keys: &[EncodedKey], values: &[EncodedBytes]) {
		self.top.insert_batch(keys, values);
	}

	pub fn remove_batch(&mut self, keys: &[EncodedKey]) {
		self.top.remove_batch(keys);
	}

	fn newest_containing(&self, key: &EncodedKey) -> Option<&Pending> {
		if self.top.contains_key(key) {
			return Some(&self.top);
		}
		self.layers.iter().rev().find(|layer| layer.contains_key(key))
	}

	pub fn get(&self, key: &EncodedKey) -> Option<&EncodedBytes> {
		self.newest_containing(key).and_then(|layer| layer.get(key))
	}

	pub fn is_removed(&self, key: &EncodedKey) -> bool {
		self.newest_containing(key).is_some_and(|layer| layer.is_removed(key))
	}

	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.newest_containing(key).is_some()
	}

	pub fn collect_range<R>(&self, range: R, out: &mut BTreeMap<EncodedKey, PendingWrite>)
	where
		R: RangeBounds<EncodedKey> + Clone,
	{
		for layer in self.layers.iter().chain(once(&self.top)) {
			for (key, write) in layer.range(range.clone()) {
				out.insert(key.clone(), write.clone());
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use std::vec;

	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn make_value(s: &str) -> EncodedBytes {
		EncodedBytes(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn test_insert_single_write() {
		let mut pending = Pending::new();
		let key = make_key("key1");
		let value = make_value("value1");

		pending.insert(key.clone(), value.clone());

		assert_eq!(pending.get(&key), Some(&value));
		assert!(!pending.is_removed(&key));
		assert!(pending.contains_key(&key));
	}

	#[test]
	fn test_insert_multiple_writes() {
		let mut pending = Pending::new();

		pending.insert(make_key("key1"), make_value("value1"));
		pending.insert(make_key("key2"), make_value("value2"));
		pending.insert(make_key("key3"), make_value("value3"));

		assert_eq!(pending.get(&make_key("key1")), Some(&make_value("value1")));
		assert_eq!(pending.get(&make_key("key2")), Some(&make_value("value2")));
		assert_eq!(pending.get(&make_key("key3")), Some(&make_value("value3")));
	}

	#[test]
	fn test_insert_overwrites_existing_key() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.insert(key.clone(), make_value("value1"));
		pending.insert(key.clone(), make_value("value2"));

		assert_eq!(pending.get(&key), Some(&make_value("value2")));
	}

	#[test]
	fn test_remove_operation() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.remove(key.clone());

		assert!(pending.is_removed(&key));
		assert!(pending.contains_key(&key));
		assert_eq!(pending.get(&key), None);
	}

	#[test]
	fn test_write_then_remove() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.insert(key.clone(), make_value("value1"));
		assert_eq!(pending.get(&key), Some(&make_value("value1")));

		pending.remove(key.clone());
		assert!(pending.is_removed(&key));
		assert_eq!(pending.get(&key), None);
	}

	#[test]
	fn test_remove_then_write() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.remove(key.clone());
		assert!(pending.is_removed(&key));

		pending.insert(key.clone(), make_value("value1"));
		assert!(!pending.is_removed(&key));
		assert_eq!(pending.get(&key), Some(&make_value("value1")));
	}

	#[test]
	fn test_iter_sorted_order() {
		let mut pending = Pending::new();

		pending.insert(make_key("zebra"), make_value("z"));
		pending.insert(make_key("apple"), make_value("a"));
		pending.insert(make_key("mango"), make_value("m"));

		let keys: Vec<_> = pending.iter_sorted().map(|(k, _)| k.clone()).collect();

		assert_eq!(keys, vec![make_key("apple"), make_key("mango"), make_key("zebra")]);
	}

	#[test]
	fn test_range_query() {
		let mut pending = Pending::new();

		pending.insert(make_key("a"), make_value("1"));
		pending.insert(make_key("b"), make_value("2"));
		pending.insert(make_key("c"), make_value("3"));
		pending.insert(make_key("d"), make_value("4"));

		let range_keys: Vec<_> = pending.range(make_key("b")..make_key("d")).map(|(k, _)| k.clone()).collect();

		assert_eq!(range_keys, vec![make_key("b"), make_key("c")]);
	}

	#[test]
	fn test_range_query_inclusive() {
		let mut pending = Pending::new();

		pending.insert(make_key("a"), make_value("1"));
		pending.insert(make_key("b"), make_value("2"));
		pending.insert(make_key("c"), make_value("3"));

		let range_keys: Vec<_> = pending.range(make_key("a")..=make_key("c")).map(|(k, _)| k.clone()).collect();

		assert_eq!(range_keys, vec![make_key("a"), make_key("b"), make_key("c")]);
	}

	#[test]
	fn test_range_query_empty() {
		let mut pending = Pending::new();

		pending.insert(make_key("a"), make_value("1"));
		pending.insert(make_key("z"), make_value("2"));

		let range_keys: Vec<_> = pending.range(make_key("m")..make_key("n")).map(|(k, _)| k.clone()).collect();

		assert!(range_keys.is_empty());
	}

	#[test]
	fn test_contains_key() {
		let mut pending = Pending::new();

		pending.insert(make_key("key1"), make_value("value1"));
		pending.remove(make_key("key2"));

		assert!(pending.contains_key(&make_key("key1")));
		assert!(pending.contains_key(&make_key("key2"))); // Remove is also "contained"
		assert!(!pending.contains_key(&make_key("key3")));
	}

	#[test]
	fn test_get_nonexistent_key() {
		let pending = Pending::new();
		assert_eq!(pending.get(&make_key("missing")), None);
	}

	#[test]
	fn test_is_removed_nonexistent_key() {
		let pending = Pending::new();
		assert!(!pending.is_removed(&make_key("missing")));
	}

	#[test]
	fn test_mixed_writes_and_removes() {
		let mut pending = Pending::new();

		pending.insert(make_key("write1"), make_value("v1"));
		pending.remove(make_key("remove1"));
		pending.insert(make_key("write2"), make_value("v2"));
		pending.remove(make_key("remove2"));

		assert_eq!(pending.get(&make_key("write1")), Some(&make_value("v1")));
		assert_eq!(pending.get(&make_key("write2")), Some(&make_value("v2")));
		assert!(pending.is_removed(&make_key("remove1")));
		assert!(pending.is_removed(&make_key("remove2")));
		assert_eq!(pending.get(&make_key("remove1")), None);
		assert_eq!(pending.get(&make_key("remove2")), None);
	}

	#[test]
	fn test_is_empty() {
		let mut pending = Pending::new();
		assert!(pending.is_empty());

		pending.insert(make_key("key1"), make_value("value1"));
		assert!(!pending.is_empty());

		let mut tombstones = Pending::new();
		tombstones.remove(make_key("key1"));
		assert!(!tombstones.is_empty());
	}

	#[test]
	fn test_extend_from_newest_wins() {
		let mut base = Pending::new();
		base.insert(make_key("a"), make_value("old"));
		base.insert(make_key("b"), make_value("kept"));
		base.remove_silent(make_key("c"));

		let mut newer = Pending::new();
		newer.insert(make_key("a"), make_value("new"));
		newer.remove(make_key("d"));
		newer.insert(make_key("c"), make_value("revived"));

		base.extend_from(&newer);

		assert_eq!(base.get(&make_key("a")), Some(&make_value("new")));
		assert_eq!(base.get(&make_key("b")), Some(&make_value("kept")));
		assert_eq!(base.get(&make_key("c")), Some(&make_value("revived")));
		assert!(!base.is_removed(&make_key("c")));
		assert!(base.is_removed(&make_key("d")));
	}

	#[test]
	fn test_extend_from_carries_tombstones() {
		let mut base = Pending::new();
		base.insert(make_key("a"), make_value("live"));

		let mut newer = Pending::new();
		newer.remove(make_key("a"));

		base.extend_from(&newer);

		assert!(base.is_removed(&make_key("a")));
		assert_eq!(base.get(&make_key("a")), None);
	}

	#[test]
	fn test_iter_ordered_keeps_write_order_across_overwrite_and_extend() {
		// A commit must replay writes in the order the operators issued them; sorting by key would
		// let a later row with a lower key overtake an earlier one and mint its identity first.
		let mut base = Pending::new();
		base.insert(make_key("zebra"), make_value("z1"));
		base.insert(make_key("apple"), make_value("a"));
		base.insert(make_key("zebra"), make_value("z2"));

		let mut newer = Pending::new();
		newer.insert(make_key("mango"), make_value("m"));
		newer.remove(make_key("apple"));

		base.extend_from(&newer);

		let ordered: Vec<_> = base.iter_ordered().map(|(k, _)| k.clone()).collect();
		assert_eq!(ordered, vec![make_key("zebra"), make_key("apple"), make_key("mango")]);
		assert_eq!(base.get(&make_key("zebra")), Some(&make_value("z2")));
		assert!(base.is_removed(&make_key("apple")));

		let sorted: Vec<_> = base.iter_sorted().map(|(k, _)| k.clone()).collect();
		assert_eq!(sorted, vec![make_key("apple"), make_key("mango"), make_key("zebra")]);
	}

	#[test]
	fn test_iter_sorted_includes_removes() {
		let mut pending = Pending::new();

		pending.insert(make_key("b"), make_value("2"));
		pending.remove(make_key("a"));
		pending.insert(make_key("c"), make_value("3"));

		let items: Vec<_> = pending.iter_sorted().collect();
		assert_eq!(items.len(), 3);

		assert_eq!(items[0].0, &make_key("a"));
		assert!(matches!(items[0].1, PendingWrite::Remove { .. }));

		assert_eq!(items[1].0, &make_key("b"));
		assert!(matches!(items[1].1, PendingWrite::Set(_)));

		assert_eq!(items[2].0, &make_key("c"));
		assert!(matches!(items[2].1, PendingWrite::Set(_)));
	}
}
