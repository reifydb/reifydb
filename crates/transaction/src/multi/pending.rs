// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	mem::{self, size_of},
	ops::RangeBounds,
};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	key::{any::TaggedKey, bound::TaggedKeyBound},
	metrics::heap::HeapSize,
};
use reifydb_value::byte_size::ByteSize;

use crate::multi::types::DeltaEntry;

const SLOT_COPIES_PER_ENTRY: usize = 2;

const ENTRY_OVERHEAD: usize = SLOT_COPIES_PER_ENTRY * (size_of::<EncodedKey>() + size_of::<EncodedBytes>());

#[derive(Debug, Default, Clone)]
pub struct PendingWrites {
	entries: Vec<DeltaEntry>,

	index: BTreeMap<TaggedKeyBound, u32>,
}

impl PendingWrites {
	pub fn new() -> Self {
		Self {
			entries: Vec::new(),
			index: BTreeMap::new(),
		}
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.index.is_empty()
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.index.len()
	}

	#[inline]
	pub fn max_batch_size(&self) -> ByteSize {
		ByteSize::from_gib(1)
	}

	#[inline]
	pub fn max_batch_entries(&self) -> u64 {
		1_000_000
	}

	#[inline]
	pub fn estimate_size(&self, entry: &DeltaEntry) -> ByteSize {
		let payload = entry.key().heap_size() + entry.bytes().map_or(0, |row| row.len());
		ByteSize::from_bytes((ENTRY_OVERHEAD + payload) as u64)
	}

	#[inline]
	fn entry_at(&self, slot: u32) -> Option<&DeltaEntry> {
		self.entries.get(slot as usize)
	}

	#[inline]
	pub fn get(&self, key: &TaggedKey) -> Option<&DeltaEntry> {
		self.index.get(&TaggedKeyBound::Key(key.clone())).and_then(|slot| self.entry_at(*slot))
	}

	pub fn insert(&mut self, value: DeltaEntry) -> Option<DeltaEntry> {
		let key = TaggedKeyBound::Key(value.key().clone());

		if let Some(&slot) = self.index.get(&key) {
			return self.entries.get_mut(slot as usize).map(|entry| mem::replace(entry, value));
		}

		let slot = self.entries.len() as u32;
		self.entries.push(value);
		self.index.insert(key, slot);
		None
	}

	pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&TaggedKeyBound, &DeltaEntry)> + '_ {
		self.index.iter().filter_map(|(key, slot)| self.entry_at(*slot).map(|entry| (key, entry)))
	}

	pub fn rollback(&mut self) {
		self.entries.clear();
		self.index.clear();
	}

	pub fn range<R>(&self, range: R) -> impl DoubleEndedIterator<Item = (&TaggedKeyBound, &DeltaEntry)> + '_
	where
		R: RangeBounds<TaggedKeyBound>,
	{
		self.index.range(range).filter_map(|(key, slot)| self.entry_at(*slot).map(|entry| (key, entry)))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::CommitVersion,
		delta::{Delta, SetPre},
		interface::catalog::id::QueueId,
		key::queue::QueueDeduplicationKey,
	};
	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn create_test_any(s: &str) -> TaggedKey {
		QueueDeduplicationKey::new(QueueId(1), s.as_bytes().iter().map(|b| !b).collect::<Vec<u8>>()).into()
	}

	fn create_test_key(s: &str) -> TaggedKey {
		create_test_any(s)
	}

	fn create_test_bound(s: &str) -> TaggedKeyBound {
		TaggedKeyBound::Key(create_test_any(s))
	}

	fn create_test_bytes(s: &str) -> EncodedBytes {
		EncodedBytes(CowVec::new(s.as_bytes().to_vec()))
	}

	fn create_test_pending(version: CommitVersion, key: &str, values_data: &str) -> DeltaEntry {
		DeltaEntry {
			delta: Delta::Set {
				key: create_test_any(key),
				bytes: create_test_bytes(values_data),
				pre: SetPre::Unknown,
			},
			version,
		}
	}

	#[test]
	fn test_basic_operations() {
		let mut pw = PendingWrites::new();

		assert!(pw.is_empty());
		assert_eq!(pw.len(), 0);

		let key1 = create_test_key("key1");
		let pending1 = create_test_pending(CommitVersion(1), "key1", "value1");

		pw.insert(pending1.clone());

		assert!(!pw.is_empty());
		assert_eq!(pw.len(), 1);
		assert_eq!(pw.get(&key1).unwrap(), &pending1);
	}

	#[test]
	fn test_update_operations() {
		let mut pw = PendingWrites::new();
		let key = create_test_key("key");

		let pending1 = create_test_pending(CommitVersion(1), "key", "value1");
		let pending2 = create_test_pending(CommitVersion(2), "key", "value2");

		pw.insert(pending1);
		assert_eq!(pw.len(), 1);

		pw.insert(pending2.clone());
		assert_eq!(pw.len(), 1); // Still 1, just updated
		assert_eq!(pw.get(&key).unwrap(), &pending2);
	}

	#[test]
	fn test_range_operations() {
		let mut pw = PendingWrites::new();

		for i in 0..10 {
			let pending =
				create_test_pending(CommitVersion(i), &format!("key{:02}", i), &format!("value{}", i));
			pw.insert(pending);
		}

		let start = create_test_bound("key03");
		let end = create_test_bound("key07");

		let range_results: Vec<_> = pw.range(start..end).collect();
		assert_eq!(range_results.len(), 4); // key03, key04, key05, key06
	}

	#[test]
	fn test_iterator_compatibility() {
		let mut pw = PendingWrites::new();

		for i in 0..5 {
			let pending =
				create_test_pending(CommitVersion(i), &format!("key{}", i), &format!("value{}", i));
			pw.insert(pending);
		}

		let iter = pw.iter();
		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 5);

		// iter() must yield key order, not insertion order; the merge with storage relies on it.
		let keys: Vec<_> = items.iter().map(|(k, _)| k).collect();
		let mut expected_keys = keys.clone();
		expected_keys.sort();
		assert_eq!(keys, expected_keys);

		let start = create_test_bound("key1");
		let end = create_test_bound("key4");
		let range_items: Vec<_> = pw.range(start..end).collect();
		assert_eq!(range_items.len(), 3); // key1, key2, key3
	}

	#[test]
	fn test_performance_operations() {
		let mut pw = PendingWrites::new();

		for i in 0..1000 {
			let pending =
				create_test_pending(CommitVersion(i), &format!("key{:06}", i), &format!("value{}", i));
			pw.insert(pending);
		}

		assert_eq!(pw.len(), 1000);

		let lookup_key = create_test_key("key000500");
		assert!(pw.get(&lookup_key).is_some());
	}

	#[test]
	fn test_rollback() {
		let mut pw = PendingWrites::new();

		for i in 0..10 {
			let pending =
				create_test_pending(CommitVersion(i), &format!("key{}", i), &format!("value{}", i));
			pw.insert(pending);
		}

		assert_eq!(pw.len(), 10);

		pw.rollback();

		assert!(pw.is_empty());
	}

	#[test]
	fn rewriting_a_key_replaces_its_value_without_a_new_entry() {
		// A re-write must replace the value and never add an entry, or the commit sees a stale or doubled row.
		let mut pw = PendingWrites::new();
		for name in ["a", "b", "c"] {
			pw.insert(create_test_pending(CommitVersion(1), name, "v1"));
		}

		pw.insert(create_test_pending(CommitVersion(2), "a", "v2"));

		assert_eq!(pw.len(), 3, "a re-write must not add an entry");
		assert_eq!(
			pw.get(&create_test_key("a")).expect("a is present").bytes().expect("a is a set"),
			&create_test_bytes("v2"),
			"the re-write must win on value"
		);
	}

	#[test]
	fn estimate_size_scales_with_payload_not_a_constant() {
		let pw = PendingWrites::new();
		let small = create_test_pending(CommitVersion(1), "k", "v");
		let big = create_test_pending(CommitVersion(1), "k", &"x".repeat(10_000));
		assert!(
			pw.estimate_size(&big) > pw.estimate_size(&small),
			"a 10 KB row must estimate larger than a 1-byte row; a constant estimate is the dead-cap bug"
		);
		assert!(
			pw.estimate_size(&big).as_bytes() >= 10_000,
			"the estimate must include the row's real byte length, got {}",
			pw.estimate_size(&big)
		);
	}

	#[test]
	fn wide_rows_reach_the_byte_cap_before_the_entry_cap() {
		let pw = PendingWrites::new();
		let wide = create_test_pending(CommitVersion(1), "k", &"x".repeat(2 * 1024 * 1024));
		let per_entry = pw.estimate_size(&wide).as_bytes();
		let entries_to_byte_cap = pw.max_batch_size().as_bytes() / per_entry;
		assert!(
			entries_to_byte_cap < pw.max_batch_entries(),
			"a 2 MiB row must trip the 1 GiB byte cap in ~512 entries, far below the 1M entry cap; \
			 modify() checks size >= max_batch_size before cnt >= max_batch_entries, so the byte cap now binds first. \
			 got {} entries to byte cap",
			entries_to_byte_cap
		);
	}
}
