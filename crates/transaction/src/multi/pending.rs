// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, mem::size_of, ops::RangeBounds, vec::IntoIter as VecIntoIter};

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_value::byte_size::ByteSize;

use crate::multi::types::DeltaEntry;

const SLOT_COPIES_PER_ENTRY: usize = 2;

const ENTRY_OVERHEAD: usize = SLOT_COPIES_PER_ENTRY * (size_of::<EncodedKey>() + size_of::<EncodedRow>());

#[derive(Debug, Default, Clone)]
pub struct PendingWrites {
	entries: Vec<Option<DeltaEntry>>,

	index: BTreeMap<EncodedKey, u32>,

	estimated_size: ByteSize,
}

impl PendingWrites {
	pub fn new() -> Self {
		Self {
			entries: Vec::new(),
			index: BTreeMap::new(),
			estimated_size: ByteSize::ZERO,
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
		let payload = entry.key().heap_bytes() + entry.row().map_or(0, |row| row.len());
		ByteSize::from_bytes((ENTRY_OVERHEAD + payload) as u64)
	}

	#[inline]
	fn entry_at(&self, slot: u32) -> Option<&DeltaEntry> {
		self.entries.get(slot as usize).and_then(|entry| entry.as_ref())
	}

	#[inline]
	pub fn get(&self, key: &EncodedKey) -> Option<&DeltaEntry> {
		self.index.get(key).and_then(|slot| self.entry_at(*slot))
	}

	#[inline]
	pub fn get_entry(&self, key: &EncodedKey) -> Option<(&EncodedKey, &DeltaEntry)> {
		let (key, slot) = self.index.get_key_value(key)?;
		self.entry_at(*slot).map(|entry| (key, entry))
	}

	#[inline]
	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.index.contains_key(key)
	}

	pub fn insert(&mut self, key: EncodedKey, value: DeltaEntry) {
		let size_estimate = self.estimate_size(&value);

		if let Some(&slot) = self.index.get(&key) {
			let pre_size = self.entry_at(slot).map(|pre| self.estimate_size(pre));
			if let Some(entry) = self.entries.get_mut(slot as usize) {
				*entry = Some(value);
			}
			if let Some(pre_size) = pre_size
				&& size_estimate != pre_size
			{
				self.estimated_size =
					self.estimated_size.saturating_sub(pre_size).saturating_add(size_estimate);
			}
			return;
		}

		let slot = self.entries.len() as u32;
		self.entries.push(Some(value));
		self.index.insert(key, slot);
		self.estimated_size = self.estimated_size.saturating_add(size_estimate);
	}

	pub fn remove_entry(&mut self, key: &EncodedKey) -> Option<(EncodedKey, DeltaEntry)> {
		let (removed_key, slot) = self.index.remove_entry(key)?;
		let removed_value = self.entries.get_mut(slot as usize).and_then(Option::take)?;
		let size_estimate = self.estimate_size(&removed_value);
		self.estimated_size = self.estimated_size.saturating_sub(size_estimate);
		Some((removed_key, removed_value))
	}

	pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&EncodedKey, &DeltaEntry)> + '_ {
		self.index.iter().filter_map(|(key, slot)| self.entry_at(*slot).map(|entry| (key, entry)))
	}

	pub fn into_iter_insertion_order(self) -> impl Iterator<Item = (EncodedKey, DeltaEntry)> {
		self.entries.into_iter().flatten().map(|entry| (entry.key().clone(), entry))
	}

	pub fn rollback(&mut self) {
		self.entries.clear();
		self.index.clear();
		self.estimated_size = ByteSize::ZERO;
	}

	#[inline]
	pub fn total_estimated_size(&self) -> ByteSize {
		self.estimated_size
	}

	pub fn range<R>(&self, range: R) -> impl DoubleEndedIterator<Item = (&EncodedKey, &DeltaEntry)> + '_
	where
		R: RangeBounds<EncodedKey>,
	{
		self.index.range(range).filter_map(|(key, slot)| self.entry_at(*slot).map(|entry| (key, entry)))
	}
}

impl IntoIterator for PendingWrites {
	type Item = (EncodedKey, DeltaEntry);
	type IntoIter = VecIntoIter<(EncodedKey, DeltaEntry)>;

	fn into_iter(self) -> Self::IntoIter {
		self.into_iter_insertion_order().collect::<Vec<_>>().into_iter()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{common::CommitVersion, delta::Delta};
	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn create_test_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn create_test_row(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(s.as_bytes().to_vec()))
	}

	fn create_test_pending(version: CommitVersion, key: &str, values_data: &str) -> DeltaEntry {
		DeltaEntry {
			delta: Delta::Set {
				key: create_test_key(key),
				row: create_test_row(values_data),
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

		pw.insert(key1.clone(), pending1.clone());

		assert!(!pw.is_empty());
		assert_eq!(pw.len(), 1);
		assert!(pw.contains_key(&key1));
		assert_eq!(pw.get(&key1).unwrap(), &pending1);
	}

	#[test]
	fn test_update_operations() {
		let mut pw = PendingWrites::new();
		let key = create_test_key("key");

		let pending1 = create_test_pending(CommitVersion(1), "key", "value1");
		let pending2 = create_test_pending(CommitVersion(2), "key", "value2");

		pw.insert(key.clone(), pending1);
		assert_eq!(pw.len(), 1);

		pw.insert(key.clone(), pending2.clone());
		assert_eq!(pw.len(), 1); // Still 1, just updated
		assert_eq!(pw.get(&key).unwrap(), &pending2);
	}

	#[test]
	fn test_range_operations() {
		let mut pw = PendingWrites::new();

		for i in 0..10 {
			let key = create_test_key(&format!("key{:02}", i));
			let pending =
				create_test_pending(CommitVersion(i), &format!("key{:02}", i), &format!("value{}", i));
			pw.insert(key, pending);
		}

		let start = create_test_key("key03");
		let end = create_test_key("key07");

		let range_results: Vec<_> = pw.range(start..end).collect();
		assert_eq!(range_results.len(), 4); // key03, key04, key05, key06
	}

	#[test]
	fn test_iterator_compatibility() {
		let mut pw = PendingWrites::new();

		// Test that iterators work with transaction system expectations
		for i in 0..5 {
			let key = create_test_key(&format!("key{}", i));
			let pending =
				create_test_pending(CommitVersion(i), &format!("key{}", i), &format!("value{}", i));
			pw.insert(key, pending);
		}

		// Test that iter() returns the expected BTreeMap iterator type
		let iter = pw.iter();
		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 5);

		// Test that the iterator is ordered (important for BTreeMap)
		let keys: Vec<_> = items.iter().map(|(k, _)| k).collect();
		let mut expected_keys = keys.clone();
		expected_keys.sort();
		assert_eq!(keys, expected_keys);

		// Test range queries
		let start = create_test_key("key1");
		let end = create_test_key("key4");
		let range_items: Vec<_> = pw.range(start..end).collect();
		assert_eq!(range_items.len(), 3); // key1, key2, key3
	}

	#[test]
	fn test_performance_operations() {
		let mut pw = PendingWrites::new();

		// Test with larger dataset to verify performance
		// characteristics
		for i in 0..1000 {
			let key = create_test_key(&format!("key{:06}", i));
			let pending =
				create_test_pending(CommitVersion(i), &format!("key{:06}", i), &format!("value{}", i));
			pw.insert(key, pending);
		}

		assert_eq!(pw.len(), 1000);

		// Test fast lookups
		let lookup_key = create_test_key("key000500");
		assert!(pw.contains_key(&lookup_key));
		assert!(pw.get(&lookup_key).is_some());

		// Test removal
		let removed = pw.remove_entry(&lookup_key);
		assert!(removed.is_some());
		assert_eq!(pw.len(), 999);
		assert!(!pw.contains_key(&lookup_key));
	}

	#[test]
	fn test_rollback() {
		let mut pw = PendingWrites::new();

		for i in 0..10 {
			let key = create_test_key(&format!("key{}", i));
			let pending =
				create_test_pending(CommitVersion(i), &format!("key{}", i), &format!("value{}", i));
			pw.insert(key, pending);
		}

		assert_eq!(pw.len(), 10);
		assert!(pw.total_estimated_size() > ByteSize::ZERO);

		pw.rollback();

		assert!(pw.is_empty());
		assert_eq!(pw.total_estimated_size(), ByteSize::ZERO);
	}

	fn insertion_keys(pw: &PendingWrites) -> Vec<String> {
		pw.clone()
			.into_iter_insertion_order()
			.map(|(key, _)| String::from_utf8(key.to_vec()).expect("test keys are utf8"))
			.collect()
	}

	// A removal must not disturb where any other key sits. The previous swap-remove moved the tail key into the
	// vacated slot, so removing B silently transposed C and D. Downstream commit ordering is derived from this
	// sequence, so a transposition here reorders the deltas a commit publishes.
	#[test]
	fn removing_a_middle_key_leaves_every_other_position_untouched() {
		let mut pw = PendingWrites::new();
		for name in ["a", "b", "c", "d"] {
			pw.insert(create_test_key(name), create_test_pending(CommitVersion(1), name, "v"));
		}

		pw.remove_entry(&create_test_key("b"));

		assert_eq!(
			insertion_keys(&pw),
			vec!["a", "c", "d"],
			"removing b must leave a, c, d in their original order; swap-remove yields a, d, c"
		);
	}

	// A re-write updates a key in place rather than moving it to the end. This is what optimize_deltas already
	// produces for the primary path, which sorts surviving deltas by first-appearance index; a pending-writes
	// order of last-write-wins would disagree with the deltas the primary commits for the same transaction.
	#[test]
	fn rewriting_a_key_keeps_its_first_insertion_position() {
		let mut pw = PendingWrites::new();
		for name in ["a", "b", "c"] {
			pw.insert(create_test_key(name), create_test_pending(CommitVersion(1), name, "v1"));
		}

		pw.insert(create_test_key("a"), create_test_pending(CommitVersion(2), "a", "v2"));

		assert_eq!(insertion_keys(&pw), vec!["a", "b", "c"], "a must hold its first position after a re-write");
		assert_eq!(pw.len(), 3, "a re-write must not add an entry");
		assert_eq!(
			pw.get(&create_test_key("a")).expect("a is present").row().expect("a is a set"),
			&create_test_row("v2"),
			"the re-write must win on value even though it keeps the old position"
		);
	}

	#[test]
	fn removing_then_reinserting_appends_at_the_end() {
		let mut pw = PendingWrites::new();
		for name in ["a", "b"] {
			pw.insert(create_test_key(name), create_test_pending(CommitVersion(1), name, "v"));
		}

		pw.remove_entry(&create_test_key("a"));
		pw.insert(create_test_key("a"), create_test_pending(CommitVersion(1), "a", "v"));

		assert_eq!(
			insertion_keys(&pw),
			vec!["b", "a"],
			"a removed key loses its slot, so re-inserting it makes it the newest entry"
		);
	}

	#[test]
	fn removing_the_only_entry_empties_the_order() {
		let mut pw = PendingWrites::new();
		pw.insert(create_test_key("a"), create_test_pending(CommitVersion(1), "a", "v"));

		pw.remove_entry(&create_test_key("a"));

		assert!(pw.is_empty());
		assert!(insertion_keys(&pw).is_empty());
		assert_eq!(pw.total_estimated_size(), ByteSize::ZERO, "removing the last entry must zero the estimate");
	}

	#[test]
	fn removing_a_missing_key_changes_nothing() {
		let mut pw = PendingWrites::new();
		pw.insert(create_test_key("a"), create_test_pending(CommitVersion(1), "a", "v"));
		let before = pw.total_estimated_size();

		assert!(pw.remove_entry(&create_test_key("zzz")).is_none());

		assert_eq!(insertion_keys(&pw), vec!["a"]);
		assert_eq!(pw.total_estimated_size(), before, "a failed removal must not touch the size estimate");
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
