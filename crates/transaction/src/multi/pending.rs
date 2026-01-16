// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{
		BTreeMap, HashMap,
		btree_map::{IntoIter as BTreeMapIntoIter, Iter as BTreeMapIter, Range as BTreeMapRange},
	},
	mem::size_of,
	ops::RangeBounds,
};

use reifydb_core::value::encoded::{encoded::EncodedValues, key::EncodedKey};

use crate::multi::types::Pending;

#[derive(Debug, Default, Clone)]
pub struct PendingWrites {
	/// Primary storage - BTreeMap for sorted key access and range queries
	writes: BTreeMap<EncodedKey, Pending>,
	/// Track insertion order for preserving delta ordering
	insertion_order: Vec<EncodedKey>,
	/// Position index: key -> index in insertion_order Vec
	/// This enables O(1) position lookup, eliminating the O(n) linear search bottleneck
	position_index: HashMap<EncodedKey, usize>,
	/// Cached size estimation for batch size limits
	estimated_size: u64,
}

impl PendingWrites {
	/// Create a new empty pending writes manager
	pub fn new() -> Self {
		Self {
			writes: BTreeMap::new(),
			insertion_order: Vec::new(),
			position_index: HashMap::new(),
			estimated_size: 0,
		}
	}

	/// Returns true if there are no pending writes
	#[inline]
	pub fn is_empty(&self) -> bool {
		self.writes.is_empty()
	}

	/// Returns the number of pending writes
	#[inline]
	pub fn len(&self) -> usize {
		self.writes.len()
	}

	/// Returns the maximum batch size in bytes - set high for performance
	#[inline]
	pub fn max_batch_size(&self) -> u64 {
		1024 * 1024 * 1024 // 1GB limit
	}

	/// Returns the maximum number of entries in a batch
	#[inline]
	pub fn max_batch_entries(&self) -> u64 {
		1_000_000 // 1M entries limit
	}

	/// Fast size estimation - uses cached value
	#[inline]
	pub fn estimate_size(&self, _entry: &Pending) -> u64 {
		// Use fixed size estimation for speed
		(size_of::<EncodedKey>() + size_of::<EncodedValues>()) as u64
	}

	/// Get a pending write by key - O(log n) performance
	#[inline]
	pub fn get(&self, key: &EncodedKey) -> Option<&Pending> {
		self.writes.get(key)
	}

	/// Get key-value pair by key - O(log n) performance  
	#[inline]
	pub fn get_entry(&self, key: &EncodedKey) -> Option<(&EncodedKey, &Pending)> {
		self.writes.get_key_value(key)
	}

	/// Check if key exists - O(log n) performance
	#[inline]
	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.writes.contains_key(key)
	}

	/// Insert a new pending write - O(log n) BTreeMap + O(1) HashMap performance
	pub fn insert(&mut self, key: EncodedKey, value: Pending) {
		let size_estimate = self.estimate_size(&value);

		if let Some(old_value) = self.writes.insert(key.clone(), value) {
			// Update existing - might change size
			let old_size = self.estimate_size(&old_value);
			if size_estimate != old_size {
				self.estimated_size =
					self.estimated_size.saturating_sub(old_size).saturating_add(size_estimate);
			}
			// Key already exists in insertion_order and position_index, don't add again
		} else {
			// New entry - track insertion order and position
			let position = self.insertion_order.len();
			self.insertion_order.push(key.clone());
			self.position_index.insert(key, position);
			self.estimated_size = self.estimated_size.saturating_add(size_estimate);
		}
	}

	/// Remove an entry by key - O(log n) BTreeMap + O(1) HashMap lookup + O(1) swap removal
	pub fn remove_entry(&mut self, key: &EncodedKey) -> Option<(EncodedKey, Pending)> {
		if let Some((removed_key, removed_value)) = self.writes.remove_entry(key) {
			if let Some(position) = self.position_index.remove(key) {
				if position < self.insertion_order.len() {
					let swapped_position = self.insertion_order.len() - 1;
					if position != swapped_position {
						self.insertion_order.swap(position, swapped_position);
						if let Some(swapped_key) = self.insertion_order.get(position) {
							self.position_index.insert(swapped_key.clone(), position);
						}
					}
					self.insertion_order.pop();
				}
			}
			let size_estimate = self.estimate_size(&removed_value);
			self.estimated_size = self.estimated_size.saturating_sub(size_estimate);
			Some((removed_key, removed_value))
		} else {
			None
		}
	}

	/// Iterate over all pending writes - returns BTreeMap iterator for sorted access
	pub fn iter(&self) -> BTreeMapIter<'_, EncodedKey, Pending> {
		self.writes.iter()
	}

	/// Consume and iterate over all pending writes in sorted order
	pub fn into_iter(self) -> BTreeMapIntoIter<EncodedKey, Pending> {
		self.writes.into_iter()
	}

	/// Consume and iterate over pending writes in insertion order
	/// Uses the insertion_order Vec to maintain original insertion sequence
	pub fn into_iter_insertion_order(self) -> impl Iterator<Item = (EncodedKey, Pending)> {
		let mut writes = self.writes;
		self.insertion_order.into_iter().filter_map(move |key| writes.remove_entry(&key))
	}

	/// Clear all pending writes
	pub fn rollback(&mut self) {
		self.writes.clear();
		self.insertion_order.clear();
		self.position_index.clear();
		self.estimated_size = 0;
	}

	/// Get estimated total size of all pending writes
	#[inline]
	pub fn total_estimated_size(&self) -> u64 {
		self.estimated_size
	}

	/// Range query support - BTreeMap provides efficient range queries
	pub fn range<R>(&self, range: R) -> BTreeMapRange<'_, EncodedKey, Pending>
	where
		R: RangeBounds<EncodedKey>,
	{
		self.writes.range(range)
	}

	/// Range query with comparable bounds (same as range for compatibility)
	pub fn range_comparable<R>(&self, range: R) -> BTreeMapRange<'_, EncodedKey, Pending>
	where
		R: RangeBounds<EncodedKey>,
	{
		self.writes.range(range)
	}

	/// Optimized get methods for compatibility (same as regular methods)
	#[inline]
	pub fn get_comparable(&self, key: &EncodedKey) -> Option<&Pending> {
		self.get(key)
	}

	#[inline]
	pub fn get_entry_comparable(&self, key: &EncodedKey) -> Option<(&EncodedKey, &Pending)> {
		self.get_entry(key)
	}

	#[inline]
	pub fn contains_key_comparable(&self, key: &EncodedKey) -> bool {
		self.contains_key(key)
	}

	#[inline]
	pub fn remove_entry_comparable(&mut self, key: &EncodedKey) -> Option<(EncodedKey, Pending)> {
		self.remove_entry(key)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{common::CommitVersion, value::encoded::key::EncodedKey};
	use reifydb_type::util::cowvec::CowVec;

	use super::*;

	fn create_test_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn create_test_values(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	fn create_test_pending(version: CommitVersion, key: &str, values_data: &str) -> Pending {
		use reifydb_core::delta::Delta;
		Pending {
			delta: Delta::Set {
				key: create_test_key(key),
				values: create_test_values(values_data),
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
		assert!(pw.total_estimated_size() > 0);

		pw.rollback();

		assert!(pw.is_empty());
		assert_eq!(pw.total_estimated_size(), 0);
	}
}
