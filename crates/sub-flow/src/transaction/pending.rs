// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{
		BTreeMap,
		btree_map::{Iter, Range},
	},
	ops::RangeBounds,
};

use reifydb_core::{EncodedKey, value::encoded::EncodedValues};

/// Represents a pending operation on a key
#[derive(Debug, Clone)]
pub enum Pending {
	Set(EncodedValues),
	Remove,
}

/// Manages pending writes and removes with sorted key access and insertion order tracking
#[derive(Debug, Default, Clone)]
pub struct PendingWrites {
	/// Primary storage - BTreeMap for sorted key access and range queries
	writes: BTreeMap<EncodedKey, Pending>,
	/// Track insertion order for preserving delta ordering
	insertion_order: Vec<EncodedKey>,
	/// Cached size estimation for batch size limits
	estimated_size: u64,
}

impl PendingWrites {
	/// Create a new empty pending writes manager
	pub fn new() -> Self {
		Self {
			writes: BTreeMap::new(),
			insertion_order: Vec::new(),
			estimated_size: 0,
		}
	}

	/// Insert a write operation
	pub fn insert(&mut self, key: EncodedKey, value: EncodedValues) {
		let new_size = key.len() as u64 + value.len() as u64;

		// Track insertion order and update size estimate correctly
		if let Some(existing) = self.writes.get(&key) {
			// Key exists - replace old size estimate with new one
			let old_size = match existing {
				Pending::Set(v) => key.len() as u64 + v.len() as u64,
				Pending::Remove => key.len() as u64,
			};
			self.estimated_size = self.estimated_size.saturating_sub(old_size).saturating_add(new_size);
		} else {
			// New key - add to insertion order and size
			self.insertion_order.push(key.clone());
			self.estimated_size = self.estimated_size.saturating_add(new_size);
		}

		self.writes.insert(key, Pending::Set(value));
	}

	/// Insert a remove operation
	pub fn remove(&mut self, key: EncodedKey) {
		let remove_size = key.len() as u64;

		// Update size estimate and track insertion order
		if let Some(existing) = self.writes.get(&key) {
			// Key exists - update size estimate
			let old_size = match existing {
				Pending::Set(v) => key.len() as u64 + v.len() as u64,
				Pending::Remove => key.len() as u64,
			};
			self.estimated_size = self.estimated_size.saturating_sub(old_size).saturating_add(remove_size);
		} else {
			// New key - add to insertion order and size
			self.insertion_order.push(key.clone());
			self.estimated_size = self.estimated_size.saturating_add(remove_size);
		}

		self.writes.insert(key, Pending::Remove);
	}

	/// Get a value if it exists and is a write (not a remove)
	pub fn get(&self, key: &EncodedKey) -> Option<&EncodedValues> {
		match self.writes.get(key) {
			Some(Pending::Set(value)) => Some(value),
			_ => None,
		}
	}

	/// Check if a key is marked for removal
	pub fn is_removed(&self, key: &EncodedKey) -> bool {
		matches!(self.writes.get(key), Some(Pending::Remove))
	}

	/// Check if a key exists (either as write or remove)
	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.writes.contains_key(key)
	}

	/// Iterate over all pending operations in sorted key order
	pub fn iter_sorted(&self) -> Iter<'_, EncodedKey, Pending> {
		self.writes.iter()
	}

	/// Range query over pending operations in sorted key order
	pub fn range<R>(&self, range: R) -> Range<'_, EncodedKey, Pending>
	where
		R: RangeBounds<EncodedKey>,
	{
		self.writes.range(range)
	}

	/// Iterate over all pending operations in insertion order
	pub fn iter_insertion_order(&self) -> impl Iterator<Item = (&EncodedKey, &Pending)> + '_ {
		self.insertion_order.iter().filter_map(move |key| self.writes.get(key).map(|pending| (key, pending)))
	}

	/// Clear all pending operations
	pub fn clear(&mut self) {
		self.writes.clear();
		self.insertion_order.clear();
		self.estimated_size = 0;
	}

	/// Get the estimated size in bytes
	pub fn estimated_size(&self) -> u64 {
		self.estimated_size
	}

	/// Get the number of pending operations
	pub fn len(&self) -> usize {
		self.writes.len()
	}

	/// Check if there are no pending operations
	pub fn is_empty(&self) -> bool {
		self.writes.is_empty()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::CowVec;

	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn test_new_is_empty() {
		let pending = PendingWrites::new();
		assert!(pending.is_empty());
		assert_eq!(pending.len(), 0);
		assert_eq!(pending.estimated_size(), 0);
	}

	#[test]
	fn test_insert_single_write() {
		let mut pending = PendingWrites::new();
		let key = make_key("key1");
		let value = make_value("value1");

		pending.insert(key.clone(), value.clone());

		assert_eq!(pending.len(), 1);
		assert!(!pending.is_empty());
		assert_eq!(pending.get(&key), Some(&value));
		assert!(!pending.is_removed(&key));
		assert!(pending.contains_key(&key));
	}

	#[test]
	fn test_insert_multiple_writes() {
		let mut pending = PendingWrites::new();

		pending.insert(make_key("key1"), make_value("value1"));
		pending.insert(make_key("key2"), make_value("value2"));
		pending.insert(make_key("key3"), make_value("value3"));

		assert_eq!(pending.len(), 3);
		assert_eq!(pending.get(&make_key("key1")), Some(&make_value("value1")));
		assert_eq!(pending.get(&make_key("key2")), Some(&make_value("value2")));
		assert_eq!(pending.get(&make_key("key3")), Some(&make_value("value3")));
	}

	#[test]
	fn test_insert_overwrites_existing_key() {
		let mut pending = PendingWrites::new();
		let key = make_key("key1");

		pending.insert(key.clone(), make_value("value1"));
		pending.insert(key.clone(), make_value("value2"));

		assert_eq!(pending.len(), 1);
		assert_eq!(pending.get(&key), Some(&make_value("value2")));
	}

	#[test]
	fn test_remove_operation() {
		let mut pending = PendingWrites::new();
		let key = make_key("key1");

		pending.remove(key.clone());

		assert_eq!(pending.len(), 1);
		assert!(pending.is_removed(&key));
		assert!(pending.contains_key(&key));
		assert_eq!(pending.get(&key), None); // Remove returns None for get
	}

	#[test]
	fn test_write_then_remove() {
		let mut pending = PendingWrites::new();
		let key = make_key("key1");

		pending.insert(key.clone(), make_value("value1"));
		assert_eq!(pending.get(&key), Some(&make_value("value1")));

		pending.remove(key.clone());
		assert!(pending.is_removed(&key));
		assert_eq!(pending.get(&key), None);
		assert_eq!(pending.len(), 1); // Still one entry, but marked as Remove
	}

	#[test]
	fn test_remove_then_write() {
		let mut pending = PendingWrites::new();
		let key = make_key("key1");

		pending.remove(key.clone());
		assert!(pending.is_removed(&key));

		pending.insert(key.clone(), make_value("value1"));
		assert!(!pending.is_removed(&key));
		assert_eq!(pending.get(&key), Some(&make_value("value1")));
		assert_eq!(pending.len(), 1);
	}

	#[test]
	fn test_estimated_size_tracking() {
		let mut pending = PendingWrites::new();

		let key1 = make_key("k1");
		let val1 = make_value("v1");
		let expected_size = key1.len() as u64 + val1.len() as u64;

		pending.insert(key1.clone(), val1);
		assert_eq!(pending.estimated_size(), expected_size);

		let key2 = make_key("k2");
		let val2 = make_value("v2");
		let expected_size2 = expected_size + key2.len() as u64 + val2.len() as u64;

		pending.insert(key2, val2);
		assert_eq!(pending.estimated_size(), expected_size2);
	}

	#[test]
	fn test_estimated_size_on_overwrite() {
		let mut pending = PendingWrites::new();
		let key = make_key("key1");
		let short_val = make_value("short");
		let long_val = make_value("much longer value");

		// First insert
		pending.insert(key.clone(), short_val.clone());
		let expected_size1 = key.len() as u64 + short_val.len() as u64;
		assert_eq!(pending.estimated_size(), expected_size1, "Initial size should be key + value");

		// Overwrite with longer value
		pending.insert(key.clone(), long_val.clone());
		let expected_size2 = key.len() as u64 + long_val.len() as u64;
		assert_eq!(pending.estimated_size(), expected_size2, "Size should replace, not accumulate");

		// Verify only one entry exists
		assert_eq!(pending.len(), 1, "Should have exactly one entry after overwrite");
	}

	#[test]
	fn test_estimated_size_with_removes() {
		let mut pending = PendingWrites::new();
		let key = make_key("key1");
		let value = make_value("value1");

		// Insert a write
		pending.insert(key.clone(), value.clone());
		let write_size = key.len() as u64 + value.len() as u64;
		assert_eq!(pending.estimated_size(), write_size, "Write size should be key + value");

		// Overwrite with remove
		pending.remove(key.clone());
		let remove_size = key.len() as u64;
		assert_eq!(pending.estimated_size(), remove_size, "Remove size should be just the key");

		// Insert again
		pending.insert(key.clone(), value.clone());
		assert_eq!(pending.estimated_size(), write_size, "Size should be back to key + value");
	}

	#[test]
	fn test_iter_sorted_order() {
		let mut pending = PendingWrites::new();

		// Insert in non-sorted order
		pending.insert(make_key("zebra"), make_value("z"));
		pending.insert(make_key("apple"), make_value("a"));
		pending.insert(make_key("mango"), make_value("m"));

		let keys: Vec<_> = pending.iter_sorted().map(|(k, _)| k.clone()).collect();

		// BTreeMap should return in sorted order
		assert_eq!(keys, vec![make_key("apple"), make_key("mango"), make_key("zebra")]);
	}

	#[test]
	fn test_iter_insertion_order() {
		let mut pending = PendingWrites::new();

		// Insert in specific order
		pending.insert(make_key("zebra"), make_value("z"));
		pending.insert(make_key("apple"), make_value("a"));
		pending.insert(make_key("mango"), make_value("m"));

		let keys: Vec<_> = pending.iter_insertion_order().map(|(k, _)| k.clone()).collect();

		// Should preserve insertion order
		assert_eq!(keys, vec![make_key("zebra"), make_key("apple"), make_key("mango")]);
	}

	#[test]
	fn test_insertion_order_preserved_on_update() {
		let mut pending = PendingWrites::new();

		pending.insert(make_key("first"), make_value("1"));
		pending.insert(make_key("second"), make_value("2"));
		pending.insert(make_key("first"), make_value("updated")); // Update first

		let keys: Vec<_> = pending.iter_insertion_order().map(|(k, _)| k.clone()).collect();

		// "first" should still be first in insertion order
		assert_eq!(keys, vec![make_key("first"), make_key("second")]);
	}

	#[test]
	fn test_range_query() {
		let mut pending = PendingWrites::new();

		pending.insert(make_key("a"), make_value("1"));
		pending.insert(make_key("b"), make_value("2"));
		pending.insert(make_key("c"), make_value("3"));
		pending.insert(make_key("d"), make_value("4"));

		let range_keys: Vec<_> = pending.range(make_key("b")..make_key("d")).map(|(k, _)| k.clone()).collect();

		assert_eq!(range_keys, vec![make_key("b"), make_key("c")]);
	}

	#[test]
	fn test_range_query_inclusive() {
		let mut pending = PendingWrites::new();

		pending.insert(make_key("a"), make_value("1"));
		pending.insert(make_key("b"), make_value("2"));
		pending.insert(make_key("c"), make_value("3"));

		let range_keys: Vec<_> = pending.range(make_key("a")..=make_key("c")).map(|(k, _)| k.clone()).collect();

		assert_eq!(range_keys, vec![make_key("a"), make_key("b"), make_key("c")]);
	}

	#[test]
	fn test_range_query_empty() {
		let mut pending = PendingWrites::new();

		pending.insert(make_key("a"), make_value("1"));
		pending.insert(make_key("z"), make_value("2"));

		let range_keys: Vec<_> = pending.range(make_key("m")..make_key("n")).map(|(k, _)| k.clone()).collect();

		assert!(range_keys.is_empty());
	}

	#[test]
	fn test_contains_key() {
		let mut pending = PendingWrites::new();

		pending.insert(make_key("key1"), make_value("value1"));
		pending.remove(make_key("key2"));

		assert!(pending.contains_key(&make_key("key1")));
		assert!(pending.contains_key(&make_key("key2"))); // Remove is also "contained"
		assert!(!pending.contains_key(&make_key("key3")));
	}

	#[test]
	fn test_clear() {
		let mut pending = PendingWrites::new();

		pending.insert(make_key("key1"), make_value("value1"));
		pending.insert(make_key("key2"), make_value("value2"));
		pending.remove(make_key("key3"));

		assert!(!pending.is_empty());

		pending.clear();

		assert!(pending.is_empty());
		assert_eq!(pending.len(), 0);
		assert_eq!(pending.estimated_size(), 0);
		assert_eq!(pending.iter_sorted().count(), 0);
		assert_eq!(pending.iter_insertion_order().count(), 0);
	}

	#[test]
	fn test_get_nonexistent_key() {
		let pending = PendingWrites::new();
		assert_eq!(pending.get(&make_key("missing")), None);
	}

	#[test]
	fn test_is_removed_nonexistent_key() {
		let pending = PendingWrites::new();
		assert!(!pending.is_removed(&make_key("missing")));
	}

	#[test]
	fn test_mixed_writes_and_removes() {
		let mut pending = PendingWrites::new();

		pending.insert(make_key("write1"), make_value("v1"));
		pending.remove(make_key("remove1"));
		pending.insert(make_key("write2"), make_value("v2"));
		pending.remove(make_key("remove2"));

		assert_eq!(pending.len(), 4);
		assert_eq!(pending.get(&make_key("write1")), Some(&make_value("v1")));
		assert_eq!(pending.get(&make_key("write2")), Some(&make_value("v2")));
		assert!(pending.is_removed(&make_key("remove1")));
		assert!(pending.is_removed(&make_key("remove2")));
		assert_eq!(pending.get(&make_key("remove1")), None);
		assert_eq!(pending.get(&make_key("remove2")), None);
	}

	#[test]
	fn test_iter_sorted_includes_removes() {
		let mut pending = PendingWrites::new();

		pending.insert(make_key("b"), make_value("2"));
		pending.remove(make_key("a"));
		pending.insert(make_key("c"), make_value("3"));

		let items: Vec<_> = pending.iter_sorted().collect();
		assert_eq!(items.len(), 3);

		// Check order
		assert_eq!(items[0].0, &make_key("a"));
		assert!(matches!(items[0].1, Pending::Remove));

		assert_eq!(items[1].0, &make_key("b"));
		assert!(matches!(items[1].1, Pending::Set(_)));

		assert_eq!(items[2].0, &make_key("c"));
		assert!(matches!(items[2].1, Pending::Set(_)));
	}

	#[test]
	fn test_default_trait() {
		let pending = PendingWrites::default();
		assert!(pending.is_empty());
		assert_eq!(pending.len(), 0);
		assert_eq!(pending.estimated_size(), 0);
	}
}
