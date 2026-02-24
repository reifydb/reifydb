// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{
		BTreeMap,
		btree_map::{Iter, Range},
	},
	ops::RangeBounds,
};

use reifydb_core::{
	encoded::{encoded::EncodedValues, key::EncodedKey},
	interface::change::Change,
};

/// Represents a pending operation on a key
#[derive(Debug, Clone)]
pub enum PendingWrite {
	Set(EncodedValues),
	Remove,
}

/// Newtype wrapping `Vec<Change>` for view changes generated during flow processing.
#[derive(Debug, Default, Clone)]
pub struct ViewChanges(Vec<Change>);

impl ViewChanges {
	pub fn new() -> Self {
		Self(Vec::new())
	}

	pub fn push(&mut self, change: Change) {
		self.0.push(change);
	}

	pub fn extend(&mut self, iter: impl IntoIterator<Item = Change>) {
		self.0.extend(iter);
	}

	pub fn drain(&mut self) -> std::vec::Drain<'_, Change> {
		self.0.drain(..)
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn iter(&self) -> std::slice::Iter<'_, Change> {
		self.0.iter()
	}
}

impl IntoIterator for ViewChanges {
	type Item = Change;
	type IntoIter = std::vec::IntoIter<Change>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

/// Manages pending writes and removes with sorted key access
#[derive(Debug, Default, Clone)]
pub struct Pending {
	/// Primary storage - BTreeMap for sorted key access and range queries
	writes: BTreeMap<EncodedKey, PendingWrite>,
	/// View changes generated during flow processing
	view_changes: ViewChanges,
}

impl Pending {
	/// Create a new empty pending writes manager
	pub fn new() -> Self {
		Self {
			writes: BTreeMap::new(),
			view_changes: ViewChanges::new(),
		}
	}

	/// Insert a write operation
	pub fn insert(&mut self, key: EncodedKey, value: EncodedValues) {
		self.writes.insert(key, PendingWrite::Set(value));
	}

	/// Insert a remove operation
	pub fn remove(&mut self, key: EncodedKey) {
		self.writes.insert(key, PendingWrite::Remove);
	}

	/// Get a value if it exists and is a write (not a remove)
	pub fn get(&self, key: &EncodedKey) -> Option<&EncodedValues> {
		match self.writes.get(key) {
			Some(PendingWrite::Set(value)) => Some(value),
			_ => None,
		}
	}

	/// Check if a key is marked for removal
	pub fn is_removed(&self, key: &EncodedKey) -> bool {
		matches!(self.writes.get(key), Some(PendingWrite::Remove))
	}

	/// Check if a key exists (either as write or remove)
	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.writes.contains_key(key)
	}

	/// Iterate over all pending operations in sorted key order
	pub fn iter_sorted(&self) -> Iter<'_, EncodedKey, PendingWrite> {
		self.writes.iter()
	}

	/// Range query over pending operations in sorted key order
	pub fn range<R>(&self, range: R) -> Range<'_, EncodedKey, PendingWrite>
	where
		R: RangeBounds<EncodedKey>,
	{
		self.writes.range(range)
	}

	/// Take all view changes, leaving an empty collection
	pub fn take_view_changes(&mut self) -> ViewChanges {
		std::mem::take(&mut self.view_changes)
	}

	/// Append a view change
	pub fn push_view_change(&mut self, change: Change) {
		self.view_changes.push(change);
	}

	/// Extend view changes from another source
	pub fn extend_view_changes(&mut self, changes: impl IntoIterator<Item = Change>) {
		self.view_changes.extend(changes);
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::encoded::{encoded::EncodedValues, key::EncodedKey};
	use reifydb_type::util::cowvec::CowVec;

	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
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

		// Insert in non-sorted order
		pending.insert(make_key("zebra"), make_value("z"));
		pending.insert(make_key("apple"), make_value("a"));
		pending.insert(make_key("mango"), make_value("m"));

		let keys: Vec<_> = pending.iter_sorted().map(|(k, _)| k.clone()).collect();

		// BTreeMap should return in sorted order
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
	fn test_iter_sorted_includes_removes() {
		let mut pending = Pending::new();

		pending.insert(make_key("b"), make_value("2"));
		pending.remove(make_key("a"));
		pending.insert(make_key("c"), make_value("3"));

		let items: Vec<_> = pending.iter_sorted().collect();
		assert_eq!(items.len(), 3);

		// Check order
		assert_eq!(items[0].0, &make_key("a"));
		assert!(matches!(items[0].1, PendingWrite::Remove));

		assert_eq!(items[1].0, &make_key("b"));
		assert!(matches!(items[1].1, PendingWrite::Set(_)));

		assert_eq!(items[2].0, &make_key("c"));
		assert!(matches!(items[2].1, PendingWrite::Set(_)));
	}
}
