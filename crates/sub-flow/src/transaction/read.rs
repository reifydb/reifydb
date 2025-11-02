// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{BoxedMultiVersionIter, MultiVersionQueryTransaction},
	value::encoded::EncodedValues,
};

use super::{FlowTransaction, iter_range::FlowRangeIter, iter_scan::FlowScanIter};

impl FlowTransaction {
	/// Get a value by key, checking pending writes first, then querying multi-version store
	pub fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<EncodedValues>> {
		self.metrics.increment_reads();

		// Check if key is marked for removal
		if self.pending.is_removed(key) {
			return Ok(None);
		}

		// Check pending writes
		if let Some(value) = self.pending.get(key) {
			return Ok(Some(value.clone()));
		}

		match self.query.get(key)? {
			Some(multi) => Ok(Some(multi.values)),
			None => Ok(None),
		}
	}

	/// Check if a key exists
	pub fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.metrics.increment_reads();

		// Check if key is marked for removal
		if self.pending.is_removed(key) {
			return Ok(false);
		}

		// Check pending writes
		if self.pending.get(key).is_some() {
			return Ok(true);
		}

		self.query.contains_key(key)
	}

	/// Scan all keys in the transaction
	pub fn scan(&mut self) -> crate::Result<BoxedMultiVersionIter> {
		self.metrics.increment_reads();

		// Merge pending writes with committed results
		let pending = self.pending.iter_sorted();
		let committed = self.query.scan()?;

		Ok(Box::new(FlowScanIter::new(pending, committed, self.version)))
	}

	/// Range query
	pub fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedMultiVersionIter> {
		self.metrics.increment_reads();

		// Merge pending writes with committed results for the range
		let pending = self.pending.range((range.start.as_ref(), range.end.as_ref()));
		let committed = self.query.range(range)?;

		Ok(Box::new(FlowRangeIter::new(pending, committed, self.version)))
	}

	/// Range query with batching
	pub fn range_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<BoxedMultiVersionIter> {
		self.metrics.increment_reads();

		// Merge pending writes with committed results for the range
		let pending = self.pending.range((range.start.as_ref(), range.end.as_ref()));
		let committed = self.query.range_batched(range, batch_size)?;

		Ok(Box::new(FlowRangeIter::new(pending, committed, self.version)))
	}

	/// Prefix scan
	pub fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter> {
		self.metrics.increment_reads();

		// Merge pending writes with committed results for the prefix
		let range = EncodedKeyRange::prefix(prefix);
		let pending = self.pending.range((range.start.as_ref(), range.end.as_ref()));
		let committed = self.query.prefix(prefix)?;

		Ok(Box::new(FlowRangeIter::new(pending, committed, self.version)))
	}
}

#[cfg(test)]
mod tests {
	use std::collections::Bound;

	use reifydb_core::{
		CommitVersion, CowVec, EncodedKey, EncodedKeyRange,
		interface::{Engine, MultiVersionCommandTransaction, MultiVersionQueryTransaction},
		value::encoded::EncodedValues,
	};

	use super::*;
	use crate::operator::stateful::test_utils::test::create_test_transaction;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn test_get_from_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		let key = make_key("key1");
		let value = make_value("value1");

		txn.set(&key, value.clone()).unwrap();

		// Should get value from pending buffer
		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(value));
	}

	#[test]
	fn test_get_from_committed() {
		use crate::operator::stateful::test_utils::test::create_test_engine;
		let engine = create_test_engine();

		let key = make_key("key1");
		let value = make_value("value1");

		// Set value in first transaction and commit
		{
			let mut cmd_txn = engine.begin_command().unwrap();
			cmd_txn.set(&key, value.clone()).unwrap();
			cmd_txn.commit().unwrap();
		}

		// Create new command transaction to read committed data
		let parent = engine.begin_command().unwrap();
		let version = parent.version();

		// Create FlowTransaction - should see committed value
		let mut txn = FlowTransaction::new(&parent, version);

		// Should get value from query transaction
		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(value));
	}

	#[test]
	fn test_get_pending_shadows_committed() {
		let mut parent = create_test_transaction();

		let key = make_key("key1");
		parent.set(&key, make_value("old")).unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::new(&parent, version);

		// Override with new value in pending
		let new_value = make_value("new");
		txn.set(&key, new_value.clone()).unwrap();

		// Should get new value from pending, not old value from committed
		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(new_value));
	}

	#[test]
	fn test_get_removed_returns_none() {
		let mut parent = create_test_transaction();

		let key = make_key("key1");
		parent.set(&key, make_value("value1")).unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::new(&parent, version);

		// Remove in pending
		txn.remove(&key).unwrap();

		// Should return None even though it exists in committed
		let result = txn.get(&key).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_get_nonexistent_key() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		let result = txn.get(&make_key("missing")).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_get_increments_reads_metric() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		assert_eq!(txn.metrics().reads, 0);

		txn.get(&make_key("key1")).unwrap();
		assert_eq!(txn.metrics().reads, 1);

		txn.get(&make_key("key2")).unwrap();
		assert_eq!(txn.metrics().reads, 2);
	}

	#[test]
	fn test_contains_key_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		let key = make_key("key1");
		txn.set(&key, make_value("value1")).unwrap();

		assert!(txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_committed() {
		use crate::operator::stateful::test_utils::test::create_test_engine;
		let engine = create_test_engine();

		let key = make_key("key1");

		// Set value in first transaction and commit
		{
			let mut cmd_txn = engine.begin_command().unwrap();
			cmd_txn.set(&key, make_value("value1")).unwrap();
			cmd_txn.commit().unwrap();
		}

		// Create new command transaction
		let parent = engine.begin_command().unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::new(&parent, version);

		assert!(txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_removed_returns_false() {
		let mut parent = create_test_transaction();

		let key = make_key("key1");
		parent.set(&key, make_value("value1")).unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::new(&parent, version);
		txn.remove(&key).unwrap();

		assert!(!txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_nonexistent() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		assert!(!txn.contains_key(&make_key("missing")).unwrap());
	}

	#[test]
	fn test_contains_key_increments_reads_metric() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		assert_eq!(txn.metrics().reads, 0);

		txn.contains_key(&make_key("key1")).unwrap();
		assert_eq!(txn.metrics().reads, 1);

		txn.contains_key(&make_key("key2")).unwrap();
		assert_eq!(txn.metrics().reads, 2);
	}

	#[test]
	fn test_scan_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		let mut iter = txn.scan().unwrap();
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_scan_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let mut iter = txn.scan().unwrap();
		let items: Vec<_> = iter.by_ref().collect();

		// Should be in sorted order
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
	}

	#[test]
	fn test_scan_filters_removes() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.remove(&make_key("b")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let mut iter = txn.scan().unwrap();
		let items: Vec<_> = iter.by_ref().collect();

		// Should only have 2 items (remove filtered out)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_scan_increments_reads_metric() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		assert_eq!(txn.metrics().reads, 0);
		let _ = txn.scan().unwrap();
		assert_eq!(txn.metrics().reads, 1);
	}

	#[test]
	fn test_range_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let mut iter = txn.range(range).unwrap();
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_range_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();
		txn.set(&make_key("d"), make_value("4")).unwrap();

		let range = EncodedKeyRange::new(Bound::Included(make_key("b")), Bound::Excluded(make_key("d")));
		let mut iter = txn.range(range).unwrap();
		let items: Vec<_> = iter.by_ref().collect();

		// Should only include b and c (not d, exclusive end)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_range_increments_reads_metric() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		assert_eq!(txn.metrics().reads, 0);

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let _ = txn.range(range).unwrap();

		assert_eq!(txn.metrics().reads, 1);
	}

	#[test]
	fn test_range_batched_increments_reads_metric() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		assert_eq!(txn.metrics().reads, 0);

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let _ = txn.range_batched(range, 10).unwrap();

		assert_eq!(txn.metrics().reads, 1);
	}

	#[test]
	fn test_prefix_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		let prefix = make_key("test_");
		let mut iter = txn.prefix(&prefix).unwrap();
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_prefix_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		txn.set(&make_key("test_a"), make_value("1")).unwrap();
		txn.set(&make_key("test_b"), make_value("2")).unwrap();
		txn.set(&make_key("other_c"), make_value("3")).unwrap();

		let prefix = make_key("test_");
		let mut iter = txn.prefix(&prefix).unwrap();
		let items: Vec<_> = iter.by_ref().collect();

		// Should only include keys with prefix "test_"
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("test_a"));
		assert_eq!(items[1].key, make_key("test_b"));
	}

	#[test]
	fn test_prefix_increments_reads_metric() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		assert_eq!(txn.metrics().reads, 0);

		let prefix = make_key("test_");
		let _ = txn.prefix(&prefix).unwrap();

		assert_eq!(txn.metrics().reads, 1);
	}

	#[test]
	fn test_multiple_read_operations_accumulate_metrics() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1));

		txn.get(&make_key("k1")).unwrap();
		txn.contains_key(&make_key("k2")).unwrap();
		let _ = txn.scan().unwrap();
		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let _ = txn.range(range).unwrap();

		assert_eq!(txn.metrics().reads, 4);
	}
}
