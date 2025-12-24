// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound::{Excluded, Included, Unbounded};

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{Key, MultiVersionBatch, QueryTransaction},
	key::KeyKind,
	value::encoded::EncodedValues,
};

use super::{FlowTransaction, iter_range::collect_batch};

impl FlowTransaction {
	/// Get a value by key, checking pending writes first, then querying multi-version store
	pub async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<EncodedValues>> {
		self.metrics.increment_reads();

		if self.pending.is_removed(key) {
			return Ok(None);
		}

		if let Some(value) = self.pending.get(key) {
			return Ok(Some(value.clone()));
		}

		let query = if Self::is_flow_state_key(key) {
			&mut self.state_query
		} else {
			&mut self.source_query
		};

		match query.get(key).await? {
			Some(multi) => Ok(Some(multi.values)),
			None => Ok(None),
		}
	}

	/// Check if a key exists
	pub async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.metrics.increment_reads();

		if self.pending.is_removed(key) {
			return Ok(false);
		}

		if self.pending.get(key).is_some() {
			return Ok(true);
		}

		let query = if Self::is_flow_state_key(key) {
			&mut self.state_query
		} else {
			&mut self.source_query
		};

		query.contains_key(key).await
	}

	/// Range query
	pub async fn range(&mut self, range: EncodedKeyRange) -> crate::Result<MultiVersionBatch> {
		self.metrics.increment_reads();

		let pending = self.pending.range((range.start.as_ref(), range.end.as_ref()));

		let query = match range.start.as_ref() {
			Included(start) | Excluded(start) => {
				if Self::is_flow_state_key(start) {
					&mut self.state_query
				} else {
					&mut self.source_query
				}
			}
			Unbounded => &mut self.source_query,
		};
		let committed_batch = query.range_batch(range, 1024).await?;

		Ok(collect_batch(pending, committed_batch, self.version))
	}

	/// Range query with batching
	pub async fn range_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		self.metrics.increment_reads();

		let pending = self.pending.range((range.start.as_ref(), range.end.as_ref()));

		let query = match range.start.as_ref() {
			Included(start) | Excluded(start) => {
				if Self::is_flow_state_key(start) {
					&mut self.state_query
				} else {
					&mut self.source_query
				}
			}
			Unbounded => &mut self.source_query,
		};
		let committed_batch = query.range_batch(range, batch_size).await?;

		Ok(collect_batch(pending, committed_batch, self.version))
	}

	/// Prefix scan
	pub async fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<MultiVersionBatch> {
		self.metrics.increment_reads();

		let range = EncodedKeyRange::prefix(prefix);
		let pending = self.pending.range((range.start.as_ref(), range.end.as_ref()));

		let query = if Self::is_flow_state_key(prefix) {
			&mut self.state_query
		} else {
			&mut self.source_query
		};
		let committed_batch = query.prefix(prefix).await?;

		Ok(collect_batch(pending, committed_batch, self.version))
	}

	fn is_flow_state_key(key: &EncodedKey) -> bool {
		match Key::kind(&key) {
			None => false,
			Some(kind) => match kind {
				KeyKind::FlowNodeState => true,
				KeyKind::FlowNodeInternalState => true,
				_ => false,
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		CommitVersion, CowVec, EncodedKey, EncodedKeyRange,
		interface::{CommandTransaction, Engine, QueryTransaction},
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

	#[tokio::test]
	async fn test_get_from_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		let key = make_key("key1");
		let value = make_value("value1");

		txn.set(&key, value.clone()).unwrap();

		// Should get value from pending buffer
		let result = txn.get(&key).await.unwrap();
		assert_eq!(result, Some(value));
	}

	#[tokio::test]
	async fn test_get_from_committed() {
		use crate::operator::stateful::test_utils::test::create_test_engine;
		let engine = create_test_engine().await;

		let key = make_key("key1");
		let value = make_value("value1");

		// Set value in first transaction and commit
		{
			let mut cmd_txn = engine.begin_command().await.unwrap();
			cmd_txn.set(&key, value.clone()).await.unwrap();
			cmd_txn.commit().await.unwrap();
		}

		// Create new command transaction to read committed data
		let parent = engine.begin_command().await.unwrap();
		let version = parent.version();

		// Create FlowTransaction - should see committed value
		let mut txn = FlowTransaction::new(&parent, version).await;

		// Should get value from query transaction
		let result = txn.get(&key).await.unwrap();
		assert_eq!(result, Some(value));
	}

	#[tokio::test]
	async fn test_get_pending_shadows_committed() {
		let mut parent = create_test_transaction().await;

		let key = make_key("key1");
		parent.set(&key, make_value("old")).await.unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::new(&parent, version).await;

		// Override with new value in pending
		let new_value = make_value("new");
		txn.set(&key, new_value.clone()).unwrap();

		// Should get new value from pending, not old value from committed
		let result = txn.get(&key).await.unwrap();
		assert_eq!(result, Some(new_value));
	}

	#[tokio::test]
	async fn test_get_removed_returns_none() {
		let mut parent = create_test_transaction().await;

		let key = make_key("key1");
		parent.set(&key, make_value("value1")).await.unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::new(&parent, version).await;

		// Remove in pending
		txn.remove(&key).unwrap();

		// Should return None even though it exists in committed
		let result = txn.get(&key).await.unwrap();
		assert_eq!(result, None);
	}

	#[tokio::test]
	async fn test_get_nonexistent_key() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		let result = txn.get(&make_key("missing")).await.unwrap();
		assert_eq!(result, None);
	}

	#[tokio::test]
	async fn test_get_increments_reads_metric() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		assert_eq!(txn.metrics().reads, 0);

		txn.get(&make_key("key1")).await.unwrap();
		assert_eq!(txn.metrics().reads, 1);

		txn.get(&make_key("key2")).await.unwrap();
		assert_eq!(txn.metrics().reads, 2);
	}

	#[tokio::test]
	async fn test_contains_key_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		let key = make_key("key1");
		txn.set(&key, make_value("value1")).unwrap();

		assert!(txn.contains_key(&key).await.unwrap());
	}

	#[tokio::test]
	async fn test_contains_key_committed() {
		use crate::operator::stateful::test_utils::test::create_test_engine;
		let engine = create_test_engine().await;

		let key = make_key("key1");

		// Set value in first transaction and commit
		{
			let mut cmd_txn = engine.begin_command().await.unwrap();
			cmd_txn.set(&key, make_value("value1")).await.unwrap();
			cmd_txn.commit().await.unwrap();
		}

		// Create new command transaction
		let parent = engine.begin_command().await.unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::new(&parent, version).await;

		assert!(txn.contains_key(&key).await.unwrap());
	}

	#[tokio::test]
	async fn test_contains_key_removed_returns_false() {
		let mut parent = create_test_transaction().await;

		let key = make_key("key1");
		parent.set(&key, make_value("value1")).await.unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::new(&parent, version).await;
		txn.remove(&key).unwrap();

		assert!(!txn.contains_key(&key).await.unwrap());
	}

	#[tokio::test]
	async fn test_contains_key_nonexistent() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		assert!(!txn.contains_key(&make_key("missing")).await.unwrap());
	}

	#[tokio::test]
	async fn test_contains_key_increments_reads_metric() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		assert_eq!(txn.metrics().reads, 0);

		txn.contains_key(&make_key("key1")).await.unwrap();
		assert_eq!(txn.metrics().reads, 1);

		txn.contains_key(&make_key("key2")).await.unwrap();
		assert_eq!(txn.metrics().reads, 2);
	}

	#[tokio::test]
	async fn test_scan_empty() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		let iter = txn.range(EncodedKeyRange::all()).await.unwrap();
		assert!(iter.items.into_iter().next().is_none());
	}

	#[tokio::test]
	async fn test_scan_only_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let iter = txn.range(EncodedKeyRange::all()).await.unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		// Should be in sorted order
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_scan_filters_removes() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.remove(&make_key("b")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let iter = txn.range(EncodedKeyRange::all()).await.unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		// Should only have 2 items (remove filtered out)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_scan_increments_reads_metric() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		assert_eq!(txn.metrics().reads, 0);
		let _ = txn.range(EncodedKeyRange::all()).await.unwrap();
		assert_eq!(txn.metrics().reads, 1);
	}

	#[tokio::test]
	async fn test_range_empty() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let iter = txn.range(range).await.unwrap();
		assert!(iter.items.into_iter().next().is_none());
	}

	#[tokio::test]
	async fn test_range_only_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();
		txn.set(&make_key("d"), make_value("4")).unwrap();

		let range = EncodedKeyRange::new(Included(make_key("b")), Excluded(make_key("d")));
		let iter = txn.range(range).await.unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		// Should only include b and c (not d, exclusive end)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_range_increments_reads_metric() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		assert_eq!(txn.metrics().reads, 0);

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let _ = txn.range(range).await.unwrap();

		assert_eq!(txn.metrics().reads, 1);
	}

	#[tokio::test]
	async fn test_range_batched_increments_reads_metric() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		assert_eq!(txn.metrics().reads, 0);

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let _ = txn.range_batched(range, 10).await.unwrap();

		assert_eq!(txn.metrics().reads, 1);
	}

	#[tokio::test]
	async fn test_prefix_empty() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		let prefix = make_key("test_");
		let iter = txn.prefix(&prefix).await.unwrap();
		assert!(iter.items.into_iter().next().is_none());
	}

	#[tokio::test]
	async fn test_prefix_only_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		txn.set(&make_key("test_a"), make_value("1")).unwrap();
		txn.set(&make_key("test_b"), make_value("2")).unwrap();
		txn.set(&make_key("other_c"), make_value("3")).unwrap();

		let prefix = make_key("test_");
		let iter = txn.prefix(&prefix).await.unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		// Should only include keys with prefix "test_"
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("test_a"));
		assert_eq!(items[1].key, make_key("test_b"));
	}

	#[tokio::test]
	async fn test_prefix_increments_reads_metric() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		assert_eq!(txn.metrics().reads, 0);

		let prefix = make_key("test_");
		let _ = txn.prefix(&prefix).await.unwrap();

		assert_eq!(txn.metrics().reads, 1);
	}

	#[tokio::test]
	async fn test_multiple_read_operations_accumulate_metrics() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		txn.get(&make_key("k1")).await.unwrap();
		txn.contains_key(&make_key("k2")).await.unwrap();
		let _ = txn.range(EncodedKeyRange::all()).await.unwrap();
		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let _ = txn.range(range).await.unwrap();

		assert_eq!(txn.metrics().reads, 4);
	}
}
