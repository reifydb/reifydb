// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	ops::Bound::{Excluded, Included, Unbounded},
	pin::Pin,
};

use async_stream::try_stream;
use futures_util::{Stream, StreamExt, TryStreamExt};
use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{Key, MultiVersionValues},
	key::KeyKind,
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::MultiVersionBatch;

use super::{FlowTransaction, Pending};

impl FlowTransaction {
	/// Get a value by key, checking pending writes first, then querying multi-version store
	pub async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<EncodedValues>> {
		if self.pending.is_removed(key) {
			return Ok(None);
		}

		if let Some(value) = self.pending.get(key) {
			return Ok(Some(value.clone()));
		}

		let query = if Self::is_flow_state_key(key) {
			&mut self.state_query
		} else {
			&mut self.primitive_query
		};

		match query.get(key).await? {
			Some(multi) => Ok(Some(multi.values().clone())),
			None => Ok(None),
		}
	}

	/// Check if a key exists
	pub async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		if self.pending.is_removed(key) {
			return Ok(false);
		}

		if self.pending.get(key).is_some() {
			return Ok(true);
		}

		let query = if Self::is_flow_state_key(key) {
			&mut self.state_query
		} else {
			&mut self.primitive_query
		};

		query.contains_key(key).await
	}

	/// Prefix scan
	pub async fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<MultiVersionBatch> {
		let range = EncodedKeyRange::prefix(prefix);
		let items: Vec<_> = self.range(range, 1024).try_collect().await?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
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

	/// Create a streaming iterator for forward range queries.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The stream yields individual entries
	/// and maintains cursor state internally. Pending writes are merged with
	/// committed storage data.
	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Pin<Box<dyn Stream<Item = crate::Result<MultiVersionValues>> + Send + '_>> {
		// Collect pending writes in range as owned data
		let pending: Vec<(EncodedKey, Pending)> = self
			.pending
			.range((range.start.as_ref(), range.end.as_ref()))
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect();

		let query = match range.start.as_ref() {
			Included(start) | Excluded(start) => {
				if Self::is_flow_state_key(start) {
					&self.state_query
				} else {
					&self.primitive_query
				}
			}
			Unbounded => &self.primitive_query,
		};

		let storage_stream = query.range(range, batch_size);
		let version = self.version;

		Box::pin(flow_merge_pending_with_stream(pending, storage_stream, version))
	}

	/// Create a streaming iterator for reverse range queries.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The stream yields individual entries
	/// in reverse key order and maintains cursor state internally.
	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Pin<Box<dyn Stream<Item = crate::Result<MultiVersionValues>> + Send + '_>> {
		// Collect pending writes in range as owned data (reversed)
		let pending: Vec<(EncodedKey, Pending)> = self
			.pending
			.range((range.start.as_ref(), range.end.as_ref()))
			.rev()
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect();

		let query = match range.start.as_ref() {
			Included(start) | Excluded(start) => {
				if Self::is_flow_state_key(start) {
					&self.state_query
				} else {
					&self.primitive_query
				}
			}
			Unbounded => &self.primitive_query,
		};

		let storage_stream = query.range_rev(range, batch_size);
		let version = self.version;

		Box::pin(flow_merge_pending_with_stream_rev(pending, storage_stream, version))
	}
}

/// Merge pending writes with a storage stream for FlowTransaction (forward order).
fn flow_merge_pending_with_stream<'a, S>(
	pending: Vec<(EncodedKey, Pending)>,
	storage_stream: S,
	version: reifydb_core::CommitVersion,
) -> impl Stream<Item = crate::Result<MultiVersionValues>> + Send + 'a
where
	S: Stream<Item = reifydb_type::Result<MultiVersionValues>> + Send + 'a,
{
	try_stream! {
		use std::cmp::Ordering;

		let mut storage_stream = std::pin::pin!(storage_stream);
		let mut pending_iter = pending.into_iter().peekable();
		let mut next_storage: Option<MultiVersionValues> = None;

		loop {
			// Fetch next storage item if needed
			if next_storage.is_none() {
				next_storage = match storage_stream.next().await {
					Some(Ok(v)) => Some(v),
					Some(Err(e)) => { Err(e)?; None }
					None => None,
				};
			}

			match (pending_iter.peek(), &next_storage) {
				(Some((pending_key, _)), Some(storage_val)) => {
					let cmp = pending_key.cmp(&storage_val.key);

					if matches!(cmp, Ordering::Less) {
						// Pending key comes first
						let (key, value) = pending_iter.next().unwrap();
						if let Pending::Set(values) = value {
							yield MultiVersionValues {
								key,
								values,
								version,
							};
						}
						// Pending::Remove = skip (tombstone)
					} else if matches!(cmp, Ordering::Equal) {
						// Same key - pending shadows storage
						let (key, value) = pending_iter.next().unwrap();
						next_storage = None; // Consume storage entry
						if let Pending::Set(values) = value {
							yield MultiVersionValues {
								key,
								values,
								version,
							};
						}
					} else {
						// Storage key comes first
						yield next_storage.take().unwrap();
					}
				}
				(Some(_), None) => {
					// Only pending left
					let (key, value) = pending_iter.next().unwrap();
					if let Pending::Set(values) = value {
						yield MultiVersionValues {
							key,
							values,
							version,
						};
					}
				}
				(None, Some(_)) => {
					// Only storage left
					yield next_storage.take().unwrap();
				}
				(None, None) => break,
			}
		}
	}
}

/// Merge pending writes with a storage stream for FlowTransaction (reverse order).
fn flow_merge_pending_with_stream_rev<'a, S>(
	pending: Vec<(EncodedKey, Pending)>,
	storage_stream: S,
	version: reifydb_core::CommitVersion,
) -> impl Stream<Item = crate::Result<MultiVersionValues>> + Send + 'a
where
	S: Stream<Item = reifydb_type::Result<MultiVersionValues>> + Send + 'a,
{
	try_stream! {
		use std::cmp::Ordering;

		let mut storage_stream = std::pin::pin!(storage_stream);
		let mut pending_iter = pending.into_iter().peekable();
		let mut next_storage: Option<MultiVersionValues> = None;

		loop {
			// Fetch next storage item if needed
			if next_storage.is_none() {
				next_storage = match storage_stream.next().await {
					Some(Ok(v)) => Some(v),
					Some(Err(e)) => { Err(e)?; None }
					None => None,
				};
			}

			match (pending_iter.peek(), &next_storage) {
				(Some((pending_key, _)), Some(storage_val)) => {
					let cmp = pending_key.cmp(&storage_val.key);

					if matches!(cmp, Ordering::Greater) {
						// Reverse: Pending key is larger (comes first in reverse)
						let (key, value) = pending_iter.next().unwrap();
						if let Pending::Set(values) = value {
							yield MultiVersionValues {
								key,
								values,
								version,
							};
						}
					} else if matches!(cmp, Ordering::Equal) {
						// Same key - pending shadows storage
						let (key, value) = pending_iter.next().unwrap();
						next_storage = None; // Consume storage entry
						if let Pending::Set(values) = value {
							yield MultiVersionValues {
								key,
								values,
								version,
							};
						}
					} else {
						// Storage key comes first in reverse order
						yield next_storage.take().unwrap();
					}
				}
				(Some(_), None) => {
					// Only pending left
					let (key, value) = pending_iter.next().unwrap();
					if let Pending::Set(values) = value {
						yield MultiVersionValues {
							key,
							values,
							version,
						};
					}
				}
				(None, Some(_)) => {
					// Only storage left
					yield next_storage.take().unwrap();
				}
				(None, None) => break,
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::Catalog;
	use reifydb_core::{CommitVersion, CowVec, EncodedKey, EncodedKeyRange, value::encoded::EncodedValues};

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
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

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
		let mut txn = FlowTransaction::new(&parent, version, Catalog::default()).await;

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

		let mut txn = FlowTransaction::new(&parent, version, Catalog::default()).await;

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

		let mut txn = FlowTransaction::new(&parent, version, Catalog::default()).await;

		// Remove in pending
		txn.remove(&key).unwrap();

		// Should return None even though it exists in committed
		let result = txn.get(&key).await.unwrap();
		assert_eq!(result, None);
	}

	#[tokio::test]
	async fn test_get_nonexistent_key() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

		let result = txn.get(&make_key("missing")).await.unwrap();
		assert_eq!(result, None);
	}

	#[tokio::test]
	async fn test_contains_key_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

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
		let mut txn = FlowTransaction::new(&parent, version, Catalog::default()).await;

		assert!(txn.contains_key(&key).await.unwrap());
	}

	#[tokio::test]
	async fn test_contains_key_removed_returns_false() {
		let mut parent = create_test_transaction().await;

		let key = make_key("key1");
		parent.set(&key, make_value("value1")).await.unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::new(&parent, version, Catalog::default()).await;
		txn.remove(&key).unwrap();

		assert!(!txn.contains_key(&key).await.unwrap());
	}

	#[tokio::test]
	async fn test_contains_key_nonexistent() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

		assert!(!txn.contains_key(&make_key("missing")).await.unwrap());
	}

	#[tokio::test]
	async fn test_scan_empty() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

		let mut stream = txn.range(EncodedKeyRange::all(), 1024);
		assert!(stream.next().await.is_none());
	}

	#[tokio::test]
	async fn test_scan_only_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let mut stream = txn.range(EncodedKeyRange::all(), 1024);
		let mut items = Vec::new();
		while let Some(result) = stream.next().await {
			items.push(result.unwrap());
		}

		// Should be in sorted order
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_scan_filters_removes() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.remove(&make_key("b")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let mut stream = txn.range(EncodedKeyRange::all(), 1024);
		let mut items = Vec::new();
		while let Some(result) = stream.next().await {
			items.push(result.unwrap());
		}

		// Should only have 2 items (remove filtered out)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_range_empty() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let mut stream = txn.range(range, 1024);
		assert!(stream.next().await.is_none());
	}

	#[tokio::test]
	async fn test_range_only_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();
		txn.set(&make_key("d"), make_value("4")).unwrap();

		let range = EncodedKeyRange::new(Included(make_key("b")), Excluded(make_key("d")));
		let mut stream = txn.range(range, 1024);
		let mut items = Vec::new();
		while let Some(result) = stream.next().await {
			items.push(result.unwrap());
		}

		// Should only include b and c (not d, exclusive end)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_prefix_empty() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

		let prefix = make_key("test_");
		let iter = txn.prefix(&prefix).await.unwrap();
		assert!(iter.items.into_iter().next().is_none());
	}

	#[tokio::test]
	async fn test_prefix_only_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::default()).await;

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
}
