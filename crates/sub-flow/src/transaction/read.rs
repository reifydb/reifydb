// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	cmp::Ordering,
	ops::Bound::{Excluded, Included, Unbounded},
};

use reifydb_core::{
	common::CommitVersion,
	encoded::{
		encoded::EncodedValues,
		key::{EncodedKey, EncodedKeyRange},
	},
	interface::store::{MultiVersionBatch, MultiVersionValues},
	key::{Key, kind::KeyKind},
};

use super::{FlowTransaction, Pending};

impl FlowTransaction {
	/// Get a value by key, checking pending writes first, then querying multi-version store
	pub fn get(&mut self, key: &EncodedKey) -> reifydb_type::Result<Option<EncodedValues>> {
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

		match query.get(key)? {
			Some(multi) => Ok(Some(multi.values().clone())),
			None => Ok(None),
		}
	}

	/// Check if a key exists
	pub fn contains_key(&mut self, key: &EncodedKey) -> reifydb_type::Result<bool> {
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

		query.contains_key(key)
	}

	/// Prefix scan
	pub fn prefix(&mut self, prefix: &EncodedKey) -> reifydb_type::Result<MultiVersionBatch> {
		let range = EncodedKeyRange::prefix(prefix);
		let items = self.range(range, 1024).collect::<Result<Vec<_>, _>>()?;
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

	/// Create an iterator for forward range queries.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The iterator yields individual entries
	/// and maintains cursor state internally. Pending writes are merged with
	/// committed storage data.
	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = reifydb_type::Result<MultiVersionValues>> + Send + '_> {
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

		let storage_iter = query.range(range, batch_size);
		let version = self.version;

		Box::new(flow_merge_pending_iterator(pending, storage_iter, version))
	}

	/// Create an iterator for reverse range queries.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The iterator yields individual entries
	/// in reverse key order and maintains cursor state internally.
	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = reifydb_type::Result<MultiVersionValues>> + Send + '_> {
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

		let storage_iter = query.range_rev(range, batch_size);
		let version = self.version;

		Box::new(flow_merge_pending_iterator_rev(pending, storage_iter, version))
	}
}

/// Iterator that merges pending writes with storage data (forward order).
struct FlowMergePendingIterator<I>
where
	I: Iterator<Item = reifydb_type::Result<MultiVersionValues>>,
{
	storage_iter: std::iter::Peekable<I>,
	pending_iter: std::iter::Peekable<std::vec::IntoIter<(EncodedKey, Pending)>>,
	version: CommitVersion,
}

impl<I> Iterator for FlowMergePendingIterator<I>
where
	I: Iterator<Item = reifydb_type::Result<MultiVersionValues>>,
{
	type Item = reifydb_type::Result<MultiVersionValues>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let next_storage = self.storage_iter.peek();

			match (self.pending_iter.peek(), next_storage) {
				(Some((pending_key, _)), Some(storage_result)) => {
					let storage_val = match storage_result {
						Ok(v) => v,
						Err(_) => {
							// Consume the error from the iterator and propagate it
							let err = self.storage_iter.next().unwrap();
							return Some(err.map_err(|e| e.into()));
						}
					};
					let cmp = pending_key.cmp(&storage_val.key);

					if matches!(cmp, Ordering::Less) {
						// Pending key comes first
						let (key, value) = self.pending_iter.next().unwrap();
						if let Pending::Set(values) = value {
							return Some(Ok(MultiVersionValues {
								key,
								values,
								version: self.version,
							}));
						}
						// Pending::Remove = skip (tombstone), continue loop
					} else if matches!(cmp, Ordering::Equal) {
						// Same key - pending shadows storage
						let (key, value) = self.pending_iter.next().unwrap();
						self.storage_iter.next(); // Consume storage entry
						if let Pending::Set(values) = value {
							return Some(Ok(MultiVersionValues {
								key,
								values,
								version: self.version,
							}));
						}
						// Pending::Remove = skip (tombstone), continue loop
					} else {
						// Storage key comes first
						return Some(self.storage_iter.next().unwrap().map_err(|e| e.into()));
					}
				}
				(Some(_), None) => {
					// Only pending left
					let (key, value) = self.pending_iter.next().unwrap();
					if let Pending::Set(values) = value {
						return Some(Ok(MultiVersionValues {
							key,
							values,
							version: self.version,
						}));
					}
					// Pending::Remove = skip (tombstone), continue loop
				}
				(None, Some(_)) => {
					// Only storage left
					return Some(self.storage_iter.next().unwrap().map_err(|e| e.into()));
				}
				(None, None) => return None,
			}
		}
	}
}

/// Create an iterator that merges pending writes with storage data (forward order).
fn flow_merge_pending_iterator<I>(
	pending: Vec<(EncodedKey, Pending)>,
	storage_iter: I,
	version: CommitVersion,
) -> FlowMergePendingIterator<I>
where
	I: Iterator<Item = reifydb_type::Result<MultiVersionValues>>,
{
	FlowMergePendingIterator {
		storage_iter: storage_iter.peekable(),
		pending_iter: pending.into_iter().peekable(),
		version,
	}
}

/// Iterator that merges pending writes with storage data (reverse order).
struct FlowMergePendingIteratorRev<I>
where
	I: Iterator<Item = reifydb_type::Result<MultiVersionValues>>,
{
	storage_iter: std::iter::Peekable<I>,
	pending_iter: std::iter::Peekable<std::vec::IntoIter<(EncodedKey, Pending)>>,
	version: CommitVersion,
}

impl<I> Iterator for FlowMergePendingIteratorRev<I>
where
	I: Iterator<Item = reifydb_type::Result<MultiVersionValues>>,
{
	type Item = reifydb_type::Result<MultiVersionValues>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let next_storage = self.storage_iter.peek();

			match (self.pending_iter.peek(), next_storage) {
				(Some((pending_key, _)), Some(storage_result)) => {
					let storage_val = match storage_result {
						Ok(v) => v,
						Err(_) => {
							// Consume the error from the iterator and propagate it
							let err = self.storage_iter.next().unwrap();
							return Some(err.map_err(|e| e.into()));
						}
					};
					let cmp = pending_key.cmp(&storage_val.key);

					if matches!(cmp, Ordering::Greater) {
						// Reverse: Pending key is larger (comes first in reverse)
						let (key, value) = self.pending_iter.next().unwrap();
						if let Pending::Set(values) = value {
							return Some(Ok(MultiVersionValues {
								key,
								values,
								version: self.version,
							}));
						}
						// Pending::Remove = skip (tombstone), continue loop
					} else if matches!(cmp, Ordering::Equal) {
						// Same key - pending shadows storage
						let (key, value) = self.pending_iter.next().unwrap();
						self.storage_iter.next(); // Consume storage entry
						if let Pending::Set(values) = value {
							return Some(Ok(MultiVersionValues {
								key,
								values,
								version: self.version,
							}));
						}
						// Pending::Remove = skip (tombstone), continue loop
					} else {
						// Storage key comes first in reverse order
						return Some(self.storage_iter.next().unwrap().map_err(|e| e.into()));
					}
				}
				(Some(_), None) => {
					// Only pending left
					let (key, value) = self.pending_iter.next().unwrap();
					if let Pending::Set(values) = value {
						return Some(Ok(MultiVersionValues {
							key,
							values,
							version: self.version,
						}));
					}
					// Pending::Remove = skip (tombstone), continue loop
				}
				(None, Some(_)) => {
					// Only storage left
					return Some(self.storage_iter.next().unwrap().map_err(|e| e.into()));
				}
				(None, None) => return None,
			}
		}
	}
}

/// Create an iterator that merges pending writes with storage data (reverse order).
fn flow_merge_pending_iterator_rev<I>(
	pending: Vec<(EncodedKey, Pending)>,
	storage_iter: I,
	version: CommitVersion,
) -> FlowMergePendingIteratorRev<I>
where
	I: Iterator<Item = reifydb_type::Result<MultiVersionValues>>,
{
	FlowMergePendingIteratorRev {
		storage_iter: storage_iter.peekable(),
		pending_iter: pending.into_iter().peekable(),
		version,
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::encoded::{
		encoded::EncodedValues,
		key::{EncodedKey, EncodedKeyRange},
	};
	use reifydb_engine::test_utils::create_test_engine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_type::util::cowvec::CowVec;

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
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		let key = make_key("key1");
		let value = make_value("value1");

		txn.set(&key, value.clone()).unwrap();

		// Should get value from pending buffer
		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(value));
	}

	#[test]
	fn test_get_from_committed() {
		let engine = create_test_engine();

		let key = make_key("key1");
		let value = make_value("value1");

		// Set value in first transaction and commit
		{
			let mut cmd_txn = engine.begin_admin().unwrap();
			cmd_txn.set(&key, value.clone()).unwrap();
			cmd_txn.commit().unwrap();
		}

		// Create new command transaction to read committed data
		let parent = engine.begin_admin().unwrap();
		let version = parent.version();

		// Create FlowTransaction - should see committed value
		let mut txn = FlowTransaction::new(&parent, version, Catalog::testing(), Interceptors::new());

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

		let mut txn = FlowTransaction::new(&parent, version, Catalog::testing(), Interceptors::new());

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

		let mut txn = FlowTransaction::new(&parent, version, Catalog::testing(), Interceptors::new());

		// Remove in pending
		txn.remove(&key).unwrap();

		// Should return None even though it exists in committed
		let result = txn.get(&key).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_get_nonexistent_key() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		let result = txn.get(&make_key("missing")).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_contains_key_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		let key = make_key("key1");
		txn.set(&key, make_value("value1")).unwrap();

		assert!(txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_committed() {
		let engine = create_test_engine();

		let key = make_key("key1");

		// Set value in first transaction and commit
		{
			let mut cmd_txn = engine.begin_admin().unwrap();
			cmd_txn.set(&key, make_value("value1")).unwrap();
			cmd_txn.commit().unwrap();
		}

		// Create new command transaction
		let parent = engine.begin_admin().unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::new(&parent, version, Catalog::testing(), Interceptors::new());

		assert!(txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_removed_returns_false() {
		let mut parent = create_test_transaction();

		let key = make_key("key1");
		parent.set(&key, make_value("value1")).unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::new(&parent, version, Catalog::testing(), Interceptors::new());
		txn.remove(&key).unwrap();

		assert!(!txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_nonexistent() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		assert!(!txn.contains_key(&make_key("missing")).unwrap());
	}

	#[test]
	fn test_scan_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		let mut iter = txn.range(EncodedKeyRange::all(), 1024);
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_scan_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();

		// Should be in sorted order
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
	}

	#[test]
	fn test_scan_filters_removes() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.remove(&make_key("b")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>, _>>().unwrap();

		// Should only have 2 items (remove filtered out)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_range_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let mut iter = txn.range(range, 1024);
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_range_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();
		txn.set(&make_key("d"), make_value("4")).unwrap();

		let range = EncodedKeyRange::new(Included(make_key("b")), Excluded(make_key("d")));
		let items: Vec<_> = txn.range(range, 1024).collect::<Result<Vec<_>, _>>().unwrap();

		// Should only include b and c (not d, exclusive end)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_prefix_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		let prefix = make_key("test_");
		let iter = txn.prefix(&prefix).unwrap();
		assert!(iter.items.into_iter().next().is_none());
	}

	#[test]
	fn test_prefix_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), Catalog::testing(), Interceptors::new());

		txn.set(&make_key("test_a"), make_value("1")).unwrap();
		txn.set(&make_key("test_b"), make_value("2")).unwrap();
		txn.set(&make_key("other_c"), make_value("3")).unwrap();

		let prefix = make_key("test_");
		let iter = txn.prefix(&prefix).unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		// Should only include keys with prefix "test_"
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("test_a"));
		assert_eq!(items[1].key, make_key("test_b"));
	}
}
