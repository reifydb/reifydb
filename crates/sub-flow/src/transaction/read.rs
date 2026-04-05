// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cmp::Ordering,
	collections, iter,
	ops::{
		Bound::{Excluded, Included, Unbounded},
		RangeBounds,
	},
	vec,
};

use collections::BTreeMap;
use iter::Peekable;
use reifydb_core::{
	common::CommitVersion,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::store::{MultiVersionBatch, MultiVersionRow},
	key::{Key, kind::KeyKind},
};
use reifydb_type::Result;
use vec::IntoIter;

use super::{FlowTransaction, PendingWrite};

/// Determines which query snapshot to read from inside a FlowTransaction.
pub(crate) enum ReadFrom {
	/// Snapshots at the latest committed version. Use for keys whose values
	/// are produced by flow processing and must reflect writes committed by
	/// previous CDC batches (operator state, ringbuffer metadata, series metadata).
	StateQuery,
	/// Snapshots at the CDC event version. Use for keys whose values are the
	/// source data being consumed — the original table rows that triggered
	/// the CDC event, plus catalog/schema metadata the flow reads but never writes.
	Query,
}

impl FlowTransaction {
	/// Get a value by key, checking pending writes first, then (if transactional) base_pending, then querying
	/// multi-version store
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		// 1. Check flow-generated pending writes
		let inner = self.inner();
		if inner.pending.is_removed(key) {
			return Ok(None);
		}
		if let Some(value) = inner.pending.get(key) {
			return Ok(Some(value.clone()));
		}

		// 2. Check transaction's base writes (only for Transactional variant)
		if let Self::Transactional {
			base_pending,
			..
		} = self
		{
			if base_pending.is_removed(key) {
				return Ok(None);
			}
			if let Some(value) = base_pending.get(key) {
				return Ok(Some(value.clone()));
			}
		}

		// 3. Ephemeral: state keys from HashMap, source keys from query
		if let Self::Ephemeral {
			inner,
			state,
		} = self
		{
			return match Self::read_from(key) {
				ReadFrom::StateQuery => Ok(state.get(key).cloned()),
				ReadFrom::Query => match inner.query.get(key)? {
					Some(multi) => Ok(Some(multi.row().clone())),
					None => Ok(None),
				},
			};
		}

		// 4. Fall through to committed storage
		let inner = self.inner_mut();
		let query = match Self::read_from(key) {
			ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
			ReadFrom::Query => &inner.query,
		};
		match query.get(key)? {
			Some(multi) => Ok(Some(multi.row().clone())),
			None => Ok(None),
		}
	}

	/// Check if a key exists
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		let inner = self.inner();
		if inner.pending.is_removed(key) {
			return Ok(false);
		}
		if inner.pending.get(key).is_some() {
			return Ok(true);
		}

		if let Self::Transactional {
			base_pending,
			..
		} = self
		{
			if base_pending.is_removed(key) {
				return Ok(false);
			}
			if base_pending.get(key).is_some() {
				return Ok(true);
			}
		}

		// Ephemeral: state keys from HashMap, source keys from query
		if let Self::Ephemeral {
			inner,
			state,
		} = self
		{
			return match Self::read_from(key) {
				ReadFrom::StateQuery => Ok(state.contains_key(key)),
				ReadFrom::Query => inner.query.contains_key(key),
			};
		}

		let inner = self.inner_mut();
		let query = match Self::read_from(key) {
			ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
			ReadFrom::Query => &inner.query,
		};
		query.contains_key(key)
	}

	/// Prefix scan
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let range = EncodedKeyRange::prefix(prefix);
		let items = self.range(range, 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub(crate) fn read_from(key: &EncodedKey) -> ReadFrom {
		match Key::kind(key) {
			None => ReadFrom::Query,
			Some(kind) => match kind {
				// Flow-produced state — read from state_query so subsequent
				// CDC batches see the prior batch's committed writes.
				KeyKind::FlowNodeState => ReadFrom::StateQuery,
				KeyKind::FlowNodeInternalState => ReadFrom::StateQuery,
				KeyKind::RingBufferMetadata => ReadFrom::StateQuery,
				KeyKind::SeriesMetadata => ReadFrom::StateQuery,

				// Row keys are used for both source tables and flow-produced views.
				// Sink operators write view/ringbuffer rows but never read them
				// back via get() — they only read metadata and operator state.
				// Source table rows must come from query (CDC event version).
				// If a future operator needs to read back its own row output
				// across CDC batches, this will need finer-grained routing
				// based on the ShapeId embedded in the RowKey.
				KeyKind::Row => ReadFrom::Query,

				// Source data & catalog metadata — read from query
				KeyKind::Namespace => ReadFrom::Query,
				KeyKind::Table => ReadFrom::Query,
				KeyKind::NamespaceTable => ReadFrom::Query,
				KeyKind::SystemSequence => ReadFrom::Query,
				KeyKind::Columns => ReadFrom::Query,
				KeyKind::Column => ReadFrom::Query,
				KeyKind::RowSequence => ReadFrom::Query,
				KeyKind::ColumnProperty => ReadFrom::Query,
				KeyKind::SystemVersion => ReadFrom::Query,
				KeyKind::TransactionVersion => ReadFrom::Query,
				KeyKind::Index => ReadFrom::Query,
				KeyKind::IndexEntry => ReadFrom::Query,
				KeyKind::ColumnSequence => ReadFrom::Query,
				KeyKind::CdcConsumer => ReadFrom::Query,
				KeyKind::View => ReadFrom::Query,
				KeyKind::NamespaceView => ReadFrom::Query,
				KeyKind::PrimaryKey => ReadFrom::Query,
				KeyKind::RingBuffer => ReadFrom::Query,
				KeyKind::NamespaceRingBuffer => ReadFrom::Query,
				KeyKind::ShapeRetentionStrategy => ReadFrom::Query,
				KeyKind::OperatorRetentionStrategy => ReadFrom::Query,
				KeyKind::Flow => ReadFrom::Query,
				KeyKind::NamespaceFlow => ReadFrom::Query,
				KeyKind::FlowNode => ReadFrom::Query,
				KeyKind::FlowNodeByFlow => ReadFrom::Query,
				KeyKind::FlowEdge => ReadFrom::Query,
				KeyKind::FlowEdgeByFlow => ReadFrom::Query,
				KeyKind::Dictionary => ReadFrom::Query,
				KeyKind::DictionaryEntry => ReadFrom::Query,
				KeyKind::DictionaryEntryIndex => ReadFrom::Query,
				KeyKind::NamespaceDictionary => ReadFrom::Query,
				KeyKind::DictionarySequence => ReadFrom::Query,
				KeyKind::Metric => ReadFrom::Query,
				KeyKind::FlowVersion => ReadFrom::Query,
				KeyKind::Subscription => ReadFrom::Query,
				KeyKind::SubscriptionRow => ReadFrom::Query,
				KeyKind::SubscriptionColumn => ReadFrom::Query,
				KeyKind::Shape => ReadFrom::Query,
				KeyKind::RowShapeField => ReadFrom::Query,
				KeyKind::SumType => ReadFrom::Query,
				KeyKind::NamespaceSumType => ReadFrom::Query,
				KeyKind::Handler => ReadFrom::Query,
				KeyKind::NamespaceHandler => ReadFrom::Query,
				KeyKind::VariantHandler => ReadFrom::Query,
				KeyKind::Series => ReadFrom::Query,
				KeyKind::NamespaceSeries => ReadFrom::Query,
				KeyKind::Identity => ReadFrom::Query,
				KeyKind::Role => ReadFrom::Query,
				KeyKind::GrantedRole => ReadFrom::Query,
				KeyKind::Policy => ReadFrom::Query,
				KeyKind::PolicyOp => ReadFrom::Query,
				KeyKind::Migration => ReadFrom::Query,
				KeyKind::MigrationEvent => ReadFrom::Query,
				KeyKind::Authentication => ReadFrom::Query,
				KeyKind::ConfigStorage => ReadFrom::Query,
				KeyKind::Token => ReadFrom::Query,
				KeyKind::Source => ReadFrom::Query,
				KeyKind::NamespaceSource => ReadFrom::Query,
				KeyKind::Sink => ReadFrom::Query,
				KeyKind::NamespaceSink => ReadFrom::Query,
				KeyKind::SourceCheckpoint => ReadFrom::Query,
				KeyKind::RowTtl => ReadFrom::Query,
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
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		match self {
			Self::Deferred {
				inner,
				..
			} => {
				let merged: BTreeMap<EncodedKey, PendingWrite> = inner
					.pending
					.range((range.start.as_ref(), range.end.as_ref()))
					.map(|(k, v)| (k.clone(), v.clone()))
					.collect();
				let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().collect();

				let query = match range.start.as_ref() {
					Included(start) | Excluded(start) => match Self::read_from(start) {
						ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
						ReadFrom::Query => &inner.query,
					},
					Unbounded => &inner.query,
				};

				let storage_iter = query.range(range, batch_size);
				let v = inner.version;
				Box::new(flow_merge_pending_iterator(pending_vec, storage_iter, v))
			}
			Self::Transactional {
				inner,
				base_pending,
				..
			} => {
				// Collect base layer entries in range, then let flow pending shadow base for same keys
				let mut merged: BTreeMap<EncodedKey, PendingWrite> = base_pending
					.range((range.start.as_ref(), range.end.as_ref()))
					.map(|(k, v)| (k.clone(), v.clone()))
					.collect();
				for (k, v) in inner.pending.range((range.start.as_ref(), range.end.as_ref())) {
					merged.insert(k.clone(), v.clone());
				}
				let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().collect();

				let query = match range.start.as_ref() {
					Included(start) | Excluded(start) => match Self::read_from(start) {
						ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
						ReadFrom::Query => &inner.query,
					},
					Unbounded => &inner.query,
				};

				let storage_iter = query.range(range, batch_size);
				let v = inner.version;
				Box::new(flow_merge_pending_iterator(pending_vec, storage_iter, v))
			}
			Self::Ephemeral {
				inner,
				state,
			} => {
				// Merge pending writes with ephemeral state or query
				let is_state_range = match range.start.as_ref() {
					Included(start) | Excluded(start) => {
						matches!(Self::read_from(start), ReadFrom::StateQuery)
					}
					Unbounded => false,
				};

				let merged: BTreeMap<EncodedKey, PendingWrite> = inner
					.pending
					.range((range.start.as_ref(), range.end.as_ref()))
					.map(|(k, v)| (k.clone(), v.clone()))
					.collect();
				let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().collect();

				if is_state_range {
					// State keys: build iterator from state HashMap
					let state_items: Vec<Result<MultiVersionRow>> = state
						.iter()
						.filter(|(k, _)| range.contains(k))
						.map(|(k, v)| {
							Ok(MultiVersionRow {
								key: k.clone(),
								row: v.clone(),
								version: inner.version,
							})
						})
						.collect();
					let v = inner.version;
					// Sort state items by key for merge iterator
					let mut sorted_items = state_items;
					sorted_items.sort_by(|a, b| match (a, b) {
						(Ok(a), Ok(b)) => a.key.cmp(&b.key),
						_ => Ordering::Equal,
					});
					Box::new(flow_merge_pending_iterator(pending_vec, sorted_items.into_iter(), v))
				} else {
					// Source data: delegate to query
					let storage_iter = inner.query.range(range, batch_size);
					let v = inner.version;
					Box::new(flow_merge_pending_iterator(pending_vec, storage_iter, v))
				}
			}
		}
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
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		match self {
			Self::Deferred {
				inner,
				..
			} => {
				let merged: BTreeMap<EncodedKey, PendingWrite> = inner
					.pending
					.range((range.start.as_ref(), range.end.as_ref()))
					.map(|(k, v)| (k.clone(), v.clone()))
					.collect();
				let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().rev().collect();

				let query = match range.start.as_ref() {
					Included(start) | Excluded(start) => match Self::read_from(start) {
						ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
						ReadFrom::Query => &inner.query,
					},
					Unbounded => &inner.query,
				};

				let storage_iter = query.range_rev(range, batch_size);
				let v = inner.version;
				Box::new(flow_merge_pending_iterator_rev(pending_vec, storage_iter, v))
			}
			Self::Transactional {
				inner,
				base_pending,
				..
			} => {
				let mut merged: BTreeMap<EncodedKey, PendingWrite> = base_pending
					.range((range.start.as_ref(), range.end.as_ref()))
					.map(|(k, v)| (k.clone(), v.clone()))
					.collect();
				for (k, v) in inner.pending.range((range.start.as_ref(), range.end.as_ref())) {
					merged.insert(k.clone(), v.clone());
				}
				let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().rev().collect();

				let query = match range.start.as_ref() {
					Included(start) | Excluded(start) => match Self::read_from(start) {
						ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
						ReadFrom::Query => &inner.query,
					},
					Unbounded => &inner.query,
				};

				let storage_iter = query.range_rev(range, batch_size);
				let v = inner.version;
				Box::new(flow_merge_pending_iterator_rev(pending_vec, storage_iter, v))
			}
			Self::Ephemeral {
				inner,
				state,
			} => {
				let is_state_range = match range.start.as_ref() {
					Included(start) | Excluded(start) => {
						matches!(Self::read_from(start), ReadFrom::StateQuery)
					}
					Unbounded => false,
				};

				let merged: BTreeMap<EncodedKey, PendingWrite> = inner
					.pending
					.range((range.start.as_ref(), range.end.as_ref()))
					.map(|(k, v)| (k.clone(), v.clone()))
					.collect();
				let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().rev().collect();

				if is_state_range {
					// State keys: build reverse iterator from state HashMap
					let mut state_items: Vec<Result<MultiVersionRow>> = state
						.iter()
						.filter(|(k, _)| range.contains(k))
						.map(|(k, v)| {
							Ok(MultiVersionRow {
								key: k.clone(),
								row: v.clone(),
								version: inner.version,
							})
						})
						.collect();
					let v = inner.version;
					// Sort in reverse order for reverse merge iterator
					state_items.sort_by(|a, b| match (a, b) {
						(Ok(a), Ok(b)) => b.key.cmp(&a.key),
						_ => Ordering::Equal,
					});
					Box::new(flow_merge_pending_iterator_rev(
						pending_vec,
						state_items.into_iter(),
						v,
					))
				} else {
					// Source data: delegate to query
					let storage_iter = inner.query.range_rev(range, batch_size);
					let v = inner.version;
					Box::new(flow_merge_pending_iterator_rev(pending_vec, storage_iter, v))
				}
			}
		}
	}
}

/// Iterator that merges pending writes with storage data (forward order).
struct FlowMergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	storage_iter: Peekable<I>,
	pending_iter: Peekable<IntoIter<(EncodedKey, PendingWrite)>>,
	version: CommitVersion,
}

impl<I> Iterator for FlowMergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	type Item = Result<MultiVersionRow>;

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
							return Some(err);
						}
					};
					let cmp = pending_key.cmp(&storage_val.key);

					if matches!(cmp, Ordering::Less) {
						// Pending key comes first
						let (key, value) = self.pending_iter.next().unwrap();
						if let PendingWrite::Set(row) = value {
							return Some(Ok(MultiVersionRow {
								key,
								row,
								version: self.version,
							}));
						}
						// PendingWrite::Remove = skip (tombstone), continue loop
					} else if matches!(cmp, Ordering::Equal) {
						// Same key - pending shadows storage
						let (key, value) = self.pending_iter.next().unwrap();
						self.storage_iter.next(); // Consume storage entry
						if let PendingWrite::Set(row) = value {
							return Some(Ok(MultiVersionRow {
								key,
								row,
								version: self.version,
							}));
						}
						// PendingWrite::Remove = skip (tombstone), continue loop
					} else {
						// Storage key comes first
						return Some(self.storage_iter.next().unwrap());
					}
				}
				(Some(_), None) => {
					// Only pending left
					let (key, value) = self.pending_iter.next().unwrap();
					if let PendingWrite::Set(row) = value {
						return Some(Ok(MultiVersionRow {
							key,
							row,
							version: self.version,
						}));
					}
					// PendingWrite::Remove = skip (tombstone), continue loop
				}
				(None, Some(_)) => {
					// Only storage left
					return Some(self.storage_iter.next().unwrap());
				}
				(None, None) => return None,
			}
		}
	}
}

/// Create an iterator that merges pending writes with storage data (forward order).
fn flow_merge_pending_iterator<I>(
	pending: Vec<(EncodedKey, PendingWrite)>,
	storage_iter: I,
	version: CommitVersion,
) -> FlowMergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
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
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	storage_iter: Peekable<I>,
	pending_iter: Peekable<IntoIter<(EncodedKey, PendingWrite)>>,
	version: CommitVersion,
}

impl<I> Iterator for FlowMergePendingIteratorRev<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	type Item = Result<MultiVersionRow>;

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
							return Some(err);
						}
					};
					let cmp = pending_key.cmp(&storage_val.key);

					if matches!(cmp, Ordering::Greater) {
						// Reverse: Pending key is larger (comes first in reverse)
						let (key, value) = self.pending_iter.next().unwrap();
						if let PendingWrite::Set(row) = value {
							return Some(Ok(MultiVersionRow {
								key,
								row,
								version: self.version,
							}));
						}
						// PendingWrite::Remove = skip (tombstone), continue loop
					} else if matches!(cmp, Ordering::Equal) {
						// Same key - pending shadows storage
						let (key, value) = self.pending_iter.next().unwrap();
						self.storage_iter.next(); // Consume storage entry
						if let PendingWrite::Set(row) = value {
							return Some(Ok(MultiVersionRow {
								key,
								row,
								version: self.version,
							}));
						}
						// PendingWrite::Remove = skip (tombstone), continue loop
					} else {
						// Storage key comes first in reverse order
						return Some(self.storage_iter.next().unwrap());
					}
				}
				(Some(_), None) => {
					// Only pending left
					let (key, value) = self.pending_iter.next().unwrap();
					if let PendingWrite::Set(row) = value {
						return Some(Ok(MultiVersionRow {
							key,
							row,
							version: self.version,
						}));
					}
					// PendingWrite::Remove = skip (tombstone), continue loop
				}
				(None, Some(_)) => {
					// Only storage left
					return Some(self.storage_iter.next().unwrap());
				}
				(None, None) => return None,
			}
		}
	}
}

/// Create an iterator that merges pending writes with storage data (reverse order).
fn flow_merge_pending_iterator_rev<I>(
	pending: Vec<(EncodedKey, PendingWrite)>,
	storage_iter: I,
	version: CommitVersion,
) -> FlowMergePendingIteratorRev<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
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
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_type::{util::cowvec::CowVec, value::identity::IdentityId};

	use super::*;
	use crate::operator::stateful::test_utils::test::create_test_transaction;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn test_get_from_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		let value = make_value("value1");

		txn.set(&key, value.clone()).unwrap();

		// Should get value from pending buffer
		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(value));
	}

	#[test]
	fn test_get_from_committed() {
		let t = TestEngine::new();

		let key = make_key("key1");
		let value = make_value("value1");

		// Set value in first transaction and commit
		{
			let mut cmd_txn = t.begin_admin(IdentityId::system()).unwrap();
			cmd_txn.set(&key, value.clone()).unwrap();
			cmd_txn.commit().unwrap();
		}

		// Create new command transaction to read committed data
		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();

		// Create FlowTransaction - should see committed value
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

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

		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

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

		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		// Remove in pending
		txn.remove(&key).unwrap();

		// Should return None even though it exists in committed
		let result = txn.get(&key).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_get_nonexistent_key() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let result = txn.get(&make_key("missing")).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_contains_key_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		txn.set(&key, make_value("value1")).unwrap();

		assert!(txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_committed() {
		let t = TestEngine::new();

		let key = make_key("key1");

		// Set value in first transaction and commit
		{
			let mut cmd_txn = t.begin_admin(IdentityId::system()).unwrap();
			cmd_txn.set(&key, make_value("value1")).unwrap();
			cmd_txn.commit().unwrap();
		}

		// Create new command transaction
		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		assert!(txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_removed_returns_false() {
		let mut parent = create_test_transaction();

		let key = make_key("key1");
		parent.set(&key, make_value("value1")).unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		txn.remove(&key).unwrap();

		assert!(!txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_nonexistent() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		assert!(!txn.contains_key(&make_key("missing")).unwrap());
	}

	#[test]
	fn test_scan_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let mut iter = txn.range(EncodedKeyRange::all(), 1024);
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_scan_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>>>().unwrap();

		// Should be in sorted order
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
	}

	#[test]
	fn test_scan_filters_removes() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.remove(&make_key("b")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let items: Vec<_> = txn.range(EncodedKeyRange::all(), 1024).collect::<Result<Vec<_>>>().unwrap();

		// Should only have 2 items (remove filtered out)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_range_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let mut iter = txn.range(range, 1024);
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_range_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();
		txn.set(&make_key("d"), make_value("4")).unwrap();

		let range = EncodedKeyRange::new(Included(make_key("b")), Excluded(make_key("d")));
		let items: Vec<_> = txn.range(range, 1024).collect::<Result<Vec<_>>>().unwrap();

		// Should only include b and c (not d, exclusive end)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_prefix_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let prefix = make_key("test_");
		let iter = txn.prefix(&prefix).unwrap();
		assert!(iter.items.into_iter().next().is_none());
	}

	#[test]
	fn test_prefix_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

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
