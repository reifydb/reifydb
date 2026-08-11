// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	iter,
	ops::{
		Bound,
		Bound::{Excluded, Included, Unbounded},
	},
	vec,
};

use iter::Peekable;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	actors::pending::PendingWrite,
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, store::MultiVersionRow},
	key::{
		Key,
		kind::KeyKind,
		operator_state::{OperatorStateKey, node_prefix},
	},
};
use reifydb_store_operator::store::OperatorStore;
use reifydb_value::Result;
use vec::IntoIter;

use crate::transaction::substrate::operator_state_coordinates;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFrom {
	OperatorState,

	StateQuery,

	Query,

	OwnedRow,
}

pub fn read_from(key: &EncodedKey) -> ReadFrom {
	match Key::kind(key) {
		None => ReadFrom::Query,
		Some(kind) => match kind {
			KeyKind::OperatorState => ReadFrom::OperatorState,
			KeyKind::RingBufferMetadata => ReadFrom::StateQuery,
			KeyKind::SeriesMetadata => ReadFrom::StateQuery,

			KeyKind::Row => ReadFrom::OwnedRow,
			KeyKind::PartitionedRow => ReadFrom::OwnedRow,
			KeyKind::Partition => ReadFrom::OwnedRow,

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
			KeyKind::OutputFrontier => ReadFrom::Query,
			KeyKind::View => ReadFrom::Query,
			KeyKind::NamespaceView => ReadFrom::Query,
			KeyKind::PrimaryKey => ReadFrom::Query,
			KeyKind::RingBuffer => ReadFrom::Query,
			KeyKind::NamespaceRingBuffer => ReadFrom::Query,
			KeyKind::Queue => ReadFrom::Query,
			KeyKind::NamespaceQueue => ReadFrom::Query,
			KeyKind::QueueDeduplication => ReadFrom::Query,
			KeyKind::Flow => ReadFrom::Query,
			KeyKind::NamespaceFlow => ReadFrom::Query,
			KeyKind::Operator => ReadFrom::Query,
			KeyKind::OperatorByFlow => ReadFrom::Query,
			KeyKind::FlowEdge => ReadFrom::Query,
			KeyKind::FlowEdgeByFlow => ReadFrom::Query,
			KeyKind::Dictionary => ReadFrom::Query,
			KeyKind::DictionaryEntry => ReadFrom::Query,
			KeyKind::DictionaryEntryIndex => ReadFrom::Query,
			KeyKind::NamespaceDictionary => ReadFrom::Query,
			KeyKind::Metric => ReadFrom::Query,
			KeyKind::FlowVersion => ReadFrom::Query,
			KeyKind::Subscription => ReadFrom::Query,
			KeyKind::SubscriptionRow => ReadFrom::Query,
			KeyKind::SubscriptionColumn => ReadFrom::Query,
			KeyKind::RowShape => ReadFrom::Query,
			KeyKind::RowShapeField => ReadFrom::Query,
			KeyKind::SumType => ReadFrom::Query,
			KeyKind::NamespaceSumType => ReadFrom::Query,
			KeyKind::Handler => ReadFrom::Query,
			KeyKind::NamespaceHandler => ReadFrom::Query,
			KeyKind::VariantHandler => ReadFrom::Query,
			KeyKind::Series => ReadFrom::Query,
			KeyKind::NamespaceSeries => ReadFrom::Query,
			KeyKind::Identity => ReadFrom::Query,
			KeyKind::IdentityAttribute => ReadFrom::Query,
			KeyKind::IdentityAttributeValue => ReadFrom::Query,
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
			KeyKind::RowSettings => ReadFrom::Query,
			KeyKind::OperatorSettings => ReadFrom::Query,
			KeyKind::Procedure => ReadFrom::Query,
			KeyKind::NamespaceProcedure => ReadFrom::Query,
			KeyKind::ProcedureParam => ReadFrom::Query,
			KeyKind::Binding => ReadFrom::Query,
			KeyKind::NamespaceBinding => ReadFrom::Query,
			KeyKind::ColumnSnapshot => ReadFrom::Query,
			KeyKind::SeriesColumnSnapshot => ReadFrom::Query,
			KeyKind::TableColumnSnapshot => ReadFrom::Query,
			KeyKind::VersionEpoch => ReadFrom::Query,
			KeyKind::Relationship => ReadFrom::Query,
		},
	}
}

pub(crate) fn operator_state_scope(range: &EncodedKeyRange) -> Option<(OperatorId, EncodedKeyRange)> {
	let start_key = match range.start.as_ref() {
		Included(key) | Excluded(key) => key,
		Unbounded => return None,
	};
	if Key::kind(start_key) != Some(KeyKind::OperatorState) {
		return None;
	}
	let (operator, _) =
		operator_state_coordinates(start_key).expect("an OperatorState-routed key must carry an operator id");
	let prefix = EncodedKey::new(node_prefix(operator));
	let strip = |bound: Bound<&EncodedKey>| match bound {
		Included(key) if key.as_slice().starts_with(prefix.as_slice()) => {
			Included(EncodedKey::new(&key.as_slice()[prefix.len()..]))
		}
		Excluded(key) if key.as_slice().starts_with(prefix.as_slice()) => {
			Excluded(EncodedKey::new(&key.as_slice()[prefix.len()..]))
		}
		_ => Unbounded,
	};
	Some((operator, EncodedKeyRange::new(strip(range.start.as_ref()), strip(range.end.as_ref()))))
}

pub(crate) struct OperatorStateRangeIter {
	store: OperatorStore,
	operator: OperatorId,
	end: Bound<EncodedKey>,
	cursor: Bound<EncodedKey>,
	batch_size: u64,
	buffered: IntoIter<(EncodedKey, EncodedBytes)>,
	exhausted: bool,
	version: CommitVersion,
}

impl OperatorStateRangeIter {
	pub(crate) fn new(
		store: OperatorStore,
		operator: OperatorId,
		range: EncodedKeyRange,
		batch_size: usize,
		version: CommitVersion,
	) -> Self {
		Self {
			store,
			operator,
			cursor: range.start,
			end: range.end,
			batch_size: batch_size.max(1) as u64,
			buffered: Vec::new().into_iter(),
			exhausted: false,
			version,
		}
	}
}

impl Iterator for OperatorStateRangeIter {
	type Item = Result<MultiVersionRow>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if let Some((inner_key, bytes)) = self.buffered.next() {
				self.cursor = Bound::Excluded(inner_key.clone());
				return Some(Ok(MultiVersionRow {
					key: {
						let (group, keyspace, suffix) =
							OperatorStateKey::decode_inner(inner_key.as_slice())
								.expect("inner keys must carry a structured encoding");
						OperatorStateKey::encoded(self.operator, group, keyspace, suffix)
					},
					bytes,
					version: self.version,
				}));
			}
			if self.exhausted {
				return None;
			}
			let range = EncodedKeyRange::new(self.cursor.clone(), self.end.clone());
			let batch = self.store.range_batch(self.operator, range, self.batch_size);
			self.exhausted = !batch.has_more;
			if batch.items.is_empty() {
				return None;
			}
			self.buffered = batch
				.items
				.into_iter()
				.map(|(key, row)| (key, row.into_bytes()))
				.collect::<Vec<_>>()
				.into_iter();
		}
	}
}

pub(crate) struct FlowMergePendingIterator<I>
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
							let err = self.storage_iter.next().unwrap();
							return Some(err);
						}
					};
					let cmp = pending_key.cmp(&storage_val.key);

					if matches!(cmp, Ordering::Less) {
						let (key, value) = self.pending_iter.next().unwrap();
						if let PendingWrite::Set(bytes) = value {
							return Some(Ok(MultiVersionRow {
								key,
								bytes,
								version: self.version,
							}));
						}
					} else if matches!(cmp, Ordering::Equal) {
						let (key, value) = self.pending_iter.next().unwrap();
						self.storage_iter.next();
						if let PendingWrite::Set(bytes) = value {
							return Some(Ok(MultiVersionRow {
								key,
								bytes,
								version: self.version,
							}));
						}
					} else {
						return Some(self.storage_iter.next().unwrap());
					}
				}
				(Some(_), None) => {
					let (key, value) = self.pending_iter.next().unwrap();
					if let PendingWrite::Set(bytes) = value {
						return Some(Ok(MultiVersionRow {
							key,
							bytes,
							version: self.version,
						}));
					}
				}
				(None, Some(_)) => {
					return Some(self.storage_iter.next().unwrap());
				}
				(None, None) => return None,
			}
		}
	}
}

pub(crate) fn flow_merge_pending_iterator<I>(
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

pub(crate) struct FlowMergePendingIteratorRev<I>
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
							let err = self.storage_iter.next().unwrap();
							return Some(err);
						}
					};
					let cmp = pending_key.cmp(&storage_val.key);

					if matches!(cmp, Ordering::Greater) {
						let (key, value) = self.pending_iter.next().unwrap();
						if let PendingWrite::Set(bytes) = value {
							return Some(Ok(MultiVersionRow {
								key,
								bytes,
								version: self.version,
							}));
						}
					} else if matches!(cmp, Ordering::Equal) {
						let (key, value) = self.pending_iter.next().unwrap();
						self.storage_iter.next();
						if let PendingWrite::Set(bytes) = value {
							return Some(Ok(MultiVersionRow {
								key,
								bytes,
								version: self.version,
							}));
						}
					} else {
						return Some(self.storage_iter.next().unwrap());
					}
				}
				(Some(_), None) => {
					let (key, value) = self.pending_iter.next().unwrap();
					if let PendingWrite::Set(bytes) = value {
						return Some(Ok(MultiVersionRow {
							key,
							bytes,
							version: self.version,
						}));
					}
				}
				(None, Some(_)) => {
					return Some(self.storage_iter.next().unwrap());
				}
				(None, None) => return None,
			}
		}
	}
}

pub(crate) fn flow_merge_pending_iterator_rev<I>(
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
	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		row::bytes::EncodedBytes,
	};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{interceptor::interceptors::Interceptors, multi::RangeScope};
	use reifydb_value::{util::cowvec::CowVec, value::identity::IdentityId};

	use super::*;
	use crate::{
		test_util::create_test_transaction,
		transaction::{deferred::DeferredTransaction, interface::FlowTransaction},
	};

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn make_value(s: &str) -> EncodedBytes {
		EncodedBytes(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn test_get_from_pending() {
		let parent = create_test_transaction();
		let mut txn = DeferredTransaction::new(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		let value = make_value("value1");

		txn.set(&key, value.clone()).unwrap();

		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(value));
	}

	#[test]
	fn test_get_from_committed() {
		let t = TestEngine::new();

		let key = make_key("key1");
		let value = make_value("value1");

		{
			let mut cmd_txn = t.begin_admin(IdentityId::system()).unwrap();
			cmd_txn.set(&key, value.clone()).unwrap();
			cmd_txn.commit().unwrap();
		}

		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();

		let mut txn = DeferredTransaction::new(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(value));
	}

	#[test]
	fn test_get_pending_shadows_committed() {
		let mut parent = create_test_transaction();

		let key = make_key("key1");
		parent.set(&key, make_value("old")).unwrap();
		let version = parent.version();

		let mut txn = DeferredTransaction::new(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let new_value = make_value("new");
		txn.set(&key, new_value.clone()).unwrap();

		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(new_value));
	}

	#[test]
	fn test_get_removed_returns_none() {
		let mut parent = create_test_transaction();

		let key = make_key("key1");
		parent.set(&key, make_value("value1")).unwrap();
		let version = parent.version();

		let mut txn = DeferredTransaction::new(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.remove(&key).unwrap();

		let result = txn.get(&key).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_get_nonexistent_key() {
		let parent = create_test_transaction();
		let mut txn = DeferredTransaction::new(
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
		let mut txn = DeferredTransaction::new(
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

		{
			let mut cmd_txn = t.begin_admin(IdentityId::system()).unwrap();
			cmd_txn.set(&key, make_value("value1")).unwrap();
			cmd_txn.commit().unwrap();
		}

		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = DeferredTransaction::new(
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

		let mut txn = DeferredTransaction::new(
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
		let mut txn = DeferredTransaction::new(
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
		let mut txn = DeferredTransaction::new(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let mut iter = txn.range(EncodedKeyRange::all(), RangeScope::All, 1024);
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_scan_only_pending() {
		let parent = create_test_transaction();
		let mut txn = DeferredTransaction::new(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let items: Vec<_> =
			txn.range(EncodedKeyRange::all(), RangeScope::All, 1024).collect::<Result<Vec<_>>>().unwrap();

		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
	}

	#[test]
	fn test_scan_filters_removes() {
		let parent = create_test_transaction();
		let mut txn = DeferredTransaction::new(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.remove(&make_key("b")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let items: Vec<_> =
			txn.range(EncodedKeyRange::all(), RangeScope::All, 1024).collect::<Result<Vec<_>>>().unwrap();

		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_range_empty() {
		let parent = create_test_transaction();
		let mut txn = DeferredTransaction::new(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let mut iter = txn.range(range, RangeScope::All, 1024);
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_range_only_pending() {
		let parent = create_test_transaction();
		let mut txn = DeferredTransaction::new(
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
		let items: Vec<_> = txn.range(range, RangeScope::All, 1024).collect::<Result<Vec<_>>>().unwrap();

		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_prefix_empty() {
		let parent = create_test_transaction();
		let mut txn = DeferredTransaction::new(
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
		let mut txn = DeferredTransaction::new(
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

		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("test_a"));
		assert_eq!(items[1].key, make_key("test_b"));
	}
}
