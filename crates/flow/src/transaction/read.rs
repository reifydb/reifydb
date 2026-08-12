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

