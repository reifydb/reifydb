// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, iter, ops::Bound, vec};

use iter::Peekable;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	actors::pending::PendingWrite,
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, store::MultiVersionRow},
	key::{any::TaggedKey, operator::state::OperatorStateKey, tag::KeyTag},
};
use reifydb_store_operator::store::OperatorStore;
use reifydb_value::Result;
use vec::IntoIter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFrom {
	OperatorState,

	StateQuery,

	Query,

	OwnedRow,
}

pub fn read_from(key: &EncodedKey) -> ReadFrom {
	match KeyTag::of(key) {
		None => ReadFrom::Query,
		Some(kind) => match kind {
			KeyTag::OperatorState => ReadFrom::OperatorState,
			KeyTag::RingBufferMetadata => ReadFrom::StateQuery,
			KeyTag::SeriesMetadata => ReadFrom::StateQuery,

			KeyTag::Row => ReadFrom::OwnedRow,
			KeyTag::SeriesRow => ReadFrom::OwnedRow,
			KeyTag::PartitionedRow => ReadFrom::OwnedRow,
			KeyTag::PartitionedSeriesRow => ReadFrom::OwnedRow,
			KeyTag::SortedViewRow => ReadFrom::OwnedRow,
			KeyTag::PartitionedSortedViewRow => ReadFrom::OwnedRow,
			KeyTag::Partition => ReadFrom::OwnedRow,

			KeyTag::Namespace => ReadFrom::Query,
			KeyTag::Table => ReadFrom::Query,
			KeyTag::NamespaceTable => ReadFrom::Query,
			KeyTag::SystemSequence => ReadFrom::Query,
			KeyTag::Columns => ReadFrom::Query,
			KeyTag::Column => ReadFrom::Query,
			KeyTag::RowSequence => ReadFrom::Query,
			KeyTag::ColumnProperty => ReadFrom::Query,
			KeyTag::SystemVersion => ReadFrom::Query,
			KeyTag::TransactionVersion => ReadFrom::Query,
			KeyTag::Index => ReadFrom::Query,
			KeyTag::IndexEntry => ReadFrom::Query,
			KeyTag::ColumnSequence => ReadFrom::Query,
			KeyTag::CdcConsumer => ReadFrom::Query,
			KeyTag::OutputFrontier => ReadFrom::Query,
			KeyTag::View => ReadFrom::Query,
			KeyTag::NamespaceView => ReadFrom::Query,
			KeyTag::PrimaryKey => ReadFrom::Query,
			KeyTag::RingBuffer => ReadFrom::Query,
			KeyTag::NamespaceRingBuffer => ReadFrom::Query,
			KeyTag::Queue => ReadFrom::Query,
			KeyTag::NamespaceQueue => ReadFrom::Query,
			KeyTag::QueueDeduplication => ReadFrom::Query,
			KeyTag::QueuePartition => ReadFrom::Query,
			KeyTag::QueueItemState => ReadFrom::Query,
			KeyTag::QueueDue => ReadFrom::Query,
			KeyTag::QueueAttempt => ReadFrom::Query,
			KeyTag::QueueKeyActive => ReadFrom::Query,
			KeyTag::Flow => ReadFrom::Query,
			KeyTag::NamespaceFlow => ReadFrom::Query,
			KeyTag::Operator => ReadFrom::Query,
			KeyTag::OperatorByFlow => ReadFrom::Query,
			KeyTag::FlowEdge => ReadFrom::Query,
			KeyTag::FlowEdgeByFlow => ReadFrom::Query,
			KeyTag::Dictionary => ReadFrom::Query,
			KeyTag::DictionaryEntry => ReadFrom::Query,
			KeyTag::DictionaryEntryIndex => ReadFrom::Query,
			KeyTag::NamespaceDictionary => ReadFrom::Query,
			KeyTag::Metric => ReadFrom::Query,
			KeyTag::FlowVersion => ReadFrom::Query,
			KeyTag::RowShape => ReadFrom::Query,
			KeyTag::RowShapeField => ReadFrom::Query,
			KeyTag::SumType => ReadFrom::Query,
			KeyTag::NamespaceSumType => ReadFrom::Query,
			KeyTag::Handler => ReadFrom::Query,
			KeyTag::NamespaceHandler => ReadFrom::Query,
			KeyTag::VariantHandler => ReadFrom::Query,
			KeyTag::Series => ReadFrom::Query,
			KeyTag::NamespaceSeries => ReadFrom::Query,
			KeyTag::Identity => ReadFrom::Query,
			KeyTag::IdentityAttribute => ReadFrom::Query,
			KeyTag::IdentityAttributeValue => ReadFrom::Query,
			KeyTag::Role => ReadFrom::Query,
			KeyTag::GrantedRole => ReadFrom::Query,
			KeyTag::Policy => ReadFrom::Query,
			KeyTag::PolicyOp => ReadFrom::Query,
			KeyTag::Migration => ReadFrom::Query,
			KeyTag::MigrationEvent => ReadFrom::Query,
			KeyTag::Authentication => ReadFrom::Query,
			KeyTag::ConfigStorage => ReadFrom::Query,
			KeyTag::Token => ReadFrom::Query,
			KeyTag::Source => ReadFrom::Query,
			KeyTag::NamespaceSource => ReadFrom::Query,
			KeyTag::Sink => ReadFrom::Query,
			KeyTag::NamespaceSink => ReadFrom::Query,
			KeyTag::RowSettings => ReadFrom::Query,
			KeyTag::OperatorSettings => ReadFrom::Query,
			KeyTag::Procedure => ReadFrom::Query,
			KeyTag::NamespaceProcedure => ReadFrom::Query,
			KeyTag::ProcedureParam => ReadFrom::Query,
			KeyTag::Binding => ReadFrom::Query,
			KeyTag::NamespaceBinding => ReadFrom::Query,
			KeyTag::ColumnSnapshot => ReadFrom::Query,
			KeyTag::SeriesColumnSnapshot => ReadFrom::Query,
			KeyTag::TableColumnSnapshot => ReadFrom::Query,
			KeyTag::VersionEpoch => ReadFrom::Query,
			KeyTag::Relationship => ReadFrom::Query,
		},
	}
}

const UNDECODABLE_PENDING_KEY: &str = "a pending flow write must carry a decodable key";

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
	type Item = Result<MultiVersionRow<TaggedKey>>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if let Some((inner_key, bytes)) = self.buffered.next() {
				self.cursor = Bound::Excluded(inner_key.clone());
				return Some(Ok(MultiVersionRow {
					key: {
						let (group, keyspace, suffix) =
							OperatorStateKey::decode_inner(inner_key.as_slice())
								.expect("inner keys must carry a structured encoding");
						OperatorStateKey::new(self.operator, group, keyspace, suffix).into()
					},
					bytes,
					version: self.version,
				}));
			}
			if self.exhausted {
				return None;
			}
			let range = EncodedKeyRange::new(self.cursor.clone(), self.end.clone());
			let batch = match self.store.range_batch(self.operator, range, self.batch_size) {
				Ok(batch) => batch,
				Err(e) => {
					self.exhausted = true;
					return Some(Err(e.into()));
				}
			};
			self.exhausted = !batch.has_more;
			let resumed = match batch.resume {
				Some(key) => {
					self.cursor = Bound::Excluded(key.into_encoded());
					true
				}
				None => false,
			};
			if batch.items.is_empty() {
				if !resumed {
					return None;
				}
				continue;
			}
			self.buffered = batch
				.items
				.into_iter()
				.map(|(key, row)| (key.into_encoded(), row.into_bytes()))
				.collect::<Vec<_>>()
				.into_iter();
		}
	}
}

pub(crate) struct FlowMergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow<TaggedKey>>>,
{
	storage_iter: Peekable<I>,
	pending_iter: Peekable<IntoIter<(TaggedKey, PendingWrite)>>,
	version: CommitVersion,
}

impl<I> Iterator for FlowMergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow<TaggedKey>>>,
{
	type Item = Result<MultiVersionRow<TaggedKey>>;

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
	I: Iterator<Item = Result<MultiVersionRow<TaggedKey>>>,
{
	let pending: Vec<(TaggedKey, PendingWrite)> = pending
		.into_iter()
		.map(|(key, write)| (TaggedKey::decode(&key).expect(UNDECODABLE_PENDING_KEY), write))
		.collect();
	FlowMergePendingIterator {
		storage_iter: storage_iter.peekable(),
		pending_iter: pending.into_iter().peekable(),
		version,
	}
}
