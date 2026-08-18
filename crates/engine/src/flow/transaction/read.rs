// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	collections, iter,
	ops::Bound::{Excluded, Included, Unbounded},
	vec,
};

use collections::BTreeMap;
use iter::Peekable;
use reifydb_codec::{
	row::operator::EncodedOperatorRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	actors::pending::PendingWrite,
	common::CommitVersion,
	interface::store::{MultiVersionBatch, MultiVersionRow},
	key::{Key, kind::KeyKind},
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{Result, error::Error as ValueError};
use vec::IntoIter;

use super::FlowTransaction;

pub(crate) enum ReadFrom {
	StateQuery,

	Query,

	OwnedRow,
}

impl FlowTransaction {
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedOperatorRow>> {
		let inner = self.inner();
		if inner.pending.is_removed(key) {
			return Ok(None);
		}
		if let Some(value) = inner.pending.get(key) {
			return Ok(Some(EncodedOperatorRow::try_from(value.clone()).map_err(ValueError::from)?));
		}
		if inner.base_pending.is_removed(key) {
			return Ok(None);
		}
		if let Some(value) = inner.base_pending.get(key) {
			return Ok(Some(EncodedOperatorRow::try_from(value.clone()).map_err(ValueError::from)?));
		}

		if matches!(self, Self::Ephemeral { .. }) {
			unimplemented!("ephemeral flow transaction")
		}

		if let Some(cached) = self.inner().prefetch.get(key) {
			return Ok(cached.clone());
		}

		let inner = self.inner_mut();
		inner.store_reads += 1;
		let route = Self::read_from(key);
		let query = match route {
			ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
			ReadFrom::Query => &inner.query,
			ReadFrom::OwnedRow => inner.state_query.as_ref().unwrap_or(&inner.query),
		};
		let result = match query.get(key)? {
			Some(multi) => {
				Some(EncodedOperatorRow::try_from(multi.bytes().clone()).map_err(ValueError::from)?)
			}
			None => None,
		};
		if matches!(route, ReadFrom::StateQuery) {
			inner.prefetch.insert(key.clone(), result.clone());
		}
		Ok(result)
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		let inner = self.inner();
		if inner.pending.is_removed(key) {
			return Ok(false);
		}
		if inner.pending.get(key).is_some() {
			return Ok(true);
		}
		if inner.base_pending.is_removed(key) {
			return Ok(false);
		}
		if inner.base_pending.get(key).is_some() {
			return Ok(true);
		}

		if matches!(self, Self::Ephemeral { .. }) {
			unimplemented!("ephemeral flow transaction")
		}

		let inner = self.inner_mut();
		let query = match Self::read_from(key) {
			ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
			ReadFrom::Query => &inner.query,
			ReadFrom::OwnedRow => inner.state_query.as_ref().unwrap_or(&inner.query),
		};
		query.contains_key(key)
	}

	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let range = EncodedKeyRange::prefix(prefix);
		let items = self.range(range, RangeScope::All, 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub(crate) fn read_from(key: &EncodedKey) -> ReadFrom {
		match Key::kind(key) {
			None => ReadFrom::Query,
			Some(kind) => match kind {
				KeyKind::OperatorState => ReadFrom::StateQuery,
				KeyKind::OperatorSettings => ReadFrom::StateQuery,
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
				KeyKind::View => ReadFrom::Query,
				KeyKind::NamespaceView => ReadFrom::Query,
				KeyKind::PrimaryKey => ReadFrom::Query,
				KeyKind::RingBuffer => ReadFrom::Query,
				KeyKind::NamespaceRingBuffer => ReadFrom::Query,
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
				KeyKind::Procedure => ReadFrom::Query,
				KeyKind::NamespaceProcedure => ReadFrom::Query,
				KeyKind::ProcedureParam => ReadFrom::Query,
				KeyKind::Binding => ReadFrom::Query,
				KeyKind::NamespaceBinding => ReadFrom::Query,
				KeyKind::ColumnSnapshot => ReadFrom::Query,
				KeyKind::SeriesColumnSnapshot => ReadFrom::Query,
				KeyKind::TableColumnSnapshot => ReadFrom::Query,
				KeyKind::OutputFrontier => ReadFrom::Query,
				KeyKind::Queue => ReadFrom::Query,
				KeyKind::NamespaceQueue => ReadFrom::Query,
				KeyKind::QueueDeduplication => ReadFrom::Query,
				KeyKind::Relationship => ReadFrom::Query,
				KeyKind::VersionEpoch => ReadFrom::Query,
			},
		}
	}

	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		match self {
			Self::Deferred {
				inner,
				..
			}
			| Self::Committing {
				inner,
				..
			}
			| Self::Transactional {
				inner,
				..
			} => {
				let mut merged: BTreeMap<EncodedKey, PendingWrite> = inner
					.base_pending
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
						ReadFrom::OwnedRow => {
							inner.state_query.as_ref().unwrap_or(&inner.query)
						}
					},
					Unbounded => &inner.query,
				};

				let storage_iter = query.range(range, scope, batch_size);
				let v = inner.version;
				Box::new(flow_merge_pending_iterator(pending_vec, storage_iter, v))
			}
			Self::Ephemeral { .. } => unimplemented!("ephemeral flow transaction"),
		}
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		match self {
			Self::Deferred {
				inner,
				..
			}
			| Self::Committing {
				inner,
				..
			}
			| Self::Transactional {
				inner,
				..
			} => {
				let mut merged: BTreeMap<EncodedKey, PendingWrite> = inner
					.base_pending
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
						ReadFrom::OwnedRow => {
							inner.state_query.as_ref().unwrap_or(&inner.query)
						}
					},
					Unbounded => &inner.query,
				};

				let storage_iter = query.range_rev(range, scope, batch_size);
				let v = inner.version;
				Box::new(flow_merge_pending_iterator_rev(pending_vec, storage_iter, v))
			}
			Self::Ephemeral { .. } => unimplemented!("ephemeral flow transaction"),
		}
	}
}

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
							let err = self.storage_iter.next().unwrap();
							return Some(err);
						}
					};
					let cmp = pending_key.cmp(&storage_val.key);

					if matches!(cmp, Ordering::Less) {
						let (key, value) = self.pending_iter.next().unwrap();
						if let PendingWrite::Set(row) = value {
							return Some(Ok(MultiVersionRow {
								key,
								bytes: row,
								version: self.version,
							}));
						}
					} else if matches!(cmp, Ordering::Equal) {
						let (key, value) = self.pending_iter.next().unwrap();
						self.storage_iter.next();
						if let PendingWrite::Set(row) = value {
							return Some(Ok(MultiVersionRow {
								key,
								bytes: row,
								version: self.version,
							}));
						}
					} else {
						return Some(self.storage_iter.next().unwrap());
					}
				}
				(Some(_), None) => {
					let (key, value) = self.pending_iter.next().unwrap();
					if let PendingWrite::Set(row) = value {
						return Some(Ok(MultiVersionRow {
							key,
							bytes: row,
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
							let err = self.storage_iter.next().unwrap();
							return Some(err);
						}
					};
					let cmp = pending_key.cmp(&storage_val.key);

					if matches!(cmp, Ordering::Greater) {
						let (key, value) = self.pending_iter.next().unwrap();
						if let PendingWrite::Set(row) = value {
							return Some(Ok(MultiVersionRow {
								key,
								bytes: row,
								version: self.version,
							}));
						}
					} else if matches!(cmp, Ordering::Equal) {
						let (key, value) = self.pending_iter.next().unwrap();
						self.storage_iter.next();
						if let PendingWrite::Set(row) = value {
							return Some(Ok(MultiVersionRow {
								key,
								bytes: row,
								version: self.version,
							}));
						}
					} else {
						return Some(self.storage_iter.next().unwrap());
					}
				}
				(Some(_), None) => {
					let (key, value) = self.pending_iter.next().unwrap();
					if let PendingWrite::Set(row) = value {
						return Some(Ok(MultiVersionRow {
							key,
							bytes: row,
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
