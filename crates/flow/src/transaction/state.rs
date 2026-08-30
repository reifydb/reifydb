// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, collections::BTreeMap};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{
		bytes::EncodedBytes,
		operator::state::{OperatorState, decode},
		pod::EncodedPodRow,
	},
};
use reifydb_core::{
	actors::pending::PendingWrite,
	interface::{
		catalog::flow::OperatorId,
		store::{MultiVersionBatch, MultiVersionRow},
	},
	key::operator_state::{GroupStateKey, KeyspaceId, OperatorStateKey, node_prefix},
	metrics::scan::ScanCounters,
};
use reifydb_store_operator::{store::StateLastIter, types::JOIN_EXPIRY_VALUE_BYTES};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{Result, byte_size::ByteSize};
use tracing::{Span, field, instrument};

use crate::transaction::{FlowTransaction, join_expiry::decode_join_expiry_suffix, scope::scoped_key};

pub(crate) fn encode_payload<T: OperatorState>(value: &T) -> Result<EncodedPodRow> {
	Ok(value.encode_state()?)
}

pub(crate) fn decode_payload<T: OperatorState>(row: &EncodedPodRow) -> Result<T> {
	Ok(decode(row)?)
}

#[derive(Debug, Clone)]
pub struct StateRange {
	pub range: EncodedKeyRange,
	pub limit: Option<usize>,
	pub site: &'static str,
}

impl StateRange {
	pub fn forward(range: EncodedKeyRange, site: &'static str) -> Self {
		Self {
			range,
			limit: None,
			site,
		}
	}

	pub fn limit(mut self, limit: usize) -> Self {
		self.limit = Some(limit);
		self
	}
}

pub trait StateExtension: FlowTransaction {
	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len(),
		found = field::Empty
	))]
	fn state_get(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		let scoped = scoped_key(id, key);
		let result = self.get(&scoped)?.map(EncodedPodRow::from);
		Span::current().record("found", result.is_some());
		Ok(result)
	}

	#[instrument(name = "flow::state::get_many", level = "debug", skip(self, keys), fields(
		operator_id = id.0,
		key_count = keys.len(),
		found_count = field::Empty
	))]
	fn state_get_many(&mut self, id: OperatorId, keys: &[GroupStateKey]) -> Result<MultiVersionBatch> {
		let version = self.version();
		let mut items: Vec<MultiVersionRow> = Vec::with_capacity(keys.len());
		let mut to_batch: Vec<EncodedKey> = Vec::new();

		for key in keys {
			let encoded_key = scoped_key(id, key);
			match self.lookup_overlays(&encoded_key) {
				Some(None) => continue,
				Some(Some(bytes)) => items.push(MultiVersionRow {
					key: encoded_key,
					bytes,
					version,
				}),
				None => to_batch.push(encoded_key),
			}
		}

		self.fetch_state_external(to_batch, &mut items)?;

		Span::current().record("found_count", items.len());
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	fn state_classify(&mut self, id: OperatorId, key: &GroupStateKey, pre: Option<ByteSize>) {
		let scoped = scoped_key(id, key);
		if self.pending_layers().top().contains_key(&scoped) {
			return;
		}
		self.classify(&scoped, pre);
	}

	#[instrument(name = "flow::state::set", level = "trace", skip(self, row), fields(
		operator_id = id.0,
		key_len = key.as_slice().len(),
		value_len = row.len()
	))]
	fn state_set(&mut self, id: OperatorId, key: &GroupStateKey, row: EncodedPodRow) -> Result<()> {
		let scoped = scoped_key(id, key);
		classify_state_write(self, id, key, &scoped)?;
		self.set(&scoped, row.into_bytes())
	}

	#[instrument(name = "flow::state::remove", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len()
	))]
	fn state_remove(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<()> {
		let scoped = scoped_key(id, key);
		classify_state_write(self, id, key, &scoped)?;
		self.remove_silent(&scoped)
	}

	#[instrument(name = "flow::state::scan", level = "debug", skip(self), fields(
		operator_id = id.0,
		result_count = field::Empty
	))]
	fn state_scan_all(&mut self, id: OperatorId) -> Result<MultiVersionBatch> {
		let range = OperatorStateKey::node_range(id);
		let iter = self.range(range, RangeScope::All, 1024);
		let mut items = Vec::new();
		for result in iter {
			items.push(result?);
		}
		Span::current().record("result_count", items.len());
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[instrument(name = "flow::state::range", level = "debug", skip(self, query), fields(
		operator_id = id.0,
		site = query.site,
		rows_fetched = field::Empty,
		rows_tombstoned = field::Empty
	))]
	fn state_range(&mut self, id: OperatorId, query: StateRange) -> Result<MultiVersionBatch> {
		let before = ScanCounters::sample();
		let prefixed_range = query.range.with_prefix(EncodedKey::new(node_prefix(id)));
		let batch_size = query.limit.map_or(1024, |limit| limit.saturating_add(1).min(1024));
		let iter = self.range(prefixed_range, RangeScope::All, batch_size);
		let mut items = Vec::new();
		let mut has_more = false;
		for result in iter {
			if query.limit.is_some_and(|l| items.len() == l) {
				has_more = true;
				break;
			}
			items.push(result?);
		}
		let scanned = before.since();
		let span = Span::current();
		span.record("rows_fetched", scanned.fetched);
		span.record("rows_tombstoned", scanned.tombstones);
		Ok(MultiVersionBatch {
			items,
			has_more,
		})
	}

	#[instrument(name = "flow::state::last", level = "debug", skip(self, range), fields(
		operator_id = id.0,
		found = field::Empty
	))]
	fn state_last(&mut self, id: OperatorId, range: EncodedKeyRange) -> Result<Option<MultiVersionRow>> {
		let prefix = node_prefix(id);
		let prefixed_range = range.with_prefix(EncodedKey::new(prefix.clone()));
		let mut merged = BTreeMap::new();
		self.pending_layers()
			.collect_range((prefixed_range.start.as_ref(), prefixed_range.end.as_ref()), &mut merged);
		let pending: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().rev().collect();

		let version = self.version();
		let store = self.operator_store();
		let mut index = 0usize;
		let mut scan = store.state_last_iter(id, range);
		let mut stored = next_stored(&mut scan, &prefix);

		let found = loop {
			match (pending.get(index), stored.take()) {
				(None, None) => break None,
				(None, Some((_, key, bytes))) => {
					break Some(MultiVersionRow {
						key,
						bytes,
						version,
					});
				}
				(Some((key, write)), None) => {
					index += 1;
					if let PendingWrite::Set(value) = write {
						break Some(MultiVersionRow {
							key: key.clone(),
							bytes: value.clone(),
							version,
						});
					}
				}
				(Some((pending_key, write)), Some((inner, key, bytes))) => {
					match pending_key.cmp(&key) {
						Ordering::Greater => {
							index += 1;
							stored = Some((inner, key, bytes));
							if let PendingWrite::Set(value) = write {
								break Some(MultiVersionRow {
									key: pending_key.clone(),
									bytes: value.clone(),
									version,
								});
							}
						}
						Ordering::Less => {
							break Some(MultiVersionRow {
								key,
								bytes,
								version,
							});
						}
						Ordering::Equal => {
							index += 1;
							if let PendingWrite::Set(value) = write {
								break Some(MultiVersionRow {
									key: pending_key.clone(),
									bytes: value.clone(),
									version,
								});
							}
							stored = next_stored(&mut scan, &prefix);
						}
					}
				}
			}
		};
		Span::current().record("found", found.is_some());
		Ok(found)
	}

	#[instrument(name = "flow::state::clear", level = "trace", skip(self), fields(
		operator_id = id.0,
		keys_removed = field::Empty
	))]
	fn state_clear(&mut self, id: OperatorId) -> Result<()> {
		let keys_to_remove = scan_keys_for_clear(self, id)?;

		let count = keys_to_remove.len();
		remove_keys(self, keys_to_remove)?;

		Span::current().record("keys_removed", count);
		Ok(())
	}
}

impl<T: FlowTransaction> StateExtension for T {}

fn next_stored(scan: &mut StateLastIter<'_>, prefix: &[u8]) -> Option<(EncodedKey, EncodedKey, EncodedBytes)> {
	scan.next().map(|(inner, row)| {
		let mut scoped = Vec::with_capacity(prefix.len() + inner.len());
		scoped.extend_from_slice(prefix);
		scoped.extend_from_slice(inner.as_slice());
		(inner, EncodedKey::new(scoped), row.into_bytes())
	})
}

#[inline]
fn classify_state_write<T: FlowTransaction>(
	txn: &mut T,
	id: OperatorId,
	key: &GroupStateKey,
	scoped: &EncodedKey,
) -> Result<()> {
	if key.keyspace() == Some(KeyspaceId::JOIN_ROW_EXPIRY) {
		return classify_durable_join_expiry(txn, id, key, scoped);
	}
	Ok(())
}

#[inline]
fn classify_durable_join_expiry<T: FlowTransaction>(
	txn: &mut T,
	id: OperatorId,
	key: &GroupStateKey,
	scoped: &EncodedKey,
) -> Result<()> {
	if txn.is_classified(scoped) {
		return Ok(());
	}
	let Some((group, side, row_number)) =
		OperatorStateKey::decode_inner(key.as_slice()).and_then(|(group, _, suffix)| {
			decode_join_expiry_suffix(&suffix).map(|(side, row)| (group, side, row))
		})
	else {
		return Ok(());
	};
	let present = txn.operator_store().join_expiry_get(id, group, side, row_number).is_some();
	txn.classify(scoped, present.then_some(JOIN_EXPIRY_VALUE_BYTES));
	Ok(())
}

#[inline]
#[instrument(name = "flow::state::clear::scan", level = "trace", skip(txn), fields(operator_id = id.0))]
fn scan_keys_for_clear<T: FlowTransaction>(txn: &mut T, id: OperatorId) -> Result<Vec<(EncodedKey, Option<ByteSize>)>> {
	let range = OperatorStateKey::node_range(id);
	let iter = txn.range(range, RangeScope::All, 1024);
	let mut keys = Vec::new();
	for result in iter {
		let multi = result?;
		keys.push((multi.key, ByteSize::from_bytes(multi.bytes.len() as u64)));
	}
	Ok(keys.into_iter()
		.map(|(key, pre)| {
			let durable = !txn.pending_layers().top().contains_key(&key);
			(key, durable.then_some(pre))
		})
		.collect())
}

#[inline]
#[instrument(name = "flow::state::clear::remove", level = "trace", skip(txn, keys), fields(count = keys.len()))]
fn remove_keys<T: FlowTransaction>(txn: &mut T, keys: Vec<(EncodedKey, Option<ByteSize>)>) -> Result<()> {
	for (key, pre) in keys {
		if let Some(pre) = pre {
			txn.classify(&key, Some(pre));
		}
		txn.remove(&key)?;
	}
	Ok(())
}
