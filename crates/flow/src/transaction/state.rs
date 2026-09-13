// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, collections::BTreeMap, ops::Bound};

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
	key::{
		any::TaggedKey,
		operator::state::{
			GroupId, GroupStateKey, OperatorStateKey, group_inner_range, group_inner_range_split,
			keyspace_inner_range_split, node_prefix,
		},
	},
	metrics::scan::ScanCounters,
	state::timer::sweep_order,
};
use reifydb_store_operator::store::state::StateLastIter;
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{Result, byte_size::ByteSize};
use tracing::{Span, field, instrument};

use crate::transaction::{FlowTransaction, read::flow_merge_pending_iterator, scope::scoped_key};

const PENDING_LAST_PAGE: usize = 64;

pub(crate) fn encode_payload<T: OperatorState>(value: &T) -> Result<EncodedPodRow> {
	Ok(value.encode_state()?)
}

pub(crate) fn decode_payload<T: OperatorState>(row: &EncodedPodRow) -> Result<T> {
	Ok(decode(row)?)
}

const MAX_STATE_PAGE: usize = 1024;

#[derive(Debug, Clone)]
pub struct StateRange {
	pub range: EncodedKeyRange,
	pub limit: Option<usize>,
	pub page: Option<usize>,
	pub site: &'static str,
}

impl StateRange {
	pub fn forward(range: EncodedKeyRange, site: &'static str) -> Self {
		Self {
			range,
			limit: None,
			page: None,
			site,
		}
	}

	pub fn limit(mut self, limit: usize) -> Self {
		self.limit = Some(limit);
		self
	}

	pub fn page(mut self, page: usize) -> Self {
		self.page = Some(page);
		self
	}

	pub fn full_page(self) -> Self {
		self.page(MAX_STATE_PAGE)
	}
}

pub trait StateExtension: FlowTransaction {
	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len()
	))]
	fn state_get(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		let scoped = scoped_key(id, key);
		Ok(self.get(&scoped)?.map(EncodedPodRow::from))
	}

	#[instrument(name = "flow::state::get_many", level = "debug", skip(self, keys), fields(
		operator_id = id.0,
		key_count = keys.len()
	))]
	fn state_get_many(&mut self, id: OperatorId, keys: &[GroupStateKey]) -> Result<MultiVersionBatch<TaggedKey>> {
		let version = self.version();
		let mut items: Vec<MultiVersionRow<TaggedKey>> = Vec::with_capacity(keys.len());
		let mut to_batch: Vec<EncodedKey> = Vec::new();

		for key in keys {
			let encoded_key = scoped_key(id, key);
			match self.lookup_overlays(&encoded_key) {
				Some(None) => continue,
				Some(Some(bytes)) => items.push(MultiVersionRow {
					key: TaggedKey::decode(&encoded_key).expect(UNDECODABLE_STATE_KEY),
					bytes,
					version,
				}),
				None => to_batch.push(encoded_key),
			}
		}

		self.fetch_state_external(to_batch, &mut items)?;

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
		self.set(&scoped, row.into_bytes())
	}

	#[instrument(name = "flow::state::remove", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len()
	))]
	fn state_remove(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<()> {
		let scoped = scoped_key(id, key);
		self.remove_silent(&scoped)
	}

	#[instrument(name = "flow::state::scan", level = "debug", skip(self), fields(
		operator_id = id.0
	))]
	fn state_scan_all(&mut self, id: OperatorId) -> Result<MultiVersionBatch<TaggedKey>> {
		let range = OperatorStateKey::node_range(id).encode();
		let iter = self.range(range, RangeScope::All, 1024);
		let mut items = Vec::new();
		for result in iter {
			items.push(result?);
		}
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
	fn state_range(&mut self, id: OperatorId, query: StateRange) -> Result<MultiVersionBatch<TaggedKey>> {
		debug_assert!(
			keyspace_inner_range_split(&query.range).is_some()
				|| group_inner_range_split(&query.range).is_some(),
			"a state range must stay inside one group; {} passed a range spanning more than one",
			query.site
		);
		let before = ScanCounters::sample();
		let prefixed_range = query.range.with_prefix(EncodedKey::new(node_prefix(id)));
		let batch_size = match query.page {
			Some(page) => page.clamp(1, MAX_STATE_PAGE),
			None => query.limit.map_or(MAX_STATE_PAGE, |limit| limit.saturating_add(1).min(MAX_STATE_PAGE)),
		};
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

	#[instrument(name = "flow::state::any_live", level = "debug", skip(self, range), fields(
		operator_id = id.0,
		site = site
	))]
	fn state_any_live(&mut self, id: OperatorId, range: EncodedKeyRange, site: &'static str) -> Result<bool> {
		let query = StateRange::forward(range, site).limit(1).full_page();
		Ok(!self.state_range(id, query)?.items.is_empty())
	}

	#[instrument(name = "flow::state::group_range", level = "debug", skip(self, groups), fields(
		operator_id = id.0,
		groups = groups.len()
	))]
	fn state_group_range(
		&mut self,
		id: OperatorId,
		groups: &[GroupId],
		limit: usize,
	) -> Result<MultiVersionBatch<TaggedKey>> {
		let ordered = sweep_order(groups);
		let prefix = EncodedKey::new(node_prefix(id));
		let mut merged = BTreeMap::new();
		for group in &ordered {
			let range = group_inner_range(*group).with_prefix(prefix.clone());
			self.pending_layers().collect_range((range.start.as_ref(), range.end.as_ref()), &mut merged);
		}
		let pending: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().collect();
		let version = self.version();
		let batch = self.operator_store().group_page(id, &ordered, limit.saturating_add(1) as u64)?;
		let truncated = batch.has_more;
		let stored: Vec<Result<MultiVersionRow<TaggedKey>>> = batch
			.items
			.into_iter()
			.map(|(inner, row)| {
				let (group, keyspace, suffix) = OperatorStateKey::decode_inner(inner.as_slice())
					.expect("inner keys must carry a structured encoding");
				Ok(MultiVersionRow {
					key: OperatorStateKey::new(id, group, keyspace, suffix).into(),
					bytes: row.into_bytes(),
					version,
				})
			})
			.collect();

		let mut items = Vec::new();
		let mut has_more = truncated;
		for result in flow_merge_pending_iterator(pending, stored.into_iter(), version) {
			if items.len() == limit {
				has_more = true;
				break;
			}
			items.push(result?);
		}
		Ok(MultiVersionBatch {
			items,
			has_more,
		})
	}

	#[instrument(name = "flow::state::last", level = "debug", skip(self, range), fields(
		operator_id = id.0
	))]
	fn state_last(&mut self, id: OperatorId, range: EncodedKeyRange) -> Result<Option<MultiVersionRow<TaggedKey>>> {
		let prefix = node_prefix(id);
		let prefixed_range = range.with_prefix(EncodedKey::new(prefix.clone()));

		let version = self.version();
		let store = self.operator_store();
		let mut scan = store.state_last_iter(id, range);
		let mut stored = next_stored(&mut scan, &prefix)?;

		let mut pending: Vec<(EncodedKey, PendingWrite)> = Vec::new();
		let mut pending_end = prefixed_range.end.clone();
		let mut pending_done = false;
		let mut index = 0usize;

		let found = loop {
			if index == pending.len() && !pending_done {
				let mut merged = BTreeMap::new();
				self.pending_layers().collect_range_back(
					(prefixed_range.start.as_ref(), pending_end.as_ref()),
					PENDING_LAST_PAGE,
					&mut merged,
				);
				pending_done = merged.len() < PENDING_LAST_PAGE;
				pending = merged.into_iter().rev().collect();
				index = 0;
				if let Some((key, _)) = pending.last() {
					pending_end = Bound::Excluded(key.clone());
				}
				continue;
			}
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
							stored = next_stored(&mut scan, &prefix)?;
						}
					}
				}
			}
		};
		Ok(found.map(|row| MultiVersionRow {
			key: TaggedKey::decode(&row.key).expect(UNDECODABLE_STATE_KEY),
			bytes: row.bytes,
			version: row.version,
		}))
	}

	#[instrument(name = "flow::state::clear", level = "trace", skip(self), fields(
		operator_id = id.0
	))]
	fn state_clear(&mut self, id: OperatorId) -> Result<()> {
		let keys_to_remove = scan_keys_for_clear(self, id)?;

		remove_keys(self, keys_to_remove)?;
		Ok(())
	}
}

impl<T: FlowTransaction> StateExtension for T {}

const UNDECODABLE_STATE_KEY: &str = "a scoped operator-state key must decode";

fn next_stored(scan: &mut StateLastIter<'_>, prefix: &[u8]) -> Result<Option<(EncodedKey, EncodedKey, EncodedBytes)>> {
	scan.next()
		.transpose()
		.map(|entry| {
			entry.map(|(inner, row)| {
				let mut scoped = Vec::with_capacity(prefix.len() + inner.len());
				scoped.extend_from_slice(prefix);
				scoped.extend_from_slice(inner.as_slice());
				(inner, EncodedKey::new(scoped), row.into_bytes())
			})
		})
		.map_err(Into::into)
}

#[inline]
#[instrument(name = "flow::state::clear::scan", level = "trace", skip(txn), fields(operator_id = id.0))]
fn scan_keys_for_clear<T: FlowTransaction>(txn: &mut T, id: OperatorId) -> Result<Vec<(EncodedKey, Option<ByteSize>)>> {
	let range = OperatorStateKey::node_range(id).encode();
	let iter = txn.range(range, RangeScope::All, 1024);
	let mut keys = Vec::new();
	for result in iter {
		let multi = result?;
		keys.push((multi.key.encode(), ByteSize::from_bytes(multi.bytes.len() as u64)));
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
