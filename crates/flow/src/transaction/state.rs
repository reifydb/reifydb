// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		store::{MultiVersionBatch, MultiVersionRow},
	},
	key::operator_state::{GroupStateKey, OperatorStateKey, node_prefix},
	metrics::scan::ScanCounters,
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::Result;
use tracing::{Span, field, instrument};

use crate::transaction::{FlowTransaction, scope::scoped_key};

#[derive(Debug, Clone)]
pub struct StateRange {
	pub range: EncodedKeyRange,
	pub limit: Option<usize>,
	pub site: &'static str,
	pub reverse: bool,
}

impl StateRange {
	pub fn forward(range: EncodedKeyRange, site: &'static str) -> Self {
		Self {
			range,
			limit: None,
			site,
			reverse: false,
		}
	}

	pub fn reverse(range: EncodedKeyRange, site: &'static str) -> Self {
		Self {
			range,
			limit: None,
			site,
			reverse: true,
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
		reverse = query.reverse,
		rows_fetched = field::Empty,
		rows_tombstoned = field::Empty
	))]
	fn state_range(&mut self, id: OperatorId, query: StateRange) -> Result<MultiVersionBatch> {
		let before = ScanCounters::sample();
		let prefixed_range = query.range.with_prefix(EncodedKey::new(node_prefix(id)));
		let batch_size = query.limit.map_or(1024, |limit| limit.saturating_add(1).min(1024));
		let iter = if query.reverse {
			self.range_rev(prefixed_range, RangeScope::All, batch_size)
		} else {
			self.range(prefixed_range, RangeScope::All, batch_size)
		};
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

#[inline]
#[instrument(name = "flow::state::clear::scan", level = "trace", skip(txn), fields(operator_id = id.0))]
fn scan_keys_for_clear<T: FlowTransaction>(txn: &mut T, id: OperatorId) -> Result<Vec<EncodedKey>> {
	let range = OperatorStateKey::node_range(id);
	let iter = txn.range(range, RangeScope::All, 1024);
	let mut keys = Vec::new();
	for result in iter {
		let multi = result?;
		keys.push(multi.key);
	}
	Ok(keys)
}

#[inline]
#[instrument(name = "flow::state::clear::remove", level = "trace", skip(txn, keys), fields(count = keys.len()))]
fn remove_keys<T: FlowTransaction>(txn: &mut T, keys: Vec<EncodedKey>) -> Result<()> {
	for key in keys {
		txn.remove(&key)?;
	}
	Ok(())
}
