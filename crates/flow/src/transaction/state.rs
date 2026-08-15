// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
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
use reifydb_value::{Result, error::Error as ValueError};
use tracing::{Span, field, instrument};

use crate::transaction::{FlowTransaction, memo::StateMemo, scope::scoped_key};

pub trait StateExtension: FlowTransaction {
	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len(),
		found = field::Empty
	))]
	fn state_get(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
		let scoped = scoped_key(id, key);
		let memoized = key.keyspace().is_some_and(StateMemo::cacheable);
		if memoized
			&& let Some(cached) = self.substrate().memo.lookup(&scoped)
		{
			Span::current().record("found", cached.is_some());
			return Ok(cached);
		}
		let result = match self.get(&scoped)? {
			Some(bytes) => Some(EncodedOperatorRow::try_from(bytes).map_err(ValueError::from)?),
			None => None,
		};
		if memoized {
			self.substrate().memo.remember(&scoped, result.clone());
		}
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
		let encoded: Vec<EncodedKey> = keys.iter().map(|key| scoped_key(id, key)).collect();
		let memoized: Vec<bool> = keys.iter().map(|key| key.keyspace().is_some_and(StateMemo::cacheable)).collect();

		let mut items: Vec<MultiVersionRow> = Vec::new();
		let mut to_batch: Vec<EncodedKey> = Vec::new();
		let mut to_memoize: Vec<EncodedKey> = Vec::new();

		for (slot, encoded_key) in encoded.iter().enumerate() {
			if memoized[slot]
				&& let Some(cached) = self.substrate().memo.lookup(encoded_key)
			{
				if let Some(row) = cached {
					items.push(MultiVersionRow {
						key: encoded_key.clone(),
						bytes: row.into_bytes(),
						version,
					});
				}
				continue;
			}
			match self.lookup_overlays(encoded_key) {
				Some(None) => continue,
				Some(Some(bytes)) => items.push(MultiVersionRow {
					key: encoded_key.clone(),
					bytes,
					version,
				}),
				None => {
					to_batch.push(encoded_key.clone());
					if memoized[slot] {
						to_memoize.push(encoded_key.clone());
					}
				}
			}
		}

		let fetched_from = items.len();
		self.fetch_state_external(&to_batch, &mut items)?;

		if !to_memoize.is_empty() {
			let memo = self.substrate().memo.clone();
			for fetched in &items[fetched_from..] {
				if let Some(position) = to_memoize.iter().position(|key| key == &fetched.key) {
					let row = EncodedOperatorRow::try_from(fetched.bytes.clone())
						.map_err(ValueError::from)?;
					memo.remember(&to_memoize.swap_remove(position), Some(row));
				}
			}
			for absent in &to_memoize {
				memo.remember(absent, None);
			}
		}

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
	fn state_set(&mut self, id: OperatorId, key: &GroupStateKey, row: EncodedOperatorRow) -> Result<()> {
		let scoped = scoped_key(id, key);
		if key.keyspace().is_some_and(StateMemo::cacheable) {
			self.substrate().memo.invalidate(&scoped);
		}
		self.set(&scoped, row.into_bytes())
	}

	#[instrument(name = "flow::state::remove", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len()
	))]
	fn state_remove(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<()> {
		let scoped = scoped_key(id, key);
		if key.keyspace().is_some_and(StateMemo::cacheable) {
			self.substrate().memo.invalidate(&scoped);
		}
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

	#[instrument(name = "flow::state::range", level = "debug", skip(self, range), fields(
		operator_id = id.0
	))]
	fn state_range_all(&mut self, id: OperatorId, range: EncodedKeyRange) -> Result<MultiVersionBatch> {
		let prefixed_range = range.with_prefix(EncodedKey::new(node_prefix(id)));
		let iter = self.range(prefixed_range, RangeScope::All, 1024);
		let mut items = Vec::new();
		for result in iter {
			items.push(result?);
		}
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[instrument(name = "flow::state::range_limited", level = "debug", skip(self, range), fields(
		operator_id = id.0,
		site = site,
		rows_fetched = field::Empty,
		rows_tombstoned = field::Empty
	))]
	fn state_range(
		&mut self,
		id: OperatorId,
		range: EncodedKeyRange,
		limit: Option<usize>,
		site: &'static str,
	) -> Result<MultiVersionBatch> {
		let before = ScanCounters::sample();
		let prefixed_range = range.with_prefix(EncodedKey::new(node_prefix(id)));
		let iter = self.range(prefixed_range, RangeScope::All, 1024);
		let mut items = Vec::new();
		let mut has_more = false;
		for result in iter {
			if limit.is_some_and(|l| items.len() == l) {
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

	#[instrument(name = "flow::state::range_rev", level = "debug", skip(self, range), fields(
		operator_id = id.0,
		site = site
	))]
	fn state_range_rev(
		&mut self,
		id: OperatorId,
		range: EncodedKeyRange,
		limit: Option<usize>,
		site: &'static str,
	) -> Result<MultiVersionBatch> {
		let prefixed_range = range.with_prefix(EncodedKey::new(node_prefix(id)));
		let iter = self.range_rev(prefixed_range, RangeScope::All, 1024);
		let mut items = Vec::new();
		let mut has_more = false;
		for result in iter {
			if limit.is_some_and(|l| items.len() == l) {
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

	#[instrument(name = "flow::state::clear", level = "trace", skip(self), fields(
		operator_id = id.0,
		keys_removed = field::Empty
	))]
	fn state_clear(&mut self, id: OperatorId) -> Result<()> {
		self.substrate().memo.clear();
		let keys_to_remove = self.scan_keys_for_clear(id)?;

		let count = keys_to_remove.len();
		self.remove_keys(keys_to_remove)?;

		Span::current().record("keys_removed", count);
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::state::clear::scan", level = "trace", skip(self), fields(operator_id = id.0))]
	fn scan_keys_for_clear(&mut self, id: OperatorId) -> Result<Vec<EncodedKey>> {
		let range = OperatorStateKey::node_range(id);
		let iter = self.range(range, RangeScope::All, 1024);
		let mut keys = Vec::new();
		for result in iter {
			let multi = result?;
			keys.push(multi.key);
		}
		Ok(keys)
	}

	#[inline]
	#[instrument(name = "flow::state::clear::remove", level = "trace", skip(self, keys), fields(count = keys.len()))]
	fn remove_keys(&mut self, keys: Vec<EncodedKey>) -> Result<()> {
		for key in keys {
			self.remove(&key)?;
		}
		Ok(())
	}
}

impl<T: FlowTransaction> StateExtension for T {}
