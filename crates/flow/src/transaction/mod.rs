// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use postcard::from_bytes;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::{
	actors::pending::{Pending, PendingLayers, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::{dictionary::Dictionary, flow::OperatorId, object::ObjectId},
		change::{Change, ChangeOrigin, Diff},
		store::{MultiVersionBatch, MultiVersionRow},
	},
	key::{
		EncodableKey,
		operator_state::{
			GroupId, GroupSet, GroupStateKey, OperatorStateKey, group_identity_inner_range, node_prefix,
		},
	},
	metrics::scan::ScanCounters,
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_operator::store::OperatorStore;
use reifydb_transaction::{
	change_accumulator::ChangeAccumulator,
	dictionary::DictionaryAllocatorRegistry,
	interceptor::interceptors::Interceptors,
	multi::{RangeScope, transaction::read::MultiReadTransaction},
};
use reifydb_value::{
	Result,
	error::Error as ValueError,
	reifydb_assertions,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		row_number::RowNumber,
	},
};
use tracing::{Span, field, instrument};

pub mod deferred;
pub mod dictionary;
pub mod ephemeral;
pub mod frontier;
pub mod group;
pub mod read;
pub mod reclaim;
pub mod row_number;
pub mod state;
pub mod substrate;
pub mod timer;
pub mod watermark;
pub mod write;

use crate::{
	timer::Timer,
	transaction::{
		group::GroupInterner,
		read::{flow_merge_pending_iterator, flow_merge_pending_iterator_rev},
		reclaim::ReclaimOutcome,
		row_number::RowNumberProvider,
		state::scoped_key,
		substrate::FlowSubstrate,
		timer::TimerWheel,
		watermark::SourceWatermarks,
	},
};

#[derive(Clone, Copy)]
pub struct ChangeCoordinate {
	pub at: Option<DateTime>,
	pub version: CommitVersion,
}

pub struct DeferredParams {
	pub version: CommitVersion,
	pub pending: PendingLayers,
	pub query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub clock: Clock,

	pub substrate: FlowSubstrate,
}

pub trait FlowTransaction: Sized + Send + 'static {
	fn version(&self) -> CommitVersion;

	fn clock(&self) -> &Clock;

	fn catalog(&self) -> &Catalog;

	fn query(&self) -> MultiReadTransaction;

	fn substrate(&self) -> &FlowSubstrate;

	fn pending_layers(&self) -> &PendingLayers;

	fn pending_layers_mut(&mut self) -> &mut PendingLayers;

	fn accumulator_mut(&mut self) -> &mut ChangeAccumulator;

	fn change_coordinate(&self) -> Option<ChangeCoordinate>;

	fn set_change_coordinate(&mut self, coordinate: ChangeCoordinate);

	fn flow_watermark(&self) -> Option<DateTime>;

	fn set_flow_watermark(&mut self, watermark: DateTime);

	fn storage_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>>;

	fn storage_contains(&mut self, key: &EncodedKey) -> Result<bool>;

	fn storage_range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>;

	fn storage_range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>;

	fn fetch_state_external(&mut self, keys: &[EncodedKey], items: &mut Vec<MultiVersionRow>) -> Result<()>;

	fn pending(&self) -> &Pending {
		self.pending_layers().top()
	}

	fn take_pending(&mut self) -> Pending {
		self.pending_layers_mut().take_top()
	}

	fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		if self.pending_layers().is_removed(key) {
			return Ok(None);
		}
		if let Some(value) = self.pending_layers().get(key) {
			return Ok(Some(value.clone()));
		}
		self.storage_get(key)
	}

	fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		if self.pending_layers().is_removed(key) {
			return Ok(false);
		}
		if self.pending_layers().get(key).is_some() {
			return Ok(true);
		}
		self.storage_contains(key)
	}

	fn range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let mut merged = BTreeMap::new();
		self.pending_layers().collect_range((range.start.as_ref(), range.end.as_ref()), &mut merged);
		let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().collect();
		let version = self.version();
		let storage_iter = self.storage_range(range, scope, batch_size);
		Box::new(flow_merge_pending_iterator(pending_vec, storage_iter, version))
	}

	fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let mut merged = BTreeMap::new();
		self.pending_layers().collect_range((range.start.as_ref(), range.end.as_ref()), &mut merged);
		let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().rev().collect();
		let version = self.version();
		let storage_iter = self.storage_range_rev(range, scope, batch_size);
		Box::new(flow_merge_pending_iterator_rev(pending_vec, storage_iter, version))
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let range = EncodedKeyRange::prefix(prefix);
		let items = self.range(range, RangeScope::All, 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	fn set(&mut self, key: &EncodedKey, value: impl Into<EncodedBytes>) -> Result<()> {
		self.pending_layers_mut().insert(key.clone(), value.into());
		Ok(())
	}

	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.pending_layers_mut().remove(key.clone());
		Ok(())
	}

	fn remove_silent(&mut self, key: &EncodedKey) -> Result<()> {
		self.pending_layers_mut().remove_silent(key.clone());
		Ok(())
	}

	fn set_batch(&mut self, keys: &[EncodedKey], values: &[EncodedBytes]) -> Result<()> {
		self.pending_layers_mut().insert_batch(keys, values);
		Ok(())
	}

	fn remove_batch(&mut self, keys: &[EncodedKey]) -> Result<()> {
		self.pending_layers_mut().remove_batch(keys);
		Ok(())
	}

	#[inline]
	fn lookup_overlays(&self, key: &EncodedKey) -> Option<Option<EncodedBytes>> {
		let pending = self.pending_layers();
		if pending.is_removed(key) {
			return Some(None);
		}
		pending.get(key).map(|row| Some(row.clone()))
	}

	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len(),
		found = field::Empty
	))]
	fn state_get(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
		let result = match self.get(&scoped_key(id, key))? {
			Some(bytes) => Some(EncodedOperatorRow::try_from(bytes).map_err(ValueError::from)?),
			None => None,
		};
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

		let mut items: Vec<MultiVersionRow> = Vec::new();
		let mut to_batch: Vec<EncodedKey> = Vec::new();

		for encoded_key in &encoded {
			match self.lookup_overlays(encoded_key) {
				Some(None) => continue,
				Some(Some(bytes)) => items.push(MultiVersionRow {
					key: encoded_key.clone(),
					bytes,
					version,
				}),
				None => to_batch.push(encoded_key.clone()),
			}
		}

		self.fetch_state_external(&to_batch, &mut items)?;

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
		self.set(&scoped_key(id, key), row.into_bytes())
	}

	#[instrument(name = "flow::state::remove", level = "trace", skip(self), fields(
		operator_id = id.0,
		key_len = key.as_slice().len()
	))]
	fn state_remove(&mut self, id: OperatorId, key: &GroupStateKey) -> Result<()> {
		self.remove_silent(&scoped_key(id, key))
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

	#[instrument(name = "flow::state::clear", level = "trace", skip(self), fields(
		operator_id = id.0,
		keys_removed = field::Empty
	))]
	fn state_clear(&mut self, id: OperatorId) -> Result<()> {
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

	fn row_numbers(&self) -> RowNumberProvider {
		self.substrate().row.clone()
	}

	fn group_interner(&self) -> GroupInterner {
		self.substrate().group.clone()
	}

	fn dictionary_allocators(&self) -> DictionaryAllocatorRegistry {
		self.substrate().dictionary.clone()
	}

	fn source_watermarks(&self) -> SourceWatermarks {
		self.substrate().watermarks.clone()
	}

	fn timer_wheel(&self) -> TimerWheel {
		self.substrate().timers.clone()
	}

	fn operator_store(&self) -> OperatorStore {
		self.substrate().operators.clone()
	}

	fn arm_timer(&mut self, operator: OperatorId, timer: &Timer) -> Result<()> {
		self.timer_wheel().arm(operator, self, timer)
	}

	fn disarm_timer(&mut self, operator: OperatorId, timer: &Timer) -> Result<()> {
		self.timer_wheel().disarm(operator, self, timer)
	}

	fn written_at(&self) -> DateTime {
		match self.change_coordinate().and_then(|coordinate| coordinate.at) {
			Some(at) => at,
			None => self.clock().now(),
		}
	}

	fn track_flow_change(&mut self, change: Change) {
		if let ChangeOrigin::Object(id) = change.origin {
			let accumulator = self.accumulator_mut();
			for diff in change.diffs {
				accumulator.track(id, diff);
			}
		}
	}

	fn take_accumulator_entries(&mut self) -> Vec<(ObjectId, Diff)> {
		let acc = self.accumulator_mut();
		let entries: Vec<_> = acc.entries_from(0).to_vec();
		acc.clear();
		entries
	}

	fn intern_group(&mut self, operator: OperatorId, group: &EncodedKey) -> Result<(GroupId, bool)> {
		let interner = self.group_interner();
		let (id, is_new) = interner.intern(operator, self, group)?;
		if is_new {
			self.row_numbers().mark_fresh(operator, id);
		}
		Ok((id, is_new))
	}

	fn intern_groups(&mut self, operator: OperatorId, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		let interner = self.group_interner();
		let results = interner.intern_many(operator, self, groups)?;
		let provider = self.row_numbers();
		for (id, is_new) in &results {
			if *is_new {
				provider.mark_fresh(operator, *id);
			}
		}
		Ok(results)
	}

	fn lookup_group(&mut self, operator: OperatorId, group: &EncodedKey) -> Result<Option<GroupId>> {
		let interner = self.group_interner();
		interner.lookup(operator, self, group)
	}

	fn forget_group(&mut self, operator: OperatorId, group: &EncodedKey) -> Result<bool> {
		let interner = self.group_interner();
		interner.forget(operator, self, group)
	}

	fn group_bytes(&mut self, operator: OperatorId, id: GroupId) -> Result<Option<EncodedKey>> {
		let interner = self.group_interner();
		interner.group_bytes(operator, self, id)
	}

	fn get_or_create_row_number(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		let provider = self.row_numbers();
		provider.get_or_create_row_number(operator, group, self, key)
	}

	fn get_or_create_row_numbers(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		let provider = self.row_numbers();
		provider.get_or_create_row_numbers(operator, group, self, keys)
	}

	fn get_row_number(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		key: &EncodedKey,
	) -> Result<Option<RowNumber>> {
		let provider = self.row_numbers();
		provider.get_row_number(operator, group, self, key)
	}

	fn get_row_numbers(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<Option<RowNumber>>> {
		let provider = self.row_numbers();
		provider.get_row_numbers(operator, group, self, keys)
	}

	fn remove_row_number(&mut self, operator: OperatorId, group: GroupId, key: &EncodedKey) -> Result<bool> {
		let provider = self.row_numbers();
		provider.remove_row_number(operator, group, self, key)
	}

	fn remove_row_numbers_below(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		upper: &EncodedKey,
	) -> Result<Vec<RowNumber>> {
		let provider = self.row_numbers();
		provider.drop_below(operator, group, self, upper)
	}

	fn remove_row_numbers_by_prefix(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		key_prefix: &[u8],
	) -> Result<()> {
		let provider = self.row_numbers();
		provider.remove_by_prefix(operator, group, self, key_prefix)
	}

	fn invalidate_row_number_groups(&mut self, operator: OperatorId, groups: &GroupSet) {
		let provider = self.row_numbers();
		provider.invalidate_groups(operator, groups)
	}

	fn find_dictionary(&self, id: DictionaryId) -> Option<Dictionary> {
		self.catalog().cache().find_dictionary_at(id, self.version())
	}

	fn find_dictionary_by_name(&self, name: &str) -> Option<Dictionary> {
		let version = self.version();
		let (namespace_name, dictionary_name) = name.rsplit_once("::")?;
		let namespace = self.catalog().cache().find_namespace_by_name_at(namespace_name, version)?;
		self.catalog().cache().find_dictionary_by_name_at(namespace.id(), dictionary_name, version)
	}

	#[instrument(name = "flow::dictionary::find", level = "trace", skip(self, dictionary, value), fields(dictionary_id = dictionary.id.0))]
	fn find_in_dictionary(&mut self, dictionary: &Dictionary, value: &Value) -> Result<Option<DictionaryEntryId>> {
		self.dictionary_allocators().find(dictionary, value)
	}

	#[instrument(name = "flow::dictionary::resolve", level = "trace", skip(self, dictionary, id), fields(dictionary_id = dictionary.id.0))]
	fn get_from_dictionary(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		match self.dictionary_allocators().get(dictionary, id.to_u128())? {
			Some(bytes) => Ok(Some(from_bytes(&bytes).expect("failed to deserialize dictionary value"))),
			None => Ok(None),
		}
	}

	fn reclaim_group_identity(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		reifydb_assertions! {
			assert!(
				!group.is_root(),
				"group id 0 is the root group; reclaiming its identity would delete the interning dictionary itself"
			);
		}
		if group.is_root() {
			return Ok(ReclaimOutcome::NOTHING);
		}
		let group_bytes = self.group_bytes(operator, group)?;
		let outcome = self.reclaim_range(operator, group_identity_inner_range(group), limit)?;
		if !outcome.more
			&& let Some(bytes) = group_bytes
		{
			self.forget_group(operator, &bytes)?;
		}
		Ok(outcome)
	}

	fn reclaim_range(
		&mut self,
		operator: OperatorId,
		range: EncodedKeyRange,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		if limit == 0 {
			return Ok(ReclaimOutcome::NOTHING);
		}
		let batch = self.state_range(operator, range, Some(limit), "reclaim::range")?;
		let keys: Vec<GroupStateKey> = batch
			.items
			.iter()
			.map(|item| {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_range must return OperatorState keys");
				GroupStateKey::from_framed(decoded.inner())
					.expect("operator state rows carry a framed inner key")
			})
			.collect();
		let removed = keys.len();
		for key in &keys {
			self.state_remove(operator, key)?;
		}
		Ok(ReclaimOutcome {
			removed,
			more: batch.has_more,
		})
	}
}
