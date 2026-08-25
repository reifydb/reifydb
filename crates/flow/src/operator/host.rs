// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, pod::EncodedPodRow},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		flow::OperatorId,
	},
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey, node_prefix},
	},
	state::timer::{StateStore, TimerKind, TimerStore},
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		row_number::RowNumber,
		value_type::ValueType,
	},
};

use crate::{
	operator::state::{iter::StateIterator, reaper::IdentityReclaim, reclaim::ReclaimOutcome},
	timer::{Timer, extension::TimerExtension},
	transaction::{
		FlowTransaction,
		anchor::{SealAnchorExtension, SealPage, anchor_key},
		dictionary::DictionaryExtension,
		group::GroupExtension,
		reclaim::ReclaimExtension,
		row_number::RowNumberExtension,
		state::{StateExtension, StateRange},
	},
};

pub trait HostContext: StateStore + TimerStore + IdentityReclaim {
	fn version(&self) -> CommitVersion;

	fn disarm_timer_by_key(&mut self, kind: TimerKind, key: &EncodedKey) -> Result<()>;

	fn anchor_at(&mut self, group: GroupId, side: u8, row_number: RowNumber) -> Result<Option<DateTime>>;

	fn anchor_min(&mut self, group: GroupId) -> Result<Option<DateTime>>;

	fn anchor_seal_page(&mut self, group: GroupId, at: DateTime, budget: usize) -> Result<SealPage>;

	fn clear_anchors(&mut self, group: GroupId, budget: usize) -> Result<()> {
		loop {
			let page = self.anchor_seal_page(group, DateTime::MAX, budget)?;
			if page.due.is_empty() {
				return Ok(());
			}
			for (side, row_number) in &page.due {
				self.state_remove(&anchor_key(group, *side, *row_number))?;
			}
			if !page.more {
				return Ok(());
			}
		}
	}

	fn config_uint8(&self, key: ConfigKey) -> u64;

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, EncodedPodRow)>>;

	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedPodRow)>>;

	fn state_range_limited(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>>;

	fn state_range_iter(&mut self, range: EncodedKeyRange) -> StateIterator<'_>;

	fn state_clear(&mut self) -> Result<()>;

	fn state_scan_all(&mut self) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
		self.state_range_iter(EncodedKeyRange::all()).collect()
	}

	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome>;

	fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>>;

	fn get_row_numbers_for_groups(
		&mut self,
		groups: &[GroupId],
		key: &EncodedKey,
	) -> Result<Vec<Option<RowNumber>>>;

	fn get_or_create_row_numbers_for_groups(
		&mut self,
		groups: &[GroupId],
		key: &EncodedKey,
	) -> Result<Vec<(RowNumber, bool)>>;

	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>>;

	fn remove_row_numbers_by_prefix(&mut self, group: GroupId, key_prefix: &[u8]) -> Result<()>;

	fn dictionary_id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>>;

	fn dictionary_value_type(&mut self, dictionary: DictionaryId) -> Option<ValueType>;

	fn dictionary_id_type(&mut self, dictionary: DictionaryId) -> Option<ValueType>;

	fn dictionary_find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>>;

	fn dictionary_get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>>;
}

pub struct TxnHostContext<'a, T: FlowTransaction> {
	txn: &'a mut T,
	operator: OperatorId,
	now: DateTime,
}

impl<'a, T: FlowTransaction> TxnHostContext<'a, T> {
	pub fn new(txn: &'a mut T, operator: OperatorId) -> Self {
		let now = txn.written_at();
		Self {
			txn,
			operator,
			now,
		}
	}
}

impl<T: FlowTransaction> TimerStore for TxnHostContext<'_, T> {
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.arm_timer(
			self.operator,
			&Timer {
				due,
				kind,
				key: key.clone(),
			},
		)
	}

	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.disarm_timer(
			self.operator,
			&Timer {
				due,
				kind,
				key: key.clone(),
			},
		)
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		Ok(self.txn.flow_watermark())
	}
}

impl<T: FlowTransaction> StateStore for TxnHostContext<'_, T> {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		self.txn.state_get(self.operator, key)
	}

	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.state_get_many(self.operator, keys)?;
		for r in batch.items {
			let Some(decoded) = OperatorStateKey::decode(&r.key) else {
				continue;
			};
			let Some(inner) = GroupStateKey::from_framed(decoded.inner()) else {
				continue;
			};
			visit(inner, EncodedPodRow::from(r.bytes))?;
		}
		Ok(())
	}

	fn state_classify(&mut self, key: &GroupStateKey, pre: Option<ByteSize>) {
		self.txn.state_classify(self.operator, key, pre);
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		self.txn.state_set(self.operator, key, payload)
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.txn.state_remove(self.operator, key)
	}

	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.state_range(
			self.operator,
			StateRange {
				range,
				limit,
				site: "operator::host_visit",
				reverse: false,
			},
		)?;
		for r in batch.items {
			if let Some(decoded) = OperatorStateKey::decode(&r.key)
				&& let Some(inner) = GroupStateKey::from_framed(decoded.inner())
			{
				visit(inner, EncodedPodRow::from(r.bytes))?;
			}
		}
		Ok(())
	}

	fn state_last(&mut self, range: EncodedKeyRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
		let batch = self
			.txn
			.state_range(self.operator, StateRange::reverse(range, "operator::host_last").limit(1))?;
		for r in batch.items {
			if let Some(decoded) = OperatorStateKey::decode(&r.key)
				&& let Some(inner) = GroupStateKey::from_framed(decoded.inner())
			{
				return Ok(Some((inner, EncodedPodRow::from(r.bytes))));
			}
		}
		Ok(None)
	}

	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		self.txn.intern_groups(self.operator, groups)
	}

	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		self.txn.lookup_groups(self.operator, groups)
	}

	fn intern_groups_in(&mut self, keyspace: Keyspace, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		self.txn.intern_groups_in(self.operator, keyspace, groups)
	}

	fn lookup_groups_in(&mut self, keyspace: Keyspace, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		self.txn.lookup_groups_in(self.operator, keyspace, groups)
	}

	fn intern_group(&mut self, group: &EncodedKey) -> Result<(GroupId, bool)> {
		self.txn.intern_group(self.operator, group)
	}

	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
		self.txn.lookup_group(self.operator, group)
	}

	fn intern_group_in(&mut self, keyspace: Keyspace, group: &EncodedKey) -> Result<(GroupId, bool)> {
		self.txn.intern_group_in(self.operator, keyspace, group)
	}

	fn lookup_group_in(&mut self, keyspace: Keyspace, group: &EncodedKey) -> Result<Option<GroupId>> {
		self.txn.lookup_group_in(self.operator, keyspace, group)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers(self.operator, group, keys)
	}

	fn get_or_create_row_numbers_for_pairs(
		&mut self,
		pairs: &[(GroupId, EncodedKey)],
	) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers_for_pairs(self.operator, pairs)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.txn.remove_row_number(self.operator, group, key)
	}

	fn remove_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
		self.txn.remove_row_numbers(self.operator, group, keys)
	}

	fn written_at(&self) -> DateTime {
		self.now
	}
}

impl<T: FlowTransaction> IdentityReclaim for TxnHostContext<'_, T> {
	fn reclaim_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		self.txn.reclaim_group_identity(self.operator, group, limit)
	}

	fn reclaim_identity_keys(&mut self, group: GroupId, keys: &[GroupStateKey]) -> Result<ReclaimOutcome> {
		self.txn.reclaim_group_identity_keys(self.operator, group, keys)
	}
}

impl<T: FlowTransaction> HostContext for TxnHostContext<'_, T> {
	fn version(&self) -> CommitVersion {
		self.txn.version()
	}

	fn disarm_timer_by_key(&mut self, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.disarm_timer_by_key(self.operator, kind, key)
	}

	fn anchor_at(&mut self, group: GroupId, side: u8, row_number: RowNumber) -> Result<Option<DateTime>> {
		self.txn.anchor_at(self.operator, group, side, row_number)
	}

	fn anchor_min(&mut self, group: GroupId) -> Result<Option<DateTime>> {
		self.txn.anchor_min(self.operator, group)
	}

	fn anchor_seal_page(&mut self, group: GroupId, at: DateTime, budget: usize) -> Result<SealPage> {
		self.txn.anchor_seal_page(self.operator, group, at, budget)
	}

	fn config_uint8(&self, key: ConfigKey) -> u64 {
		self.txn.catalog().get_config_uint8(key)
	}

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		let batch = self.txn.state_get_many(self.operator, keys)?;
		let mut out = Vec::with_capacity(batch.items.len());
		for r in batch.items {
			let Some(key) = unscope(&r.key) else {
				continue;
			};
			out.push((key, EncodedPodRow::from(r.bytes)));
		}
		Ok(out)
	}

	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		self.state_range_limited(range, None)
	}

	fn state_range_limited(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		let mut query = StateRange::forward(range, "operator::host_range");
		query.limit = limit;
		let batch = self.txn.state_range(self.operator, query)?;
		let mut out = Vec::with_capacity(batch.items.len());
		for r in batch.items {
			let Some(key) = unscope(&r.key) else {
				continue;
			};
			out.push((key, EncodedPodRow::from(r.bytes)));
		}
		Ok(out)
	}

	fn state_range_iter(&mut self, range: EncodedKeyRange) -> StateIterator<'_> {
		let prefixed = range.with_prefix(EncodedKey::new(node_prefix(self.operator)));
		StateIterator::new(self.txn.range(prefixed, RangeScope::All, 1024))
	}

	fn state_clear(&mut self) -> Result<()> {
		self.txn.state_clear(self.operator)
	}

	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		self.txn.reclaim_group_identity(self.operator, group, limit)
	}

	fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
		self.txn.get_row_numbers(self.operator, group, keys)
	}

	fn get_row_numbers_for_groups(
		&mut self,
		groups: &[GroupId],
		key: &EncodedKey,
	) -> Result<Vec<Option<RowNumber>>> {
		self.txn.get_row_numbers_for_groups(self.operator, groups, key)
	}

	fn get_or_create_row_numbers_for_groups(
		&mut self,
		groups: &[GroupId],
		key: &EncodedKey,
	) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers_for_groups(self.operator, groups, key)
	}

	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		self.txn.remove_row_numbers_below(self.operator, group, upper)
	}

	fn remove_row_numbers_by_prefix(&mut self, group: GroupId, key_prefix: &[u8]) -> Result<()> {
		self.txn.remove_row_numbers_by_prefix(self.operator, group, key_prefix)
	}

	fn dictionary_id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>> {
		Ok(self.txn.find_dictionary_by_name(name).map(|d| d.id))
	}

	fn dictionary_value_type(&mut self, dictionary: DictionaryId) -> Option<ValueType> {
		self.txn.find_dictionary(dictionary).map(|d| d.value_type)
	}

	fn dictionary_id_type(&mut self, dictionary: DictionaryId) -> Option<ValueType> {
		self.txn.find_dictionary(dictionary).map(|d| d.id_type)
	}

	fn dictionary_find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.find_in_dictionary(&dict, value),
			None => Ok(None),
		}
	}

	fn dictionary_get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.get_from_dictionary(&dict, id),
			None => Ok(None),
		}
	}
}

fn unscope(key: &EncodedKey) -> Option<GroupStateKey> {
	GroupStateKey::from_framed(OperatorStateKey::decode(key)?.inner())
}
