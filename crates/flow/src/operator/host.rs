// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		flow::OperatorId,
	},
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupSet, GroupStateKey, OperatorStateKey, node_prefix},
	},
	state::store::{StateStore, TimerKind, TimerStore},
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		row_number::RowNumber,
		value_type::ValueType,
	},
};

use crate::{
	operator::stateful::StateIterator,
	timer::Timer,
	transaction::{
		FlowTransaction,
		dictionary::DictionaryTxn,
		group::GroupTxn,
		reclaim::{ReclaimOutcome, ReclaimTxn},
		row_number::RowNumberTxn,
		state::StateTxn,
		timer::TimerTxn,
	},
};

pub trait HostContext: StateStore + TimerStore {
	fn version(&self) -> CommitVersion;

	fn config_uint8(&self, key: ConfigKey) -> u64;

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, EncodedOperatorRow)>>;

	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedOperatorRow)>>;

	fn state_range_iter(&mut self, range: EncodedKeyRange) -> StateIterator<'_>;

	fn state_clear(&mut self) -> Result<()>;

	fn state_scan_all(&mut self) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
		self.state_range_iter(EncodedKeyRange::all()).collect()
	}

	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>>;

	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>>;

	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome>;

	fn get_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<Option<RowNumber>>;

	fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>>;

	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>>;

	fn remove_row_numbers_by_prefix(&mut self, group: GroupId, key_prefix: &[u8]) -> Result<()>;

	fn invalidate_row_number_groups(&mut self, groups: &GroupSet);

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
	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.arm_timer(
			self.operator,
			&Timer {
				at,
				kind,
				key: key.clone(),
			},
		)
	}

	fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.disarm_timer(
			self.operator,
			&Timer {
				at,
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
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
		self.txn.state_get(self.operator, key)
	}

	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.state_get_many(self.operator, keys)?;
		for r in batch.items {
			let Some(decoded) = OperatorStateKey::decode(&r.key) else {
				continue;
			};
			let Some(inner) = GroupStateKey::from_framed(decoded.inner()) else {
				continue;
			};
			visit(inner, EncodedOperatorRow::try_from(r.bytes)?)?;
		}
		Ok(())
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> Result<()> {
		self.txn.state_set(self.operator, key, payload)
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.txn.state_remove(self.operator, key)
	}

	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()> {
		let batch = self.txn.state_range(self.operator, range, limit, "operator::host_visit")?;
		for r in batch.items {
			if let Some(decoded) = OperatorStateKey::decode(&r.key)
				&& let Some(inner) = GroupStateKey::from_framed(decoded.inner())
			{
				visit(inner, EncodedOperatorRow::try_from(r.bytes)?)?;
			}
		}
		Ok(())
	}

	fn intern_group(&mut self, group: &EncodedKey) -> Result<GroupId> {
		Ok(self.txn.intern_group(self.operator, group)?.0)
	}

	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
		self.txn.lookup_group(self.operator, group)
	}

	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		self.txn.get_or_create_row_number(self.operator, group, key)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers(self.operator, group, keys)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.txn.remove_row_number(self.operator, group, key).map(|_| ())
	}

	fn written_at(&self) -> DateTime {
		self.now
	}
}

impl<T: FlowTransaction> HostContext for TxnHostContext<'_, T> {
	fn version(&self) -> CommitVersion {
		self.txn.version()
	}

	fn config_uint8(&self, key: ConfigKey) -> u64 {
		self.txn.catalog().get_config_uint8(key)
	}

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, EncodedOperatorRow)>> {
		let batch = self.txn.state_get_many(self.operator, keys)?;
		let mut out = Vec::with_capacity(batch.items.len());
		for r in batch.items {
			let Some(key) = unscope(&r.key) else {
				continue;
			};
			out.push((key, EncodedOperatorRow::try_from(r.bytes)?));
		}
		Ok(out)
	}

	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedOperatorRow)>> {
		let batch = self.txn.state_range_all(self.operator, range)?;
		let mut out = Vec::with_capacity(batch.items.len());
		for r in batch.items {
			let Some(key) = unscope(&r.key) else {
				continue;
			};
			out.push((key, EncodedOperatorRow::try_from(r.bytes)?));
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

	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		self.txn.intern_groups(self.operator, groups)
	}

	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		groups.iter().map(|group| self.txn.lookup_group(self.operator, group)).collect()
	}

	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		self.txn.reclaim_group_identity(self.operator, group, limit)
	}

	fn get_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<Option<RowNumber>> {
		self.txn.get_row_number(self.operator, group, key)
	}

	fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
		self.txn.get_row_numbers(self.operator, group, keys)
	}

	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		self.txn.remove_row_numbers_below(self.operator, group, upper)
	}

	fn remove_row_numbers_by_prefix(&mut self, group: GroupId, key_prefix: &[u8]) -> Result<()> {
		self.txn.remove_row_numbers_by_prefix(self.operator, group, key_prefix)
	}

	fn invalidate_row_number_groups(&mut self, groups: &GroupSet) {
		self.txn.invalidate_row_number_groups(self.operator, groups)
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
