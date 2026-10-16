// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, pod::EncodedPodRow, shape::RowShape},
};
use reifydb_core::{
	common::{CommitVersion, SourceVersion},
	interface::catalog::{config::ConfigKey, dictionary::Dictionary, id::ViewId, storage::StorageId},
	key::operator::{
		keyspace::join::JoinRowMappingKey,
		state::{GroupId, GroupStateKey, KeyspaceMask},
	},
	state::{
		batch::StateBatch,
		timer::{GroupSweep, StateStore, TimerKind, TimerStore},
	},
};
use reifydb_flow::txn::Resolve;
use reifydb_flow_async::{
	operator::{
		host::{HostContext, TxnHostContext},
		state::{iter::StateIterator, reaper::IdentityReclaim, reclaim::ReclaimOutcome},
	},
	transaction::{
		FlowTransaction,
		join_expiry::{DueStart, JoinDueEntry, JoinDuePage},
	},
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		partition::Partition,
		row_number::RowNumber,
		value_type::ValueType,
	},
};

pub type Call = (&'static str, usize);

pub struct CountingHost<'a, T: FlowTransaction> {
	inner: TxnHostContext<'a, T>,
	calls: &'a mut Vec<Call>,
}

impl<'a, T: FlowTransaction> CountingHost<'a, T> {
	pub fn new(inner: TxnHostContext<'a, T>, calls: &'a mut Vec<Call>) -> Self {
		Self {
			inner,
			calls,
		}
	}

	fn record(&mut self, name: &'static str, size: usize) {
		self.calls.push((name, size));
	}
}

impl<T: FlowTransaction> TimerStore for CountingHost<'_, T> {
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.record("arm_timer", 1);
		self.inner.arm_timer(due, kind, key)
	}

	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.record("disarm_timer", 1);
		self.inner.disarm_timer(due, kind, key)
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		self.record("flow_watermark", 1);
		self.inner.flow_watermark()
	}
}

impl<T: FlowTransaction> StateStore for CountingHost<'_, T> {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		self.record("state_get", 1);
		self.inner.state_get(key)
	}

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>> {
		self.record("state_get_many", keys.len());
		self.inner.state_get_many(keys)
	}

	fn state_classify(&mut self, key: &GroupStateKey, pre: Option<ByteSize>) {
		self.inner.state_classify(key, pre)
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		self.record("state_set", 1);
		self.inner.state_set(key, payload)
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.record("state_remove", 1);
		self.inner.state_remove(key)
	}

	fn state_remove_many(&mut self, keys: &[GroupStateKey]) -> Result<()> {
		self.record("state_remove_many", keys.len());
		self.inner.state_remove_many(keys)
	}

	fn state_batch(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		self.record("state_batch", keys.len());
		self.inner.state_batch(keys)
	}

	fn state_write_batch(&mut self, batch: StateBatch) -> Result<()> {
		self.record("state_write_batch", batch.len());
		self.inner.state_write_batch(batch)
	}

	fn state_set_many(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
		self.record("state_set_many", rows.len());
		self.inner.state_set_many(rows)
	}

	fn state_page(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		self.record("state_page", 1);
		self.inner.state_page(range, limit)
	}

	fn state_page_inner(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		self.record("state_page_inner", 1);
		self.inner.state_page_inner(range, limit)
	}

	fn group_sweep(
		&mut self,
		group: GroupId,
		keyspaces: KeyspaceMask,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		self.record("group_sweep", 1);
		self.inner.group_sweep(group, keyspaces, limit)
	}

	fn group_sweep_many(
		&mut self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<GroupSweep> {
		self.record("group_sweep_many", groups.len());
		self.record("group_sweep_many_limit", limit);
		self.inner.group_sweep_many(groups, limit, keyspaces)
	}

	fn remove_root_siblings(&mut self, swept: &[(GroupStateKey, EncodedPodRow)]) -> Result<()> {
		self.record("remove_root_siblings", swept.len());
		self.inner.remove_root_siblings(swept)
	}

	fn state_last(&mut self, range: EncodedKeyRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
		self.record("state_last", 1);
		self.inner.state_last(range)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.record("get_or_create_row_numbers", keys.len());
		self.inner.get_or_create_row_numbers(group, keys)
	}

	fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		self.record("get_or_create_row_numbers_for_groups", groups.len());
		self.inner.get_or_create_row_numbers_for_groups(groups)
	}

	fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
		self.record("get_row_numbers", keys.len());
		self.inner.get_row_numbers(group, keys)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.record("remove_row_number", 1);
		self.inner.remove_row_number(group, key)
	}

	fn remove_row_number_for_group(&mut self, group: GroupId) -> Result<()> {
		self.record("remove_row_number_for_group", 1);
		self.inner.remove_row_number_for_group(group)
	}

	fn remove_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
		self.record("remove_row_numbers", keys.len());
		self.inner.remove_row_numbers(group, keys)
	}

	fn remove_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<()> {
		self.record("remove_row_numbers_for_groups", groups.len());
		self.inner.remove_row_numbers_for_groups(groups)
	}

	fn written_at(&self) -> DateTime {
		self.inner.written_at()
	}
}

impl<T: FlowTransaction> IdentityReclaim for CountingHost<'_, T> {
	fn reclaim_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		self.record("reclaim_identity", 1);
		self.inner.reclaim_identity(group, limit)
	}
}

impl<T: FlowTransaction> Resolve for CountingHost<'_, T> {
	fn dictionary(&mut self, id: DictionaryId) -> Result<Option<Dictionary>> {
		self.record("dictionary", 1);
		self.inner.dictionary(id)
	}

	fn resolve(&mut self, dictionary: &Dictionary, ids: &[DictionaryEntryId]) -> Result<Vec<Option<Value>>> {
		self.record("resolve", ids.len());
		self.inner.resolve(dictionary, ids)
	}
}

impl<T: FlowTransaction> HostContext for CountingHost<'_, T> {
	fn version(&self) -> CommitVersion {
		self.inner.version()
	}

	fn disarm_timer_by_key(&mut self, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.record("disarm_timer_by_key", 1);
		self.inner.disarm_timer_by_key(kind, key)
	}

	fn join_expiry_arm(&mut self, group: GroupId, side: u8, row_number: RowNumber, at: DateTime) -> Result<()> {
		self.record("join_expiry_arm", 1);
		self.inner.join_expiry_arm(group, side, row_number, at)
	}

	fn join_expiry_clear(&mut self, group: GroupId, side: u8, row_number: RowNumber) -> Result<Option<DateTime>> {
		self.record("join_expiry_clear", 1);
		self.inner.join_expiry_clear(group, side, row_number)
	}

	fn join_expiry_free(&mut self, entry: &JoinDueEntry) -> Result<()> {
		self.record("join_expiry_free", 1);
		self.inner.join_expiry_free(entry)
	}

	fn join_expiry_min(&mut self) -> Result<Option<DateTime>> {
		self.record("join_expiry_min", 1);
		self.inner.join_expiry_min()
	}

	fn join_due_page(&mut self, at: DateTime, budget: usize, start: &DueStart) -> Result<JoinDuePage> {
		self.record("join_due_page", 1);
		self.inner.join_due_page(at, budget, start)
	}

	fn config_uint8(&self, key: ConfigKey) -> u64 {
		self.inner.config_uint8(key)
	}

	fn row_shape_cache(&mut self) -> &mut HashMap<EncodedKey, RowShape> {
		self.inner.row_shape_cache()
	}

	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		self.record("state_range", 1);
		self.inner.state_range(range)
	}

	fn state_range_limited(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		self.record("state_range_limited", 1);
		self.inner.state_range_limited(range, limit)
	}

	fn state_range_limited_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		self.record("state_range_limited_visit", 1);
		self.inner.state_range_limited_visit(range, limit, visit)
	}

	fn state_range_iter(&mut self, range: EncodedKeyRange) -> StateIterator<'_> {
		self.record("state_range_iter", 1);
		self.inner.state_range_iter(range)
	}

	fn reclaim_group_identity(&mut self, group: GroupId, limit: usize) -> Result<ReclaimOutcome> {
		self.record("reclaim_group_identity", 1);
		self.inner.reclaim_group_identity(group, limit)
	}

	fn get_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<Option<RowNumber>>> {
		self.record("get_row_numbers_for_groups", groups.len());
		self.inner.get_row_numbers_for_groups(groups)
	}

	fn get_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<Vec<Option<RowNumber>>> {
		self.record("get_join_row_numbers", keys.len());
		self.inner.get_join_row_numbers(keys)
	}

	fn get_or_create_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.record("get_or_create_join_row_numbers", keys.len());
		self.inner.get_or_create_join_row_numbers(keys)
	}

	fn remove_join_row_numbers(&mut self, keys: &[JoinRowMappingKey]) -> Result<()> {
		self.record("remove_join_row_numbers", keys.len());
		self.inner.remove_join_row_numbers(keys)
	}

	fn remove_join_row_numbers_for_left(&mut self, tag: u8, left: u64) -> Result<()> {
		self.record("remove_join_row_numbers_for_left", 1);
		self.inner.remove_join_row_numbers_for_left(tag, left)
	}

	fn dictionary_id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>> {
		self.record("dictionary_id_by_name", 1);
		self.inner.dictionary_id_by_name(name)
	}

	fn dictionary_id_type(&mut self, dictionary: DictionaryId) -> Option<ValueType> {
		self.record("dictionary_id_type", 1);
		self.inner.dictionary_id_type(dictionary)
	}

	fn dictionary_find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>> {
		self.record("dictionary_find", 1);
		self.inner.dictionary_find(dictionary, value)
	}

	fn dictionary_get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>> {
		self.record("dictionary_get", 1);
		self.inner.dictionary_get(dictionary, id)
	}

	fn dictionary_find_many(
		&mut self,
		dictionary: DictionaryId,
		values: &[Value],
	) -> Result<Vec<Option<DictionaryEntryId>>> {
		self.record("dictionary_find_many", values.len());
		self.inner.dictionary_find_many(dictionary, values)
	}

	fn dictionary_get_many(
		&mut self,
		dictionary: DictionaryId,
		ids: &[DictionaryEntryId],
	) -> Result<Vec<Option<Value>>> {
		self.record("dictionary_get_many", ids.len());
		self.inner.dictionary_get_many(dictionary, ids)
	}

	fn lookup_read_many(
		&mut self,
		storage: StorageId,
		version: CommitVersion,
		partitions: &[Partition],
	) -> Result<Vec<Option<(RowNumber, EncodedBytes)>>> {
		self.record("lookup_read_many", partitions.len());
		self.inner.lookup_read_many(storage, version, partitions)
	}

	fn lookup_view_version(&self, view: ViewId, source: SourceVersion) -> CommitVersion {
		self.inner.lookup_view_version(view, source)
	}

	fn lookup_floor(&self) -> Option<CommitVersion> {
		self.inner.lookup_floor()
	}
}
