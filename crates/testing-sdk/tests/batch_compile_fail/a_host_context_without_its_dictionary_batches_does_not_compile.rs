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
		host::HostContext,
		state::{iter::StateIterator, reaper::IdentityReclaim, reclaim::ReclaimOutcome},
	},
	transaction::join_expiry::{DueStart, JoinDueEntry, JoinDuePage},
};
use reifydb_value::{
	Result,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		partition::Partition,
		row_number::RowNumber,
		value_type::ValueType,
	},
};

// A host that only resolves dictionary entries one at a time must not compile, or every batch of values pays one
// dictionary read per value.
struct SingleEntryHost;

impl TimerStore for SingleEntryHost {
	fn arm_timer(&mut self, _due: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
		unimplemented!()
	}

	fn disarm_timer(&mut self, _due: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
		unimplemented!()
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		unimplemented!()
	}
}

impl StateStore for SingleEntryHost {
	fn state_get(&mut self, _key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		unimplemented!()
	}

	fn state_get_many(&mut self, _keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>> {
		unimplemented!()
	}

	fn state_set(&mut self, _key: &GroupStateKey, _payload: EncodedPodRow) -> Result<()> {
		unimplemented!()
	}

	fn state_remove(&mut self, _key: &GroupStateKey) -> Result<()> {
		unimplemented!()
	}

	fn state_remove_many(&mut self, _keys: &[GroupStateKey]) -> Result<()> {
		unimplemented!()
	}

	fn state_batch(&mut self, _keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		unimplemented!()
	}

	fn state_write_batch(&mut self, _batch: StateBatch) -> Result<()> {
		unimplemented!()
	}

	fn state_set_many(&mut self, _rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
		unimplemented!()
	}

	fn state_page_inner(
		&mut self,
		_range: EncodedKeyRange,
		_limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		unimplemented!()
	}

	fn group_sweep(
		&mut self,
		_group: GroupId,
		_keyspaces: KeyspaceMask,
		_limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		unimplemented!()
	}

	fn group_sweep_many(
		&mut self,
		_groups: &[GroupId],
		_limit: usize,
		_keyspaces: KeyspaceMask,
	) -> Result<GroupSweep> {
		unimplemented!()
	}

	fn get_or_create_row_numbers(
		&mut self,
		_group: GroupId,
		_keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		unimplemented!()
	}

	fn get_or_create_row_numbers_for_groups(&mut self, _groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		unimplemented!()
	}

	fn get_row_numbers(&mut self, _group: GroupId, _keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
		unimplemented!()
	}

	fn remove_row_number(&mut self, _group: GroupId, _key: &EncodedKey) -> Result<()> {
		unimplemented!()
	}

	fn remove_row_number_for_group(&mut self, _group: GroupId) -> Result<()> {
		unimplemented!()
	}

	fn remove_row_numbers(&mut self, _group: GroupId, _keys: &[EncodedKey]) -> Result<()> {
		unimplemented!()
	}

	fn remove_row_numbers_for_groups(&mut self, _groups: &[GroupId]) -> Result<()> {
		unimplemented!()
	}

	fn written_at(&self) -> DateTime {
		unimplemented!()
	}
}

impl IdentityReclaim for SingleEntryHost {
	fn reclaim_identity(&mut self, _group: GroupId, _limit: usize) -> Result<ReclaimOutcome> {
		unimplemented!()
	}
}

impl Resolve for SingleEntryHost {
	fn dictionary(&mut self, _id: DictionaryId) -> Result<Option<Dictionary>> {
		unimplemented!()
	}

	fn resolve(&mut self, _dictionary: &Dictionary, _ids: &[DictionaryEntryId]) -> Result<Vec<Option<Value>>> {
		unimplemented!()
	}
}

impl HostContext for SingleEntryHost {
	fn version(&self) -> CommitVersion {
		unimplemented!()
	}

	fn disarm_timer_by_key(&mut self, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
		unimplemented!()
	}

	fn join_expiry_arm(&mut self, _group: GroupId, _side: u8, _row_number: RowNumber, _at: DateTime) -> Result<()> {
		unimplemented!()
	}

	fn join_expiry_clear(
		&mut self,
		_group: GroupId,
		_side: u8,
		_row_number: RowNumber,
	) -> Result<Option<DateTime>> {
		unimplemented!()
	}

	fn join_expiry_free(&mut self, _entry: &JoinDueEntry) -> Result<()> {
		unimplemented!()
	}

	fn join_expiry_min(&mut self) -> Result<Option<DateTime>> {
		unimplemented!()
	}

	fn join_due_page(&mut self, _at: DateTime, _budget: usize, _start: &DueStart) -> Result<JoinDuePage> {
		unimplemented!()
	}

	fn config_uint8(&self, _key: ConfigKey) -> u64 {
		unimplemented!()
	}

	fn row_shape_cache(&mut self) -> &mut HashMap<EncodedKey, RowShape> {
		unimplemented!()
	}

	fn state_range(&mut self, _range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		unimplemented!()
	}

	fn state_range_limited(
		&mut self,
		_range: EncodedKeyRange,
		_limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		unimplemented!()
	}

	fn state_range_iter(&mut self, _range: EncodedKeyRange) -> StateIterator<'_> {
		unimplemented!()
	}

	fn reclaim_group_identity(&mut self, _group: GroupId, _limit: usize) -> Result<ReclaimOutcome> {
		unimplemented!()
	}

	fn get_row_numbers_for_groups(&mut self, _groups: &[GroupId]) -> Result<Vec<Option<RowNumber>>> {
		unimplemented!()
	}

	fn get_join_row_numbers(&mut self, _keys: &[JoinRowMappingKey]) -> Result<Vec<Option<RowNumber>>> {
		unimplemented!()
	}

	fn get_or_create_join_row_numbers(&mut self, _keys: &[JoinRowMappingKey]) -> Result<Vec<(RowNumber, bool)>> {
		unimplemented!()
	}

	fn remove_join_row_numbers(&mut self, _keys: &[JoinRowMappingKey]) -> Result<()> {
		unimplemented!()
	}

	fn remove_join_row_numbers_for_left(&mut self, _tag: u8, _left: u64) -> Result<()> {
		unimplemented!()
	}

	fn dictionary_id_type(&mut self, _dictionary: DictionaryId) -> Option<ValueType> {
		unimplemented!()
	}

	fn dictionary_find(&mut self, _dictionary: DictionaryId, _value: &Value) -> Result<Option<DictionaryEntryId>> {
		unimplemented!()
	}

	fn lookup_read_many(
		&mut self,
		_storage: StorageId,
		_version: CommitVersion,
		_partitions: &[Partition],
	) -> Result<Vec<Option<(RowNumber, EncodedBytes)>>> {
		unimplemented!()
	}

	fn lookup_view_version(&self, _view: ViewId, _source: SourceVersion) -> CommitVersion {
		unimplemented!()
	}

	fn lookup_floor(&self) -> Option<CommitVersion> {
		unimplemented!()
	}
}

fn main() {}
