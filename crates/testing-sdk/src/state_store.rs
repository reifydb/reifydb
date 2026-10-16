// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey, KeyspaceMask},
	state::{
		batch::StateBatch,
		per_key,
		timer::{GroupSweep, StateStore, TimerKind, TimerStore},
	},
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

#[derive(Default)]
pub struct MemoryStateStore {
	data: HashMap<Vec<u8>, EncodedPodRow>,
	rows: HashMap<(GroupId, Vec<u8>), u64>,
	next_row: u64,
	armed: Vec<(DateTime, TimerKind, EncodedKey)>,
}

impl MemoryStateStore {
	pub fn armed(&self) -> &[(DateTime, TimerKind, EncodedKey)] {
		&self.armed
	}

	fn row_number_for(&mut self, group: GroupId, key: &EncodedKey) -> (RowNumber, bool) {
		let slot = (group, key.as_bytes().to_vec());
		if let Some(rn) = self.rows.get(&slot) {
			return (RowNumber(*rn), false);
		}
		self.next_row += 1;
		self.rows.insert(slot, self.next_row);
		(RowNumber(self.next_row), true)
	}
}

impl TimerStore for MemoryStateStore {
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.armed.push((due, kind, key.clone()));
		Ok(())
	}

	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.armed.retain(|(d, k, armed_key)| !(*d == due && *k == kind && armed_key == key));
		Ok(())
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		Ok(None)
	}
}

impl StateStore for MemoryStateStore {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		Ok(self.data.get(key.as_slice()).cloned())
	}

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>> {
		Ok(keys.iter().map(|key| self.data.get(key.as_slice()).cloned()).collect())
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		self.data.insert(key.as_slice().to_vec(), payload);
		Ok(())
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.data.remove(key.as_slice());
		Ok(())
	}

	fn state_remove_many(&mut self, keys: &[GroupStateKey]) -> Result<()> {
		per_key::remove_many(self, keys)
	}

	fn state_batch(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		per_key::read_batch(self, keys)
	}

	fn state_write_batch(&mut self, batch: StateBatch) -> Result<()> {
		per_key::write_batch(self, batch)
	}

	fn state_set_many(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
		per_key::set_many(self, rows)
	}

	fn state_page_inner(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		let after_start = |k: &[u8]| match &range.start {
			Bound::Included(s) => k >= s.as_bytes(),
			Bound::Excluded(s) => k > s.as_bytes(),
			Bound::Unbounded => true,
		};
		let before_end = |k: &[u8]| match &range.end {
			Bound::Included(e) => k <= e.as_bytes(),
			Bound::Excluded(e) => k < e.as_bytes(),
			Bound::Unbounded => true,
		};
		let mut matched: Vec<(Vec<u8>, EncodedPodRow)> = self
			.data
			.iter()
			.filter(|(k, _)| after_start(k) && before_end(k))
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect();
		matched.sort_by(|a, b| a.0.cmp(&b.0));
		if let Some(limit) = limit {
			matched.truncate(limit);
		}
		Ok(matched
			.into_iter()
			.map(|(k, payload)| {
				let key = GroupStateKey::from_framed(EncodedKey::new(k))
					.expect("memory state store holds only framed state keys");
				(key, payload)
			})
			.collect())
	}

	fn group_sweep(
		&mut self,
		group: GroupId,
		keyspaces: KeyspaceMask,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		per_key::group_sweep(self, group, keyspaces, limit)
	}

	fn group_sweep_many(
		&mut self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<GroupSweep> {
		per_key::group_sweep_many(self, groups, limit, keyspaces)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		Ok(keys.iter().map(|key| self.row_number_for(group, key)).collect())
	}

	fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		Ok(groups.iter().map(|group| self.row_number_for(*group, &EncodedKey::new(Vec::new()))).collect())
	}

	fn get_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
		Ok(keys.iter()
			.map(|key| self.rows.get(&(group, key.as_bytes().to_vec())).map(|rn| RowNumber(*rn)))
			.collect())
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.rows.remove(&(group, key.as_bytes().to_vec()));
		Ok(())
	}

	fn remove_row_number_for_group(&mut self, group: GroupId) -> Result<()> {
		self.rows.remove(&(group, Vec::new()));
		Ok(())
	}

	fn remove_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
		per_key::remove_row_numbers(self, group, keys)
	}

	fn remove_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<()> {
		per_key::remove_row_numbers_for_groups(self, groups)
	}

	fn written_at(&self) -> DateTime {
		DateTime::default()
	}
}
