// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	actors::pending::PendingWrite,
	key::operator::state::{GroupId, GroupStateKey, KeyspaceMask},
	state::{
		batch::StateBatch,
		timer::{GroupSweep, StateStore},
	},
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};

pub struct Prefetched<'a> {
	store: &'a mut dyn StateStore,
	batch: StateBatch,
	slots: HashMap<GroupStateKey, usize>,
}

impl<'a> Prefetched<'a> {
	pub fn read(store: &'a mut dyn StateStore, keys: Vec<GroupStateKey>) -> Result<Self> {
		let mut slots = HashMap::with_capacity(keys.len());
		let mut distinct = Vec::with_capacity(keys.len());
		for key in keys {
			if !slots.contains_key(&key) {
				slots.insert(key.clone(), distinct.len());
				distinct.push(key);
			}
		}
		let batch = store.state_batch(distinct)?;
		Ok(Self {
			store,
			batch,
			slots,
		})
	}

	pub fn write(self) -> Result<()> {
		self.store.state_write_batch(self.batch)
	}
}

impl StateStore for Prefetched<'_> {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		let Some(&slot) = self.slots.get(key) else {
			return self.store.state_get(key);
		};
		Ok(match self.batch.op(slot) {
			Some(PendingWrite::Set(row)) => Some(EncodedPodRow::from(row.clone())),
			Some(PendingWrite::Remove {
				..
			}) => None,
			None => self.batch.value(slot).cloned(),
		})
	}

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>> {
		self.store.state_get_many(keys)
	}

	fn state_classify(&mut self, key: &GroupStateKey, pre: Option<ByteSize>) {
		if !self.slots.contains_key(key) {
			self.store.state_classify(key, pre);
		}
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		match self.slots.get(key) {
			Some(&slot) => {
				self.batch.set(slot, payload);
				Ok(())
			}
			None => self.store.state_set(key, payload),
		}
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		match self.slots.get(key) {
			Some(&slot) => {
				self.batch.remove(slot);
				Ok(())
			}
			None => self.store.state_remove(key),
		}
	}

	fn state_remove_many(&mut self, keys: &[GroupStateKey]) -> Result<()> {
		self.store.state_remove_many(keys)
	}

	fn state_batch(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		self.store.state_batch(keys)
	}

	fn state_write_batch(&mut self, batch: StateBatch) -> Result<()> {
		self.store.state_write_batch(batch)
	}

	fn state_set_many(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
		self.store.state_set_many(rows)
	}

	fn state_page_inner(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		self.store.state_page_inner(range, limit)
	}

	fn group_sweep(
		&mut self,
		group: GroupId,
		keyspaces: KeyspaceMask,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		self.store.group_sweep(group, keyspaces, limit)
	}

	fn group_sweep_many(
		&mut self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<GroupSweep> {
		self.store.group_sweep_many(groups, limit, keyspaces)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.store.get_or_create_row_numbers(group, keys)
	}

	fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		self.store.get_or_create_row_numbers_for_groups(groups)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.store.remove_row_number(group, key)
	}

	fn remove_row_number_for_group(&mut self, group: GroupId) -> Result<()> {
		self.store.remove_row_number_for_group(group)
	}

	fn remove_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
		self.store.remove_row_numbers(group, keys)
	}

	fn remove_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<()> {
		self.store.remove_row_numbers_for_groups(groups)
	}

	fn written_at(&self) -> DateTime {
		self.store.written_at()
	}
}
