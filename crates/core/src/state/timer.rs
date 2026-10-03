// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Reverse;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{
	actors::pending::PendingWrite,
	key::operator::{
		keyspace::{RootSibling, root_sibling_of},
		state::{GroupId, GroupStateKey, KeyspaceMask, keyspace_inner_range, keyspace_inner_range_split},
	},
	state::batch::StateBatch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum TimerKind {
	Seal = 0,
	Grace = 1,
	RowTtl = 2,
	Maintenance = 3,
	Reclaim = 4,
}

impl TimerKind {
	pub fn is_maintenance(&self) -> bool {
		matches!(self, Self::Maintenance)
	}

	pub fn from_u8(value: u8) -> Option<Self> {
		match value {
			0 => Some(Self::Seal),
			1 => Some(Self::Grace),
			2 => Some(Self::RowTtl),
			3 => Some(Self::Maintenance),
			4 => Some(Self::Reclaim),
			_ => None,
		}
	}
}

pub struct GroupSweep {
	pub rows: Vec<(GroupStateKey, EncodedPodRow)>,
	pub complete: bool,
}

impl GroupSweep {
	pub fn of(mut rows: Vec<(GroupStateKey, EncodedPodRow)>, limit: usize) -> Self {
		let complete = rows.len() <= limit;
		rows.truncate(limit);
		Self {
			rows,
			complete,
		}
	}
}

pub fn sweep_order(groups: &[GroupId]) -> Vec<GroupId> {
	let mut ordered = groups.to_vec();
	ordered.sort_by_key(|group| Reverse(*group.as_bytes()));
	ordered.dedup();
	ordered
}

pub trait StateStore {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>>;

	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>>;

	fn state_classify(&mut self, _key: &GroupStateKey, _pre: Option<ByteSize>) {}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()>;

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()>;

	fn state_remove_many(&mut self, keys: &[GroupStateKey]) -> Result<()> {
		for key in keys {
			self.state_remove(key)?;
		}
		Ok(())
	}

	fn state_batch(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
		let batch = StateBatch::read(keys, |keys| self.state_get_many(keys))?;
		for slot in 0..batch.len() {
			self.state_classify(batch.key(slot), batch.value(slot).map(EncodedPodRow::byte_size));
		}
		Ok(batch)
	}

	fn state_write_batch(&mut self, batch: StateBatch) -> Result<()> {
		for (key, write) in batch.into_writes() {
			match write {
				PendingWrite::Set(bytes) => self.state_set(&key, EncodedPodRow::from(bytes))?,
				PendingWrite::Remove {
					..
				} => self.state_remove(&key)?,
			}
		}
		Ok(())
	}

	fn state_set_many(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
		for (key, row) in rows {
			self.state_set(&key, row)?;
		}
		Ok(())
	}

	fn state_page(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		debug_assert!(
			keyspace_inner_range_split(&range).is_some(),
			"a state page must stay inside one group and one keyspace; {range:?} spans more than one"
		);
		self.state_page_inner(range, limit)
	}

	fn state_page_inner(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>>;

	fn group_sweep(
		&mut self,
		group: GroupId,
		keyspaces: KeyspaceMask,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		let mut rows = Vec::new();
		for keyspace in keyspaces.held().into_iter().rev() {
			let remaining = match limit {
				Some(limit) if rows.len() >= limit => break,
				Some(limit) => Some(limit - rows.len()),
				None => None,
			};
			rows.extend(self.state_page_inner(keyspace_inner_range(group, keyspace), remaining)?);
		}
		Ok(rows)
	}

	fn group_sweep_many(
		&mut self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<GroupSweep> {
		let mut rows = Vec::new();
		for group in sweep_order(groups) {
			if rows.len() > limit {
				break;
			}
			let remaining = limit.saturating_add(1).saturating_sub(rows.len());
			rows.extend(self.group_sweep(group, keyspaces, Some(remaining))?);
		}
		Ok(GroupSweep::of(rows, limit))
	}

	fn remove_root_siblings(&mut self, swept: &[(GroupStateKey, EncodedPodRow)]) -> Result<()> {
		let mut siblings = Vec::new();
		for (key, row) in swept {
			if let Some(RootSibling::Derived(sibling)) = root_sibling_of(key, row) {
				siblings.push(sibling);
			}
		}
		self.state_remove_many(&siblings)
	}

	fn state_last(&mut self, range: EncodedKeyRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
		Ok(self.state_page(range, None)?.pop())
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;

	fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>>;

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;

	fn remove_row_number_for_group(&mut self, group: GroupId) -> Result<()>;

	fn written_at(&self) -> DateTime;
}

pub trait TimerStore {
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;

	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;

	fn flow_watermark(&mut self) -> Result<Option<DateTime>>;
}
