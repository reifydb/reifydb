// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Reverse;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_value::{
	byte_size::ByteSize,
	error::ValueError,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{
	key::operator::{
		keyspace::{RootSibling, root_sibling_of},
		state::{GroupId, GroupStateKey, GroupStateRange, KeyspaceMask},
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

pub fn sweep_order(groups: &[GroupId]) -> Vec<GroupId> {
	let mut ordered = groups.to_vec();
	ordered.sort_by_key(|group| Reverse(*group.as_bytes()));
	ordered.dedup();
	ordered
}

pub trait StateStore {
	fn state_get(&mut self, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>, ValueError>;

	fn state_classify(&mut self, _key: &GroupStateKey, _pre: Option<ByteSize>) {}

	fn state_retires_groups(&self) -> bool {
		false
	}

	fn state_set(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<(), ValueError>;

	fn state_remove(&mut self, keys: &[GroupStateKey]) -> Result<(), ValueError>;

	fn state_batch(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch, ValueError>;

	fn state_write_batch(&mut self, batch: StateBatch) -> Result<(), ValueError>;

	fn state_page(
		&mut self,
		range: GroupStateRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>, ValueError> {
		debug_assert!(
			matches!(range, GroupStateRange::Keyspace { .. }),
			"a state page must stay inside one group and one keyspace; {range:?} spans more than one"
		);
		self.state_page_inner(range, limit)
	}

	fn state_page_inner(
		&mut self,
		range: GroupStateRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>, ValueError>;

	fn group_sweep(
		&mut self,
		groups: &[GroupId],
		limit: usize,
		keyspaces: KeyspaceMask,
	) -> Result<GroupSweep, ValueError>;

	fn remove_root_siblings(&mut self, swept: &[(GroupStateKey, EncodedPodRow)]) -> Result<(), ValueError> {
		let mut siblings = Vec::new();
		for (key, row) in swept {
			if let Some(RootSibling::Derived(sibling)) = root_sibling_of(key, row) {
				siblings.push(sibling);
			}
		}
		self.state_remove(&siblings)
	}

	fn state_last(&mut self, range: GroupStateRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>, ValueError> {
		Ok(self.state_page(range, None)?.pop())
	}

	fn get_or_create_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>, ValueError>;

	fn get_or_create_row_numbers_for_groups(
		&mut self,
		groups: &[GroupId],
	) -> Result<Vec<(RowNumber, bool)>, ValueError>;

	fn get_row_numbers(
		&mut self,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<Option<RowNumber>>, ValueError>;

	fn remove_row_number(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<(), ValueError>;

	fn remove_row_number_for_group(&mut self, groups: &[GroupId]) -> Result<(), ValueError>;

	fn mint_row_numbers(&mut self, count: usize) -> Result<RowNumber, ValueError>;

	fn get_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<Option<RowNumber>>, ValueError>;

	fn set_row_numbers_for_groups(&mut self, rows: &[(GroupId, RowNumber)]) -> Result<(), ValueError>;

	fn batch_time(&self) -> DateTime;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerMove {
	pub key: EncodedKey,
	pub before: Option<DateTime>,
	pub after: Option<DateTime>,
}

pub trait TimerStore {
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<(), ValueError>;

	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<(), ValueError>;

	fn flow_watermark(&mut self) -> Result<Option<DateTime>, ValueError>;

	fn rearm_timers(&mut self, kind: TimerKind, moves: &[TimerMove]) -> Result<(), ValueError> {
		for timer in moves.iter().filter(|timer| timer.before != timer.after) {
			if let Some(before) = timer.before {
				self.disarm_timer(before, kind, &timer.key)?;
			}
			if let Some(after) = timer.after {
				self.arm_timer(after, kind, &timer.key)?;
			}
		}
		Ok(())
	}
}
