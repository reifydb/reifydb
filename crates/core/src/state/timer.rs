// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_ref;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::key::operator_state::{GroupId, GroupStateKey, Keyspace};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TimerKind {
	Seal = 0,
	Grace = 1,
	RowTtl = 2,
	Maintenance = 3,
}

impl TimerKind {
	pub fn is_unique(&self) -> bool {
		matches!(self, Self::Maintenance)
	}

	pub fn from_u8(value: u8) -> Option<Self> {
		match value {
			0 => Some(Self::Seal),
			1 => Some(Self::Grace),
			2 => Some(Self::RowTtl),
			3 => Some(Self::Maintenance),
			_ => None,
		}
	}
}

pub trait StateStore {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>>;

	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()>;

	fn state_classify(&mut self, _key: &GroupStateKey, _pre: Option<ByteSize>) {}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()>;

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()>;

	// FIXME remove
	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()>;

	fn state_last(&mut self, range: EncodedKeyRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
		let mut last = None;
		self.state_range_visit(range, None, &mut |key, payload| {
			last = Some((key, payload));
			Ok(())
		})?;
		Ok(last)
	}

	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>>;

	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>>;

	fn intern_groups_in(&mut self, keyspace: Keyspace, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		let _ = keyspace;
		self.intern_groups(groups)
	}

	fn lookup_groups_in(&mut self, keyspace: Keyspace, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		let _ = keyspace;
		self.lookup_groups(groups)
	}

	fn intern_group(&mut self, group: &EncodedKey) -> Result<(GroupId, bool)> {
		Ok(self.intern_groups(from_ref(group))?
			.into_iter()
			.next()
			.expect("intern_groups answers every requested key"))
	}

	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
		Ok(self.lookup_groups(from_ref(group))?
			.into_iter()
			.next()
			.expect("lookup_groups answers every requested key"))
	}

	fn intern_group_in(&mut self, keyspace: Keyspace, group: &EncodedKey) -> Result<(GroupId, bool)> {
		Ok(self.intern_groups_in(keyspace, from_ref(group))?
			.into_iter()
			.next()
			.expect("intern_groups_in answers every requested key"))
	}

	fn lookup_group_in(&mut self, keyspace: Keyspace, group: &EncodedKey) -> Result<Option<GroupId>> {
		Ok(self.lookup_groups_in(keyspace, from_ref(group))?
			.into_iter()
			.next()
			.expect("lookup_groups_in answers every requested key"))
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;

	fn get_or_create_row_numbers_for_pairs(
		&mut self,
		pairs: &[(GroupId, EncodedKey)],
	) -> Result<Vec<(RowNumber, bool)>>;

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;

	fn remove_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
		for key in keys {
			self.remove_row_number(group, key)?;
		}
		Ok(())
	}

	fn written_at(&self) -> DateTime;
}

pub trait TimerStore {
	fn arm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;

	fn disarm_timer(&mut self, due: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;

	fn flow_watermark(&mut self) -> Result<Option<DateTime>>;
}
