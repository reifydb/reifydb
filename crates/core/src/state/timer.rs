// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	reifydb_assertions,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::key::operator::{
	keyspace::{RootSibling, root_sibling_of},
	state::{
		GroupId, GroupStateKey, KeyspaceMask, group_data_inner_range, group_inner_range,
		keyspace_inner_range_split,
	},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum TimerKind {
	Seal = 0,
	Grace = 1,
	RowTtl = 2,
	Maintenance = 3,
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
		data_only: bool,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		self.group_sweep_in(group, KeyspaceMask::KNOWN, data_only, limit)
	}

	#[cfg_attr(not(debug_assertions), allow(unused_variables))]
	fn group_sweep_in(
		&mut self,
		group: GroupId,
		mask: KeyspaceMask,
		data_only: bool,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		let range = match data_only {
			true => group_data_inner_range(group),
			false => group_inner_range(group),
		};
		let swept = self.state_page_inner(range, limit)?;
		reifydb_assertions! {
			for (key, _) in &swept {
				let Some((_, keyspace, _)) =
					crate::key::operator::state::OperatorStateKey::decode_inner(key.as_encoded().as_bytes())
				else {
					continue;
				};
				assert!(
					mask.contains(keyspace),
					"group {} holds a row in {} which the sweep set omits; declaring the group done \
					 would orphan it behind a group id nothing can resolve again",
					group,
					keyspace.name()
				);
			}
		}
		Ok(swept)
	}

	fn remove_root_siblings(&mut self, swept: &[(GroupStateKey, EncodedPodRow)]) -> Result<()> {
		for (key, row) in swept {
			let Some(sibling) = root_sibling_of(key, row) else {
				continue;
			};
			if let RootSibling::Derived(sibling) = sibling {
				self.state_remove(&sibling)?;
			}
		}
		Ok(())
	}

	fn state_last(&mut self, range: EncodedKeyRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
		Ok(self.state_page(range, None)?.pop())
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;

	fn get_or_create_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>>;

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;

	fn remove_row_number_for_group(&mut self, group: GroupId) -> Result<()>;

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
