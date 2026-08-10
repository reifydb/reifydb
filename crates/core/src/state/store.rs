// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::key::operator_state::{GroupId, GroupStateKey};

pub trait StateStore {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>>;

	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()>;

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> Result<()>;

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()>;

	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
	) -> Result<()>;

	fn intern_group(&mut self, group: &EncodedKey) -> Result<GroupId>;

	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>>;

	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)>;

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;

	fn written_at(&self) -> DateTime;

	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;

	fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()>;

	fn flow_watermark(&mut self) -> Result<Option<DateTime>>;
}
