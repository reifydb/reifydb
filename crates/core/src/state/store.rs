// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	state::StateBytes,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::key::operator_state::{GroupId, StateKey};

pub trait StateStore {
	fn state_get(&mut self, key: &StateKey) -> Result<Option<StateBytes>>;

	fn state_get_many_visit(
		&mut self,
		keys: &[StateKey],
		visit: &mut dyn FnMut(StateKey, StateBytes) -> Result<()>,
	) -> Result<()>;

	fn state_set(&mut self, key: &StateKey, payload: StateBytes) -> Result<()>;

	fn state_remove(&mut self, key: &StateKey) -> Result<()>;

	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(StateKey, StateBytes) -> Result<()>,
	) -> Result<()>;

	fn intern_group(&mut self, group: &EncodedKey) -> Result<GroupId>;

	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>>;

	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)>;

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()>;

	fn clock_now(&self) -> DateTime;
}
