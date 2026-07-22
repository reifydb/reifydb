// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	state::StateBytes,
};
use reifydb_value::{Result, value::row_number::RowNumber};

pub trait StateStore {
	fn state_get(&mut self, key: &EncodedKey) -> Result<Option<StateBytes>>;

	fn state_get_many_visit(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()>;

	fn state_set(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()>;

	fn state_remove(&mut self, key: &EncodedKey) -> Result<()>;

	fn state_drop(&mut self, key: &EncodedKey) -> Result<()>;

	fn internal_get(&mut self, key: &EncodedKey) -> Result<Option<StateBytes>>;

	fn internal_get_many_visit(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()>;

	fn internal_set(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()>;

	fn internal_remove(&mut self, key: &EncodedKey) -> Result<()>;

	fn internal_drop(&mut self, key: &EncodedKey) -> Result<()>;

	fn internal_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()>;

	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)>;

	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;

	fn drop_row_number(&mut self, key: &EncodedKey) -> Result<()>;

	fn clock_now_nanos(&self) -> u64;
}
