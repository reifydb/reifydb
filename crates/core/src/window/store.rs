// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::{Result, value::row_number::RowNumber};
use serde::{Serialize, de::DeserializeOwned};

use crate::encoded::key::EncodedKey;

pub trait WindowStore {
	fn state_get<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Result<Option<V>>;

	fn state_get_many_visit<V: DeserializeOwned>(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
	) -> Result<()>;

	fn state_set<V: Serialize>(&mut self, key: &EncodedKey, value: &V) -> Result<()>;

	fn state_remove(&mut self, key: &EncodedKey) -> Result<()>;

	fn internal_get<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Result<Option<V>>;

	fn internal_get_many_visit<V: DeserializeOwned>(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
	) -> Result<()>;

	fn internal_set<V: Serialize>(&mut self, key: &EncodedKey, value: &V) -> Result<()>;

	fn internal_remove(&mut self, key: &EncodedKey) -> Result<()>;

	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)>;

	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>>;

	fn allocate_row_numbers(&mut self, count: u64) -> Result<RowNumber>;

	fn clock_now_nanos(&self) -> u64;
}
