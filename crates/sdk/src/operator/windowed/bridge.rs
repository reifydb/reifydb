// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	state::StateBytes,
};
use reifydb_core::state::store::StateStore;
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::operator::context::{InternalStateApi, OperatorContext, StateApi};

pub struct OperatorContextStore<'a, C: OperatorContext>(pub &'a mut C);

impl<C: OperatorContext> StateStore for OperatorContextStore<'_, C> {
	fn state_get(&mut self, key: &EncodedKey) -> Result<Option<StateBytes>> {
		Ok(self.0.state().get_bytes(key)?)
	}

	fn state_get_many_visit(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		self.0.state().get_many_bytes_visit(keys, &mut |k, v| visit(k, v).map_err(Into::into))?;
		Ok(())
	}

	fn state_set(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()> {
		self.0.state().set_bytes(key, payload)?;
		Ok(())
	}

	fn state_remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.0.state().remove(key)?;
		Ok(())
	}

	fn state_drop(&mut self, key: &EncodedKey) -> Result<()> {
		self.0.state().drop(key)?;
		Ok(())
	}

	fn internal_get(&mut self, key: &EncodedKey) -> Result<Option<StateBytes>> {
		Ok(self.0.internal_state().get_bytes(key)?)
	}

	fn internal_get_many_visit(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		self.0.internal_state().get_many_bytes_visit(keys, &mut |k, v| visit(k, v).map_err(Into::into))?;
		Ok(())
	}

	fn internal_set(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()> {
		self.0.internal_state().set_bytes(key, payload)?;
		Ok(())
	}

	fn internal_remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.0.internal_state().remove(key)?;
		Ok(())
	}

	fn internal_drop(&mut self, key: &EncodedKey) -> Result<()> {
		self.0.internal_state().drop(key)?;
		Ok(())
	}

	fn internal_range_visit(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		let start = match &range.start {
			Bound::Included(k) => Bound::Included(k),
			Bound::Excluded(k) => Bound::Excluded(k),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match &range.end {
			Bound::Included(k) => Bound::Included(k),
			Bound::Excluded(k) => Bound::Excluded(k),
			Bound::Unbounded => Bound::Unbounded,
		};
		let mut remaining = limit;
		self.0.internal_state().range_bytes_visit(start, end, &mut |k, v| match remaining.as_mut() {
			Some(0) => Ok(()),
			Some(r) => {
				*r -= 1;
				visit(k, v).map_err(Into::into)
			}
			None => visit(k, v).map_err(Into::into),
		})?;
		Ok(())
	}

	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		Ok(self.0.get_or_create_row_number(key)?)
	}

	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		Ok(self.0.get_or_create_row_numbers(keys)?)
	}

	fn drop_row_number(&mut self, key: &EncodedKey) -> Result<()> {
		Ok(self.0.drop_row_number(key)?)
	}

	fn allocate_row_numbers(&mut self, count: u64) -> Result<RowNumber> {
		Ok(self.0.allocate_row_numbers(count)?)
	}

	fn clock_now_nanos(&self) -> u64 {
		self.0.clock_now_nanos()
	}
}
