// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	state::StateBytes,
};
use reifydb_core::{
	key::operator_state::GroupId,
	state::{horizon::GroupPosition, store::StateStore},
};
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::operator::context::{OperatorContext, StateApi};

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

	fn state_range_visit(
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
		self.0.state().range_bytes_visit(start, end, &mut |k, v| match remaining.as_mut() {
			Some(0) => Ok(()),
			Some(r) => {
				*r -= 1;
				visit(k, v).map_err(Into::into)
			}
			None => visit(k, v).map_err(Into::into),
		})?;
		Ok(())
	}

	fn intern_group(&mut self, group: &EncodedKey, position: GroupPosition) -> Result<GroupId> {
		Ok(self.0.intern_group(group, position)?)
	}

	fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
		Ok(self.0.lookup_group(group)?)
	}

	fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		Ok(self.0.get_or_create_row_number(group, key)?)
	}

	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		Ok(self.0.get_or_create_row_numbers(group, keys)?)
	}

	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		Ok(self.0.remove_row_number(group, key)?)
	}

	fn clock_now_nanos(&self) -> u64 {
		self.0.clock_now_nanos()
	}
}
