// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_core::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	window::store::WindowStore,
};
use reifydb_value::{Result, value::row_number::RowNumber};
use serde::{Serialize, de::DeserializeOwned};

use crate::operator::context::{InternalStateApi, OperatorContext, StateApi};

pub struct OperatorContextStore<'a, C: OperatorContext>(pub &'a mut C);

impl<C: OperatorContext> WindowStore for OperatorContextStore<'_, C> {
	fn state_get<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Result<Option<V>> {
		Ok(self.0.state().get::<V>(key)?)
	}

	fn state_get_many_visit<V: DeserializeOwned>(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
	) -> Result<()> {
		self.0.state().get_many_visit::<V>(keys, &mut |k, v| visit(k, v).map_err(Into::into))?;
		Ok(())
	}

	fn state_set<V: Serialize>(&mut self, key: &EncodedKey, value: &V) -> Result<()> {
		self.0.state().set::<V>(key, value)?;
		Ok(())
	}

	fn state_remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.0.state().remove(key)?;
		Ok(())
	}

	fn internal_get<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Result<Option<V>> {
		Ok(self.0.internal_state().get::<V>(key)?)
	}

	fn internal_get_many_visit<V: DeserializeOwned>(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
	) -> Result<()> {
		self.0.internal_state().get_many_visit::<V>(keys, &mut |k, v| visit(k, v).map_err(Into::into))?;
		Ok(())
	}

	fn internal_set<V: Serialize>(&mut self, key: &EncodedKey, value: &V) -> Result<()> {
		self.0.internal_state().set::<V>(key, value)?;
		Ok(())
	}

	fn internal_remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.0.internal_state().remove(key)?;
		Ok(())
	}

	fn internal_range_visit<V: DeserializeOwned>(
		&mut self,
		range: EncodedKeyRange,
		visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
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
		self.0.internal_state().range_visit::<V>(start, end, &mut |k, v| visit(k, v).map_err(Into::into))?;
		Ok(())
	}

	fn get_or_create_row_number(&mut self, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		Ok(self.0.get_or_create_row_number(key)?)
	}

	fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		Ok(self.0.get_or_create_row_numbers(keys)?)
	}

	fn allocate_row_numbers(&mut self, count: u64) -> Result<RowNumber> {
		Ok(self.0.allocate_row_numbers(count)?)
	}

	fn clock_now_nanos(&self) -> u64 {
		self.0.clock_now_nanos()
	}
}
