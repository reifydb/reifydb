// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod ffi;
pub mod keyed;
pub mod single;
pub mod utils;
pub mod window;

use std::ops::Bound;

use reifydb_codec::{
	encoded::bytes::EncodedBytes,
	key::encoded::EncodedKey,
	state::{OperatorState, StateBytes, decode_state},
};
use reifydb_core::key::operator_group_state::GroupStateKey;
use reifydb_value::{error::Error as ValueError, value::datetime::DateTime};

use crate::{
	error::Result,
	operator::context::{OperatorContext, StateApi, ffi::FFIOperatorContext},
};

pub struct State<'a> {
	ctx: &'a mut FFIOperatorContext,
}

impl<'a> State<'a> {
	pub(crate) fn new(ctx: &'a mut FFIOperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	pub fn get<T: OperatorState>(&self, key: &GroupStateKey) -> Result<Option<T>> {
		match ffi::get(self.ctx, key.as_encoded())? {
			Some(row) => decode_payload(&row).map(Some),
			None => Ok(None),
		}
	}

	pub fn set<T: OperatorState>(&mut self, key: &GroupStateKey, value: &T) -> Result<()> {
		let row = encode_payload(value, self.written_at())?;
		ffi::set(self.ctx, key.as_encoded(), &row)
	}

	pub fn remove(&mut self, key: &GroupStateKey) -> Result<()> {
		ffi::remove(self.ctx, key.as_encoded())
	}

	pub fn contains(&self, key: &GroupStateKey) -> Result<bool> {
		Ok(ffi::get(self.ctx, key.as_encoded())?.is_some())
	}

	pub fn clear(&mut self) -> Result<()> {
		ffi::clear(self.ctx)
	}

	pub fn scan_prefix<T: OperatorState>(&self, prefix: &GroupStateKey) -> Result<Vec<(GroupStateKey, T)>> {
		ffi::prefix(self.ctx, prefix.as_encoded())?
			.into_iter()
			.filter_map(|(k, row)| GroupStateKey::from_framed(k).map(|k| (k, row)))
			.map(|(k, row)| Ok((k, decode_payload(&row)?)))
			.collect()
	}

	pub fn get_many<T: OperatorState>(&self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, T)>> {
		let raw: Vec<EncodedKey> = keys.iter().map(|k| k.as_encoded().clone()).collect();
		ffi::get_many(self.ctx, &raw)?
			.into_iter()
			.filter_map(|(k, row)| GroupStateKey::from_framed(k).map(|k| (k, row)))
			.map(|(k, row)| Ok((k, decode_payload(&row)?)))
			.collect()
	}

	pub fn keys_with_prefix(&self, prefix: &GroupStateKey) -> Result<Vec<GroupStateKey>> {
		Ok(ffi::prefix(self.ctx, prefix.as_encoded())?
			.into_iter()
			.filter_map(|(k, _)| GroupStateKey::from_framed(k))
			.collect())
	}

	pub fn range<T: OperatorState>(
		&self,
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
	) -> Result<Vec<(GroupStateKey, T)>> {
		ffi::range(self.ctx, start.map(GroupStateKey::as_encoded), end.map(GroupStateKey::as_encoded))?
			.into_iter()
			.filter_map(|(k, row)| GroupStateKey::from_framed(k).map(|k| (k, row)))
			.map(|(k, row)| Ok((k, decode_payload(&row)?)))
			.collect()
	}

	pub fn get_bytes(&self, key: &GroupStateKey) -> Result<Option<StateBytes>> {
		match ffi::get(self.ctx, key.as_encoded())? {
			Some(row) => Ok(Some(StateBytes::from_bytes(row).map_err(ValueError::from)?)),
			None => Ok(None),
		}
	}

	pub fn set_bytes(&mut self, key: &GroupStateKey, payload: StateBytes) -> Result<()> {
		ffi::set(self.ctx, key.as_encoded(), &payload.into_bytes())
	}

	pub fn get_many_bytes_visit(
		&self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		let raw: Vec<EncodedKey> = keys.iter().map(|k| k.as_encoded().clone()).collect();
		for (k, row) in ffi::get_many(self.ctx, &raw)? {
			let Some(k) = GroupStateKey::from_framed(k) else {
				continue;
			};
			visit(k, StateBytes::from_bytes(row).map_err(ValueError::from)?)?;
		}
		Ok(())
	}

	pub fn range_bytes_visit(
		&self,
		start: Bound<&GroupStateKey>,
		end: Bound<&GroupStateKey>,
		visit: &mut dyn FnMut(GroupStateKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		for (k, row) in
			ffi::range(self.ctx, start.map(GroupStateKey::as_encoded), end.map(GroupStateKey::as_encoded))?
		{
			let Some(k) = GroupStateKey::from_framed(k) else {
				continue;
			};
			visit(k, StateBytes::from_bytes(row).map_err(ValueError::from)?)?;
		}
		Ok(())
	}

	#[inline]
	fn written_at(&self) -> DateTime {
		// SAFETY: FFIOperatorContext::new asserts ctx.ctx is non-null, and the host keeps the ContextFFI alive
		// and aligned for at least the lifetime of the borrow this State was created from.
		DateTime::from_nanos(unsafe { (*self.ctx.ctx).written_at_nanos })
	}
}

#[inline]
pub fn encode_payload<T: OperatorState>(value: &T, now: DateTime) -> Result<EncodedBytes> {
	let bytes = value.encode_state(now).map_err(ValueError::from)?;
	Ok(bytes.into_bytes())
}

#[inline]
pub fn decode_payload<T: OperatorState>(row: &EncodedBytes) -> Result<T> {
	let bytes = StateBytes::from_bytes(row.clone()).map_err(ValueError::from)?;
	Ok(decode_state(&bytes).map_err(ValueError::from)?)
}

pub trait RawStatefulOperator {
	fn state_get<T: OperatorState>(
		&self,
		ctx: &mut impl OperatorContext,
		key: &GroupStateKey,
	) -> Result<Option<T>> {
		ctx.state().get(key)
	}

	fn state_set<T: OperatorState>(
		&self,
		ctx: &mut impl OperatorContext,
		key: &GroupStateKey,
		value: &T,
	) -> Result<()> {
		ctx.state().set(key, value)
	}

	fn state_remove(&self, ctx: &mut impl OperatorContext, key: &GroupStateKey) -> Result<()> {
		ctx.state().remove(key)
	}

	fn state_clear(&self, ctx: &mut impl OperatorContext) -> Result<()> {
		ctx.state().clear()
	}
}
