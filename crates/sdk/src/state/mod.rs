// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod ffi;
pub mod keyed;
pub mod row;
pub mod single;
pub mod utils;
pub mod window;

use std::ops::Bound;

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::EncodedKey,
	state::{OperatorState, StateBytes, decode_state},
};
use reifydb_value::error::Error as ValueError;

use crate::{
	error::Result,
	operator::context::{InternalStateApi, OperatorContext, StateApi, ffi::FFIOperatorContext},
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

	pub fn get<T: OperatorState>(&self, key: &EncodedKey) -> Result<Option<T>> {
		match ffi::get(self.ctx, key)? {
			Some(row) => decode_payload(&row).map(Some),
			None => Ok(None),
		}
	}

	pub fn set<T: OperatorState>(&mut self, key: &EncodedKey, value: &T) -> Result<()> {
		let row = encode_payload(value, self.now_nanos())?;
		ffi::set(self.ctx, key, &row)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		ffi::remove(self.ctx, key)
	}

	pub fn drop(&mut self, key: &EncodedKey) -> Result<()> {
		ffi::drop(self.ctx, key)
	}

	pub fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Ok(ffi::get(self.ctx, key)?.is_some())
	}

	pub fn clear(&mut self) -> Result<()> {
		ffi::clear(self.ctx)
	}

	pub fn scan_prefix<T: OperatorState>(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, T)>> {
		ffi::prefix(self.ctx, prefix)?.into_iter().map(|(k, row)| Ok((k, decode_payload(&row)?))).collect()
	}

	pub fn get_many<T: OperatorState>(&self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, T)>> {
		ffi::get_many(self.ctx, keys)?.into_iter().map(|(k, row)| Ok((k, decode_payload(&row)?))).collect()
	}

	pub fn keys_with_prefix(&self, prefix: &EncodedKey) -> Result<Vec<EncodedKey>> {
		Ok(ffi::prefix(self.ctx, prefix)?.into_iter().map(|(k, _)| k).collect())
	}

	pub fn range<T: OperatorState>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, T)>> {
		ffi::range(self.ctx, start, end)?.into_iter().map(|(k, row)| Ok((k, decode_payload(&row)?))).collect()
	}

	pub fn get_bytes(&self, key: &EncodedKey) -> Result<Option<StateBytes>> {
		match ffi::get(self.ctx, key)? {
			Some(row) => Ok(Some(StateBytes::from_row(row).map_err(ValueError::from)?)),
			None => Ok(None),
		}
	}

	pub fn set_bytes(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()> {
		ffi::set(self.ctx, key, &payload.into_row())
	}

	pub fn get_many_bytes_visit(
		&self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		for (k, row) in ffi::get_many(self.ctx, keys)? {
			visit(k, StateBytes::from_row(row).map_err(ValueError::from)?)?;
		}
		Ok(())
	}

	#[inline]
	fn now_nanos(&self) -> u64 {
		unsafe { (*self.ctx.ctx).clock_now_nanos }
	}
}

/// Operator-internal sequence-and-mapping state, stored under
/// `FlowNodeInternalStateKey` instead of `FlowNodeStateKey`. Use this for
/// state that must outlive operator TTL GC (e.g. `RowNumberProvider`'s
/// monotonic counter and `EncodedKey -> RowNumber` mappings).
///
/// The host wraps each user-supplied key in
/// `FlowNodeInternalStateKey(operator_id, ...)` so callers pass only the
/// inner-tag bytes.
pub struct InternalState<'a> {
	ctx: &'a mut FFIOperatorContext,
}

impl<'a> InternalState<'a> {
	pub(crate) fn new(ctx: &'a mut FFIOperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	pub fn get<T: OperatorState>(&self, key: &EncodedKey) -> Result<Option<T>> {
		match ffi::internal_get(self.ctx, key)? {
			Some(row) => decode_payload(&row).map(Some),
			None => Ok(None),
		}
	}

	pub fn get_many<T: OperatorState>(&self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, T)>> {
		ffi::internal_get_many(self.ctx, keys)?
			.into_iter()
			.map(|(k, row)| Ok((k, decode_payload(&row)?)))
			.collect()
	}

	pub fn set<T: OperatorState>(&mut self, key: &EncodedKey, value: &T) -> Result<()> {
		let row = encode_payload(value, self.now_nanos())?;
		ffi::internal_set(self.ctx, key, &row)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		ffi::internal_remove(self.ctx, key)
	}

	pub fn drop(&mut self, key: &EncodedKey) -> Result<()> {
		ffi::internal_drop(self.ctx, key)
	}

	pub fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Ok(ffi::internal_get(self.ctx, key)?.is_some())
	}

	pub fn range<T: OperatorState>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, T)>> {
		ffi::internal_range(self.ctx, start, end)?
			.into_iter()
			.map(|(k, row)| Ok((k, decode_payload(&row)?)))
			.collect()
	}

	pub fn get_bytes(&self, key: &EncodedKey) -> Result<Option<StateBytes>> {
		match ffi::internal_get(self.ctx, key)? {
			Some(row) => Ok(Some(StateBytes::from_row(row).map_err(ValueError::from)?)),
			None => Ok(None),
		}
	}

	pub fn set_bytes(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()> {
		ffi::internal_set(self.ctx, key, &payload.into_row())
	}

	pub fn get_many_bytes_visit(
		&self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		for (k, row) in ffi::internal_get_many(self.ctx, keys)? {
			visit(k, StateBytes::from_row(row).map_err(ValueError::from)?)?;
		}
		Ok(())
	}

	pub fn range_bytes_visit(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
	) -> Result<()> {
		for (k, row) in ffi::internal_range(self.ctx, start, end)? {
			visit(k, StateBytes::from_row(row).map_err(ValueError::from)?)?;
		}
		Ok(())
	}

	#[inline]
	fn now_nanos(&self) -> u64 {
		unsafe { (*self.ctx.ctx).clock_now_nanos }
	}
}

#[inline]
pub fn encode_payload<T: OperatorState>(value: &T, now_nanos: u64) -> Result<EncodedRow> {
	let bytes = value.encode_state(now_nanos).map_err(ValueError::from)?;
	Ok(bytes.into_row())
}

#[inline]
pub fn decode_payload<T: OperatorState>(row: &EncodedRow) -> Result<T> {
	let bytes = StateBytes::from_row(row.clone()).map_err(ValueError::from)?;
	Ok(decode_state(&bytes).map_err(ValueError::from)?)
}

pub trait RawStatefulOperator {
	fn state_get<T: OperatorState>(&self, ctx: &mut impl OperatorContext, key: &EncodedKey) -> Result<Option<T>> {
		ctx.state().get(key)
	}

	fn state_set<T: OperatorState>(
		&self,
		ctx: &mut impl OperatorContext,
		key: &EncodedKey,
		value: &T,
	) -> Result<()> {
		ctx.state().set(key, value)
	}

	fn state_remove(&self, ctx: &mut impl OperatorContext, key: &EncodedKey) -> Result<()> {
		ctx.state().remove(key)
	}

	fn state_scan_prefix<T: OperatorState>(
		&self,
		ctx: &mut impl OperatorContext,
		prefix: &EncodedKey,
	) -> Result<Vec<(EncodedKey, T)>> {
		ctx.state().scan_prefix(prefix)
	}

	fn state_keys_with_prefix(
		&self,
		ctx: &mut impl OperatorContext,
		prefix: &EncodedKey,
	) -> Result<Vec<EncodedKey>> {
		ctx.state().keys_with_prefix(prefix)
	}

	fn state_contains(&self, ctx: &mut impl OperatorContext, key: &EncodedKey) -> Result<bool> {
		ctx.state().contains(key)
	}

	fn state_clear(&self, ctx: &mut impl OperatorContext) -> Result<()> {
		ctx.state().clear()
	}

	fn state_scan_range<T: OperatorState>(
		&self,
		ctx: &mut impl OperatorContext,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, T)>> {
		ctx.state().range(start, end)
	}

	// `internal_state_*` mirrors the regular `state_*` surface but routes
	// through `ctx.internal_state()`, which lives in
	// `FlowNodeInternalStateKey` (outside operator TTL GC). Use for
	// monotonic sequences, identity bindings, and watermarks.

	fn internal_state_get<T: OperatorState>(
		&self,
		ctx: &mut impl OperatorContext,
		key: &EncodedKey,
	) -> Result<Option<T>> {
		ctx.internal_state().get(key)
	}

	fn internal_state_set<T: OperatorState>(
		&self,
		ctx: &mut impl OperatorContext,
		key: &EncodedKey,
		value: &T,
	) -> Result<()> {
		ctx.internal_state().set(key, value)
	}

	fn internal_state_remove(&self, ctx: &mut impl OperatorContext, key: &EncodedKey) -> Result<()> {
		ctx.internal_state().remove(key)
	}

	fn internal_state_contains(&self, ctx: &mut impl OperatorContext, key: &EncodedKey) -> Result<bool> {
		ctx.internal_state().contains(key)
	}
}
