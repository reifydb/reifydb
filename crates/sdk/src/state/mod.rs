// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod cache;
pub mod ffi;
pub mod keyed;
pub mod row;
pub mod single;
pub mod utils;
pub mod window;

use std::ops::Bound;

use postcard::{from_bytes, to_allocvec};
use reifydb_core::encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape};
use reifydb_type::value::blob::Blob;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	error::{Result, SdkError},
	operator::context::{InternalStateApi, OperatorContext, StateApi, ffi::FFIOperatorContext},
};

pub struct StateEntry<T> {
	pub value: T,
	pub created_at_nanos: u64,
	pub updated_at_nanos: u64,
}

pub struct State<'a> {
	ctx: &'a mut FFIOperatorContext,
}

impl<'a> State<'a> {
	pub(crate) fn new(ctx: &'a mut FFIOperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	pub fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> Result<Option<T>> {
		match ffi::get(self.ctx, key)? {
			Some(row) => decode_payload(&row).map(Some),
			None => Ok(None),
		}
	}

	pub fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> Result<()> {
		let row = encode_payload(value, self.now_nanos())?;
		ffi::set(self.ctx, key, &row)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		ffi::remove(self.ctx, key)
	}

	pub fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Ok(ffi::get(self.ctx, key)?.is_some())
	}

	pub fn clear(&mut self) -> Result<()> {
		ffi::clear(self.ctx)
	}

	pub fn scan_prefix<T: DeserializeOwned>(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, T)>> {
		ffi::prefix(self.ctx, prefix)?.into_iter().map(|(k, row)| Ok((k, decode_payload(&row)?))).collect()
	}

	pub fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, T)>> {
		ffi::get_many(self.ctx, keys)?.into_iter().map(|(k, row)| Ok((k, decode_payload(&row)?))).collect()
	}

	pub fn keys_with_prefix(&self, prefix: &EncodedKey) -> Result<Vec<EncodedKey>> {
		Ok(ffi::prefix(self.ctx, prefix)?.into_iter().map(|(k, _)| k).collect())
	}

	pub fn range<T: DeserializeOwned>(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, T)>> {
		ffi::range(self.ctx, start, end)?.into_iter().map(|(k, row)| Ok((k, decode_payload(&row)?))).collect()
	}

	pub fn get_with_anchors<T: DeserializeOwned>(&self, key: &EncodedKey) -> Result<Option<StateEntry<T>>> {
		match ffi::get(self.ctx, key)? {
			Some(row) => Ok(Some(StateEntry {
				created_at_nanos: row.created_at_nanos(),
				updated_at_nanos: row.updated_at_nanos(),
				value: decode_payload(&row)?,
			})),
			None => Ok(None),
		}
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

	pub fn get<T: DeserializeOwned>(&self, key: &EncodedKey) -> Result<Option<T>> {
		match ffi::internal_get(self.ctx, key)? {
			Some(row) => decode_payload(&row).map(Some),
			None => Ok(None),
		}
	}

	pub fn get_many<T: DeserializeOwned>(&self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, T)>> {
		ffi::internal_get_many(self.ctx, keys)?
			.into_iter()
			.map(|(k, row)| Ok((k, decode_payload(&row)?)))
			.collect()
	}

	pub fn set<T: Serialize>(&mut self, key: &EncodedKey, value: &T) -> Result<()> {
		let row = encode_payload(value, self.now_nanos())?;
		ffi::internal_set(self.ctx, key, &row)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		ffi::internal_remove(self.ctx, key)
	}

	pub fn contains(&self, key: &EncodedKey) -> Result<bool> {
		Ok(ffi::internal_get(self.ctx, key)?.is_some())
	}

	#[inline]
	fn now_nanos(&self) -> u64 {
		unsafe { (*self.ctx.ctx).clock_now_nanos }
	}
}

#[inline]
pub fn encode_payload<T: Serialize>(value: &T, now_nanos: u64) -> Result<EncodedRow> {
	let bytes = to_allocvec(value)
		.map_err(|e| SdkError::Serialization(format!("operator state serialization failed: {}", e)))?;
	let shape = RowShape::operator_state();
	let mut row = shape.allocate();
	shape.set_blob(&mut row, 0, &Blob::new(bytes));
	row.set_timestamps(now_nanos, now_nanos);
	Ok(row)
}

#[inline]
pub fn decode_payload<T: DeserializeOwned>(row: &EncodedRow) -> Result<T> {
	let shape = RowShape::operator_state();
	let blob = shape.get_blob(row, 0);
	from_bytes(blob.as_bytes())
		.map_err(|e| SdkError::Serialization(format!("operator state deserialization failed: {}", e)))
}

pub trait RawStatefulOperator {
	fn state_get<T: DeserializeOwned>(
		&self,
		ctx: &mut impl OperatorContext,
		key: &EncodedKey,
	) -> Result<Option<T>> {
		ctx.state().get(key)
	}

	fn state_set<T: Serialize>(&self, ctx: &mut impl OperatorContext, key: &EncodedKey, value: &T) -> Result<()> {
		ctx.state().set(key, value)
	}

	fn state_remove(&self, ctx: &mut impl OperatorContext, key: &EncodedKey) -> Result<()> {
		ctx.state().remove(key)
	}

	fn state_scan_prefix<T: DeserializeOwned>(
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

	fn state_scan_range<T: DeserializeOwned>(
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

	fn internal_state_get<T: DeserializeOwned>(
		&self,
		ctx: &mut impl OperatorContext,
		key: &EncodedKey,
	) -> Result<Option<T>> {
		ctx.internal_state().get(key)
	}

	fn internal_state_set<T: Serialize>(
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
