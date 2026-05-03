// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
	error::{FFIError, Result},
	operator::{FFIOperator, context::OperatorContext},
};

pub struct StateEntry<T> {
	pub value: T,
	pub created_at_nanos: u64,
	pub updated_at_nanos: u64,
}

pub struct State<'a> {
	ctx: &'a mut OperatorContext,
}

impl<'a> State<'a> {
	pub(crate) fn new(ctx: &'a mut OperatorContext) -> Self {
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

#[inline]
fn encode_payload<T: Serialize>(value: &T, now_nanos: u64) -> Result<EncodedRow> {
	let bytes = to_allocvec(value)
		.map_err(|e| FFIError::Serialization(format!("operator state serialization failed: {}", e)))?;
	let shape = RowShape::operator_state();
	let mut row = shape.allocate();
	shape.set_blob(&mut row, 0, &Blob::new(bytes));
	row.set_timestamps(now_nanos, now_nanos);
	Ok(row)
}

#[inline]
fn decode_payload<T: DeserializeOwned>(row: &EncodedRow) -> Result<T> {
	let shape = RowShape::operator_state();
	let blob = shape.get_blob(row, 0);
	from_bytes(blob.as_bytes())
		.map_err(|e| FFIError::Serialization(format!("operator state deserialization failed: {}", e)))
}

pub trait FFIRawStatefulOperator: FFIOperator {
	fn state_get<T: DeserializeOwned>(&self, ctx: &mut OperatorContext, key: &EncodedKey) -> Result<Option<T>> {
		ctx.state().get(key)
	}

	fn state_set<T: Serialize>(&self, ctx: &mut OperatorContext, key: &EncodedKey, value: &T) -> Result<()> {
		ctx.state().set(key, value)
	}

	fn state_remove(&self, ctx: &mut OperatorContext, key: &EncodedKey) -> Result<()> {
		ctx.state().remove(key)
	}

	fn state_scan_prefix<T: DeserializeOwned>(
		&self,
		ctx: &mut OperatorContext,
		prefix: &EncodedKey,
	) -> Result<Vec<(EncodedKey, T)>> {
		ctx.state().scan_prefix(prefix)
	}

	fn state_keys_with_prefix(&self, ctx: &mut OperatorContext, prefix: &EncodedKey) -> Result<Vec<EncodedKey>> {
		ctx.state().keys_with_prefix(prefix)
	}

	fn state_contains(&self, ctx: &mut OperatorContext, key: &EncodedKey) -> Result<bool> {
		ctx.state().contains(key)
	}

	fn state_clear(&self, ctx: &mut OperatorContext) -> Result<()> {
		ctx.state().clear()
	}

	fn state_scan_range<T: DeserializeOwned>(
		&self,
		ctx: &mut OperatorContext,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, T)>> {
		ctx.state().range(start, end)
	}
}
