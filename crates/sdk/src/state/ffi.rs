// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{ops::Bound, ptr, ptr::null_mut, slice::from_raw_parts};

use reifydb_abi::{
	constants::{FFI_END_OF_ITERATION, FFI_NOT_FOUND, FFI_OK},
	context::iterators::StateIteratorFFI,
	data::buffer::BufferFFI,
};
use reifydb_core::encoded::{key::EncodedKey, row::EncodedRow};
use reifydb_type::util::cowvec::CowVec;
use tracing::{Span, instrument};

use crate::{
	error::{FFIError, Result},
	operator::context::OperatorContext,
};

#[instrument(name = "flow::operator::state::ffi:get", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len(),
	found
))]
pub(crate) fn get(ctx: &OperatorContext, key: &EncodedKey) -> Result<Option<EncodedRow>> {
	let key_bytes = key.as_bytes();
	let mut output = BufferFFI {
		ptr: null_mut(),
		len: 0,
		cap: 0,
	};

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.get)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
			&mut output,
		);

		if result == FFI_OK {
			if output.ptr.is_null() || output.len == 0 {
				Span::current().record("found", false);
				Ok(None)
			} else {
				let value_bytes = from_raw_parts(output.ptr, output.len).to_vec();

				((*ctx.ctx).callbacks.memory.free)(output.ptr as *mut u8, output.len);
				Span::current().record("found", true);
				Ok(Some(EncodedRow(CowVec::new(value_bytes))))
			}
		} else if result == FFI_NOT_FOUND {
			Span::current().record("found", false);
			Ok(None)
		} else {
			Err(FFIError::Other(format!("host_state_get failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::state::ffi:set", level = "trace", skip(ctx, value), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len(),
	value_len = value.as_ref().len()
))]
pub(crate) fn set(ctx: &mut OperatorContext, key: &EncodedKey, value: &EncodedRow) -> Result<()> {
	let key_bytes = key.as_bytes();
	let value_bytes = value.as_ref();

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.set)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
			value_bytes.as_ptr(),
			value_bytes.len(),
		);

		if result == FFI_OK {
			Ok(())
		} else {
			Err(FFIError::Other(format!("host_state_set failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::state::ffi::remove", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len()
))]
pub(crate) fn remove(ctx: &mut OperatorContext, key: &EncodedKey) -> Result<()> {
	let key_bytes = key.as_bytes();

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.remove)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
		);

		if result == FFI_OK {
			Ok(())
		} else {
			Err(FFIError::Other(format!("host_state_remove failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::state::ffi:prefix", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	prefix_len = prefix.as_bytes().len(),
	result_count
))]
pub(crate) fn prefix(ctx: &OperatorContext, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedRow)>> {
	let prefix_bytes = prefix.as_bytes();
	let mut iterator: *mut StateIteratorFFI = null_mut();

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.prefix)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			prefix_bytes.as_ptr(),
			prefix_bytes.len(),
			&mut iterator,
		);

		if result != FFI_OK {
			return Err(FFIError::Other(format!("host_state_prefix failed with code {}", result)));
		}

		if iterator.is_null() {
			Span::current().record("result_count", 0);
			return Ok(Vec::new());
		}

		let mut results = Vec::new();

		loop {
			let mut key_buf = BufferFFI {
				ptr: null_mut(),
				len: 0,
				cap: 0,
			};
			let mut value_buf = BufferFFI {
				ptr: null_mut(),
				len: 0,
				cap: 0,
			};

			let next_result =
				((*ctx.ctx).callbacks.state.iterator_next)(iterator, &mut key_buf, &mut value_buf);

			if next_result == FFI_END_OF_ITERATION {
				break;
			} else if next_result != FFI_OK {
				((*ctx.ctx).callbacks.state.iterator_free)(iterator);
				return Err(FFIError::Other(format!(
					"host_state_iterator_next failed with code {}",
					next_result
				)));
			}

			if !key_buf.ptr.is_null() && key_buf.len > 0 {
				let key_bytes = from_raw_parts(key_buf.ptr, key_buf.len).to_vec();
				let key = EncodedKey(CowVec::new(key_bytes));

				let value = if !value_buf.ptr.is_null() && value_buf.len > 0 {
					let value_bytes = from_raw_parts(value_buf.ptr, value_buf.len).to_vec();
					EncodedRow(CowVec::new(value_bytes))
				} else {
					EncodedRow(CowVec::new(Vec::new()))
				};

				((*ctx.ctx).callbacks.memory.free)(key_buf.ptr as *mut u8, key_buf.len);
				if !value_buf.ptr.is_null() && value_buf.len > 0 {
					((*ctx.ctx).callbacks.memory.free)(value_buf.ptr as *mut u8, value_buf.len);
				}

				results.push((key, value));
			}
		}

		((*ctx.ctx).callbacks.state.iterator_free)(iterator);
		Span::current().record("result_count", results.len());
		Ok(results)
	}
}

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

#[instrument(name = "flow::operator::state::ffi::range", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	result_count
))]
pub(crate) fn range(
	ctx: &OperatorContext,
	start: Bound<&EncodedKey>,
	end: Bound<&EncodedKey>,
) -> Result<Vec<(EncodedKey, EncodedRow)>> {
	let mut iterator: *mut StateIteratorFFI = null_mut();

	unsafe {
		let (start_ptr, start_len, start_bound_type) = match start {
			Bound::Unbounded => (ptr::null(), 0, BOUND_UNBOUNDED),
			Bound::Included(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_INCLUDED),
			Bound::Excluded(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_EXCLUDED),
		};

		let (end_ptr, end_len, end_bound_type) = match end {
			Bound::Unbounded => (ptr::null(), 0, BOUND_UNBOUNDED),
			Bound::Included(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_INCLUDED),
			Bound::Excluded(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_EXCLUDED),
		};

		let result = ((*ctx.ctx).callbacks.state.range)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			start_ptr,
			start_len,
			start_bound_type,
			end_ptr,
			end_len,
			end_bound_type,
			&mut iterator,
		);

		if result != FFI_OK {
			return Err(FFIError::Other(format!("host_state_range failed with code {}", result)));
		}

		if iterator.is_null() {
			Span::current().record("result_count", 0);
			return Ok(Vec::new());
		}

		let mut results = Vec::new();

		loop {
			let mut key_buf = BufferFFI {
				ptr: null_mut(),
				len: 0,
				cap: 0,
			};
			let mut value_buf = BufferFFI {
				ptr: null_mut(),
				len: 0,
				cap: 0,
			};

			let next_result =
				((*ctx.ctx).callbacks.state.iterator_next)(iterator, &mut key_buf, &mut value_buf);

			if next_result == FFI_END_OF_ITERATION {
				break;
			} else if next_result != FFI_OK {
				((*ctx.ctx).callbacks.state.iterator_free)(iterator);
				return Err(FFIError::Other(format!(
					"host_state_iterator_next failed with code {}",
					next_result
				)));
			}

			if !key_buf.ptr.is_null() && key_buf.len > 0 {
				let key_bytes = from_raw_parts(key_buf.ptr, key_buf.len).to_vec();
				let key = EncodedKey(CowVec::new(key_bytes));

				let value = if !value_buf.ptr.is_null() && value_buf.len > 0 {
					let value_bytes = from_raw_parts(value_buf.ptr, value_buf.len).to_vec();
					EncodedRow(CowVec::new(value_bytes))
				} else {
					EncodedRow(CowVec::new(Vec::new()))
				};

				((*ctx.ctx).callbacks.memory.free)(key_buf.ptr as *mut u8, key_buf.len);
				if !value_buf.ptr.is_null() && value_buf.len > 0 {
					((*ctx.ctx).callbacks.memory.free)(value_buf.ptr as *mut u8, value_buf.len);
				}

				results.push((key, value));
			}
		}

		((*ctx.ctx).callbacks.state.iterator_free)(iterator);
		Span::current().record("result_count", results.len());
		Ok(results)
	}
}

#[instrument(name = "flow::operator::state::ffi::clear", level = "debug", skip(ctx), fields(
	operator_id = ctx.operator_id().0
))]
pub(crate) fn clear(ctx: &mut OperatorContext) -> Result<()> {
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.clear)((*ctx.ctx).operator_id, ctx.ctx);

		if result == FFI_OK {
			Ok(())
		} else {
			Err(FFIError::Other(format!("host_state_clear failed with code {}", result)))
		}
	}
}
