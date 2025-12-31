// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Raw FFI state operations
//!
//! Low-level state operations that directly call host FFI callbacks.
//! These functions should not be used directly - use the State API instead.

use std::{ptr::null_mut, slice::from_raw_parts};

use reifydb_abi::{BufferFFI, FFI_END_OF_ITERATION, FFI_NOT_FOUND, FFI_OK, StateIteratorFFI};
use reifydb_core::{
	CowVec,
	value::encoded::{EncodedKey, EncodedValues},
};
use tracing::instrument;

use crate::{
	OperatorContext,
	error::{FFIError, Result},
};

/// Get a value from state by key
#[instrument(name = "flow::operator::state::get", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len(),
	found
))]
pub(crate) fn raw_state_get(ctx: &OperatorContext, key: &EncodedKey) -> Result<Option<EncodedValues>> {
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
			// Success - value found
			if output.ptr.is_null() || output.len == 0 {
				tracing::Span::current().record("found", false);
				Ok(None)
			} else {
				let value_bytes = from_raw_parts(output.ptr, output.len).to_vec();
				// Free the buffer allocated by host
				((*ctx.ctx).callbacks.memory.free)(output.ptr as *mut u8, output.len);
				tracing::Span::current().record("found", true);
				Ok(Some(EncodedValues(CowVec::new(value_bytes))))
			}
		} else if result == FFI_NOT_FOUND {
			// Key not found
			tracing::Span::current().record("found", false);
			Ok(None)
		} else {
			Err(FFIError::Other(format!("host_state_get failed with code {}", result)))
		}
	}
}

/// Set a value in state by key
#[instrument(name = "flow::operator::state::set", level = "trace", skip(ctx, value), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len(),
	value_len = value.as_ref().len()
))]
pub(crate) fn raw_state_set(ctx: &mut OperatorContext, key: &EncodedKey, value: &EncodedValues) -> Result<()> {
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

/// Remove a value from state by key
#[instrument(name = "flow::operator::state::remove", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len()
))]
pub(crate) fn raw_state_remove(ctx: &mut OperatorContext, key: &EncodedKey) -> Result<()> {
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

/// Scan all keys with a given prefix
#[instrument(name = "flow::operator::state::prefix", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	prefix_len = prefix.as_bytes().len(),
	result_count
))]
pub(crate) fn raw_state_prefix(ctx: &OperatorContext, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedValues)>> {
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
			tracing::Span::current().record("result_count", 0);
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
				// End of iteration
				break;
			} else if next_result != FFI_OK {
				((*ctx.ctx).callbacks.state.iterator_free)(iterator);
				return Err(FFIError::Other(format!(
					"host_state_iterator_next failed with code {}",
					next_result
				)));
			}

			// Convert buffers to owned data
			if !key_buf.ptr.is_null() && key_buf.len > 0 {
				let key_bytes = from_raw_parts(key_buf.ptr, key_buf.len).to_vec();
				let key = EncodedKey(CowVec::new(key_bytes));

				let value = if !value_buf.ptr.is_null() && value_buf.len > 0 {
					let value_bytes = from_raw_parts(value_buf.ptr, value_buf.len).to_vec();
					EncodedValues(CowVec::new(value_bytes))
				} else {
					EncodedValues(CowVec::new(Vec::new()))
				};

				// Free buffers allocated by host
				((*ctx.ctx).callbacks.memory.free)(key_buf.ptr as *mut u8, key_buf.len);
				if !value_buf.ptr.is_null() && value_buf.len > 0 {
					((*ctx.ctx).callbacks.memory.free)(value_buf.ptr as *mut u8, value_buf.len);
				}

				results.push((key, value));
			}
		}

		((*ctx.ctx).callbacks.state.iterator_free)(iterator);
		tracing::Span::current().record("result_count", results.len());
		Ok(results)
	}
}

/// Clear all state for this operator
#[instrument(name = "flow::operator::state::clear", level = "debug", skip(ctx), fields(
	operator_id = ctx.operator_id().0
))]
pub(crate) fn raw_state_clear(ctx: &mut OperatorContext) -> Result<()> {
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.clear)((*ctx.ctx).operator_id, ctx.ctx);

		if result == FFI_OK {
			Ok(())
		} else {
			Err(FFIError::Other(format!("host_state_clear failed with code {}", result)))
		}
	}
}
