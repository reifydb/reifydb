// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, ptr, ptr::null_mut, slice::from_raw_parts};

use reifydb_abi::{
	constants::{FFI_END_OF_ITERATION, FFI_NOT_FOUND, FFI_OK},
	context::iterators::StateIteratorFFI,
	data::{
		buffer::BufferFFI,
		key_ref::KeyRefFFI,
		state::{StateEntryFFI, StateSliceFFI},
	},
};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_value::{util::cowvec::CowVec, value::row_number::RowNumber};
use tracing::{Span, instrument};

use crate::{
	error::{Result, SdkError},
	operator::context::ffi::FFIOperatorContext,
};

#[instrument(name = "flow::operator::state::ffi:get", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len(),
	found
))]
pub(crate) fn get(ctx: &FFIOperatorContext, key: &EncodedKey) -> Result<Option<EncodedRow>> {
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
			Err(SdkError::Other(format!("host_state_get failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::state::ffi:set", level = "trace", skip(ctx, value), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len(),
	value_len = value.as_ref().len()
))]
pub(crate) fn set(ctx: &mut FFIOperatorContext, key: &EncodedKey, value: &EncodedRow) -> Result<()> {
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
			Err(SdkError::Other(format!("host_state_set failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::state::ffi::remove", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len()
))]
pub(crate) fn remove(ctx: &mut FFIOperatorContext, key: &EncodedKey) -> Result<()> {
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
			Err(SdkError::Other(format!("host_state_remove failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::state::ffi:get_many", level = "debug", skip(ctx, keys), fields(
	operator_id = ctx.operator_id().0,
	key_count = keys.len(),
	result_count
))]
pub(crate) fn get_many(ctx: &FFIOperatorContext, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, EncodedRow)>> {
	if keys.is_empty() {
		Span::current().record("result_count", 0);
		return Ok(Vec::new());
	}

	let key_refs: Vec<KeyRefFFI> = keys
		.iter()
		.map(|key| {
			let bytes = key.as_bytes();
			KeyRefFFI {
				ptr: bytes.as_ptr(),
				len: bytes.len(),
			}
		})
		.collect();

	let mut iterator: *mut StateIteratorFFI = null_mut();

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.get_many)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_refs.as_ptr(),
			key_refs.len(),
			&mut iterator,
		);

		if result != FFI_OK {
			return Err(SdkError::Other(format!("host_state_get_many failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

#[instrument(name = "flow::operator::state::ffi:prefix", level = "debug", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	prefix_len = prefix.as_bytes().len(),
	result_count
))]
pub(crate) fn prefix(ctx: &FFIOperatorContext, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedRow)>> {
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
			return Err(SdkError::Other(format!("host_state_prefix failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

#[instrument(name = "flow::operator::state::ffi::range", level = "debug", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	result_count
))]
pub(crate) fn range(
	ctx: &FFIOperatorContext,
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
			return Err(SdkError::Other(format!("host_state_range failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

pub(crate) fn internal_range(
	ctx: &FFIOperatorContext,
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

		let result = ((*ctx.ctx).callbacks.state.internal_range)(
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
			return Err(SdkError::Other(format!("host_internal_state_range failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

#[instrument(
	name = "flow::operator::state::collect_iterator",
	level = "debug",
	skip(ctx, iterator),
	fields(result_count)
)]
unsafe fn collect_iterator_results(
	ctx: &FFIOperatorContext,
	iterator: *mut StateIteratorFFI,
) -> Result<Vec<(EncodedKey, EncodedRow)>> {
	if iterator.is_null() {
		Span::current().record("result_count", 0);
		return Ok(Vec::new());
	}

	const ITERATOR_BATCH_CAP: usize = 256;
	let empty = StateSliceFFI {
		ptr: ptr::null(),
		len: 0,
	};
	let mut batch = [StateEntryFFI {
		key: empty,
		value: empty,
	}; ITERATOR_BATCH_CAP];
	let mut results = Vec::new();

	loop {
		let mut out_len = 0usize;
		let next_result = unsafe {
			((*ctx.ctx).callbacks.state.iterator_next)(
				iterator,
				batch.as_mut_ptr(),
				ITERATOR_BATCH_CAP,
				&mut out_len,
			)
		};

		if next_result != FFI_OK && next_result != FFI_END_OF_ITERATION {
			unsafe { ((*ctx.ctx).callbacks.state.iterator_free)(iterator) };
			return Err(SdkError::Other(format!(
				"host_state_iterator_next failed with code {}",
				next_result
			)));
		}

		for entry in batch.iter().take(out_len) {
			if entry.key.ptr.is_null() || entry.key.len == 0 {
				continue;
			}
			// SAFETY: the host guarantees every returned slice points to memory

			let key_bytes = unsafe { from_raw_parts(entry.key.ptr, entry.key.len) }.to_vec();
			let value = if !entry.value.ptr.is_null() && entry.value.len > 0 {
				// SAFETY: same iterator-owned lifetime contract as the key slice.
				let value_bytes = unsafe { from_raw_parts(entry.value.ptr, entry.value.len) }.to_vec();
				EncodedRow(CowVec::new(value_bytes))
			} else {
				EncodedRow(CowVec::new(Vec::new()))
			};
			results.push((EncodedKey::new(key_bytes), value));
		}

		if next_result == FFI_END_OF_ITERATION {
			break;
		}
	}

	unsafe { ((*ctx.ctx).callbacks.state.iterator_free)(iterator) };
	Span::current().record("result_count", results.len());
	Ok(results)
}

#[instrument(name = "flow::operator::state::ffi::clear", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0
))]
pub(crate) fn clear(ctx: &mut FFIOperatorContext) -> Result<()> {
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.clear)((*ctx.ctx).operator_id, ctx.ctx);

		if result == FFI_OK {
			Ok(())
		} else {
			Err(SdkError::Other(format!("host_state_clear failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::internal_state::ffi:get", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len(),
	found
))]
pub(crate) fn internal_get(ctx: &FFIOperatorContext, key: &EncodedKey) -> Result<Option<EncodedRow>> {
	let key_bytes = key.as_bytes();
	let mut output = BufferFFI {
		ptr: null_mut(),
		len: 0,
		cap: 0,
	};

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.internal_get)(
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
			Err(SdkError::Other(format!("host_internal_state_get failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::internal_state::ffi:set", level = "trace", skip(ctx, value), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len(),
	value_len = value.as_ref().len()
))]
pub(crate) fn internal_set(ctx: &mut FFIOperatorContext, key: &EncodedKey, value: &EncodedRow) -> Result<()> {
	let key_bytes = key.as_bytes();
	let value_bytes = value.as_ref();

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.internal_set)(
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
			Err(SdkError::Other(format!("host_internal_state_set failed with code {}", result)))
		}
	}
}

pub(crate) fn get_or_create_row_numbers(
	ctx: &mut FFIOperatorContext,
	keys: &[EncodedKey],
) -> Result<Vec<(RowNumber, bool)>> {
	if keys.is_empty() {
		return Ok(Vec::new());
	}
	let key_refs: Vec<KeyRefFFI> = keys
		.iter()
		.map(|key| {
			let bytes = key.as_bytes();
			KeyRefFFI {
				ptr: bytes.as_ptr(),
				len: bytes.len(),
			}
		})
		.collect();
	let mut row_numbers = vec![0u64; keys.len()];
	let mut is_new = vec![0u8; keys.len()];

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.get_or_create_row_numbers)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_refs.as_ptr(),
			key_refs.len(),
			row_numbers.as_mut_ptr(),
			is_new.as_mut_ptr(),
		);
		if result != FFI_OK {
			return Err(SdkError::Other(format!(
				"host_get_or_create_row_numbers failed with code {}",
				result
			)));
		}
	}

	Ok(row_numbers.into_iter().zip(is_new).map(|(rn, new)| (RowNumber(rn), new != 0)).collect())
}

pub(crate) fn remove_row_number(ctx: &mut FFIOperatorContext, key: &EncodedKey) -> Result<()> {
	let key_bytes = key.as_bytes();
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.remove_row_number)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
		);
		if result == FFI_OK {
			Ok(())
		} else {
			Err(SdkError::Other(format!("host_remove_row_number failed with code {}", result)))
		}
	}
}

pub(crate) fn remove_row_numbers_below(ctx: &mut FFIOperatorContext, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
	let upper_bytes = upper.as_bytes();
	let mut output = BufferFFI {
		ptr: null_mut(),
		len: 0,
		cap: 0,
	};
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.remove_row_numbers_below)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			upper_bytes.as_ptr(),
			upper_bytes.len(),
			&mut output,
		);
		if result != FFI_OK {
			return Err(SdkError::Other(format!(
				"host_remove_row_numbers_below failed with code {}",
				result
			)));
		}
		if output.ptr.is_null() || output.len == 0 {
			return Ok(Vec::new());
		}
		let bytes = from_raw_parts(output.ptr, output.len);
		let dropped = bytes
			.chunks_exact(8)
			.map(|chunk| RowNumber(u64::from_le_bytes(chunk.try_into().unwrap())))
			.collect();
		((*ctx.ctx).callbacks.memory.free)(output.ptr as *mut u8, output.len);
		Ok(dropped)
	}
}

#[instrument(name = "flow::operator::internal_state::ffi::remove", level = "trace", skip(ctx), fields(
	operator_id = ctx.operator_id().0,
	key_len = key.as_bytes().len()
))]
pub(crate) fn internal_remove(ctx: &mut FFIOperatorContext, key: &EncodedKey) -> Result<()> {
	let key_bytes = key.as_bytes();

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.internal_remove)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
		);

		if result == FFI_OK {
			Ok(())
		} else {
			Err(SdkError::Other(format!("host_internal_state_remove failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::internal_state::ffi:get_many", level = "debug", skip(ctx, keys), fields(
	operator_id = ctx.operator_id().0,
	key_count = keys.len(),
	result_count
))]
pub(crate) fn internal_get_many(
	ctx: &FFIOperatorContext,
	keys: &[EncodedKey],
) -> Result<Vec<(EncodedKey, EncodedRow)>> {
	if keys.is_empty() {
		Span::current().record("result_count", 0);
		return Ok(Vec::new());
	}

	let key_refs: Vec<KeyRefFFI> = keys
		.iter()
		.map(|key| {
			let bytes = key.as_bytes();
			KeyRefFFI {
				ptr: bytes.as_ptr(),
				len: bytes.len(),
			}
		})
		.collect();

	let mut iterator: *mut StateIteratorFFI = null_mut();

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.internal_get_many)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_refs.as_ptr(),
			key_refs.len(),
			&mut iterator,
		);

		if result != FFI_OK {
			return Err(SdkError::Other(format!(
				"host_internal_state_get_many failed with code {}",
				result
			)));
		}

		collect_iterator_results(ctx, iterator)
	}
}
