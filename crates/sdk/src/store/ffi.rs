// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, ptr, ptr::null_mut, slice::from_raw_parts};

use reifydb_abi::{
	constants::{FFI_END_OF_ITERATION, FFI_NOT_FOUND, FFI_OK},
	context::iterators::StoreIteratorFFI,
	data::buffer::BufferFFI,
};
use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey};
use reifydb_value::util::cowvec::CowVec;
use tracing::{Span, instrument};

use crate::{
	error::{Result, SdkError},
	operator::context::ffi::FFIOperatorContext,
};

pub(super) fn raw_store_get(ctx: &FFIOperatorContext, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
	let key_bytes = key.as_bytes();
	let mut output = BufferFFI {
		ptr: null_mut(),
		len: 0,
		cap: 0,
	};

	// SAFETY: FFIOperatorContext::new asserts ctx.ctx is non-null and the host keeps the ContextFFI valid for the
	// whole guest call; key_bytes outlives the callback, which only reads it. On FFI_OK the host writes a buffer of
	// output.len initialised bytes, copied out before memory.free releases it with the length it was allocated at.
	unsafe {
		let result =
			((*ctx.ctx).callbacks.store.get)(ctx.ctx, key_bytes.as_ptr(), key_bytes.len(), &mut output);

		if result == FFI_OK {
			if output.ptr.is_null() || output.len == 0 {
				Ok(None)
			} else {
				let value_bytes = from_raw_parts(output.ptr, output.len).to_vec();

				((*ctx.ctx).callbacks.memory.free)(output.ptr as *mut u8, output.len);
				Ok(Some(EncodedBytes(CowVec::new(value_bytes))))
			}
		} else if result == FFI_NOT_FOUND {
			Ok(None)
		} else {
			Err(SdkError::Other(format!("host_store_get failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::store::raw::contains_key", level = "trace", skip(ctx), fields(
	key_len = key.as_bytes().len()
))]
pub(super) fn raw_store_contains_key(ctx: &FFIOperatorContext, key: &EncodedKey) -> Result<bool> {
	let key_bytes = key.as_bytes();
	let mut result_byte: u8 = 0;

	// SAFETY: FFIOperatorContext::new asserts ctx.ctx is non-null and the host keeps the ContextFFI valid for the
	// whole guest call; key_bytes outlives the callback, and result_byte is a live local slot the host writes.
	unsafe {
		let result = ((*ctx.ctx).callbacks.store.contains_key)(
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
			&mut result_byte,
		);

		if result == FFI_OK {
			Ok(result_byte != 0)
		} else {
			Err(SdkError::Other(format!("host_store_contains_key failed with code {}", result)))
		}
	}
}

#[instrument(name = "flow::operator::store::raw::prefix", level = "debug", skip(ctx), fields(
	prefix_len = prefix.as_bytes().len()
))]
pub(super) fn raw_store_prefix(
	ctx: &FFIOperatorContext,
	prefix: &EncodedKey,
) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
	let prefix_bytes = prefix.as_bytes();
	let mut iterator: *mut StoreIteratorFFI = null_mut();

	// SAFETY: FFIOperatorContext::new asserts ctx.ctx is non-null and the host keeps the ContextFFI valid for the
	// whole guest call; prefix_bytes outlives the callback. The handle the host opens is passed once to
	// collect_iterator_results, discharging its precondition that the handle is fresh and freed exactly there.
	unsafe {
		let result = ((*ctx.ctx).callbacks.store.prefix)(
			ctx.ctx,
			prefix_bytes.as_ptr(),
			prefix_bytes.len(),
			&mut iterator,
		);

		if result < 0 {
			return Err(SdkError::Other(format!("host_store_prefix failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

#[instrument(name = "flow::operator::store::raw::range", level = "debug", skip(ctx, start, end))]
pub(super) fn raw_store_range(
	ctx: &FFIOperatorContext,
	start: Bound<&EncodedKey>,
	end: Bound<&EncodedKey>,
) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
	let mut iterator: *mut StoreIteratorFFI = null_mut();

	// SAFETY: FFIOperatorContext::new asserts ctx.ctx is non-null and the host keeps the ContextFFI valid for the
	// whole guest call; each bound pointer is null with length 0 or borrows a key that outlives the callback. The
	// handle the host opens is passed once to collect_iterator_results, which owns and frees it.
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

		let result = ((*ctx.ctx).callbacks.store.range)(
			ctx.ctx,
			start_ptr,
			start_len,
			start_bound_type,
			end_ptr,
			end_len,
			end_bound_type,
			&mut iterator,
		);

		if result < 0 {
			return Err(SdkError::Other(format!("host_store_range failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

/// # Safety
///
/// `ctx.ctx` must point to a live `ContextFFI` whose store and memory callbacks
/// are valid, and `iterator` must be null or an open handle from that context's
/// store scan/range callback that has not yet been freed. This takes ownership
/// of the handle and frees it before returning, so the caller must not free it
/// again.
#[instrument(
	name = "flow::operator::store::collect_iterator",
	level = "debug",
	skip(ctx, iterator),
	fields(result_count)
)]
pub(super) unsafe fn collect_iterator_results(
	ctx: &FFIOperatorContext,
	iterator: *mut StoreIteratorFFI,
) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
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

		// SAFETY: iterator was checked non-null above and is still the open handle this call owns; key_buf and
		// value_buf are live local slots the host overwrites with buffers it allocated.
		let next_result =
			unsafe { ((*ctx.ctx).callbacks.store.iterator_next)(iterator, &mut key_buf, &mut value_buf) };

		if next_result == FFI_END_OF_ITERATION {
			break;
		} else if next_result != FFI_OK {
			// SAFETY: iterator is the open handle this call owns and has not been freed yet; the return
			// immediately after means it is freed exactly once.
			unsafe { ((*ctx.ctx).callbacks.store.iterator_free)(iterator) };
			return Err(SdkError::Other(format!(
				"host_store_iterator_next failed with code {}",
				next_result
			)));
		}

		if !key_buf.ptr.is_null() && key_buf.len > 0 {
			// SAFETY: the ptr/len the host just wrote describe key_buf.len initialised bytes it allocated,
			// checked non-null and non-empty above and not freed until after this copy.
			let key_bytes = unsafe { from_raw_parts(key_buf.ptr, key_buf.len) }.to_vec();
			let key = EncodedKey::new(key_bytes);

			let value = if !value_buf.ptr.is_null() && value_buf.len > 0 {
				// SAFETY: same host-allocated buffer contract as the key slice, with ptr and len
				// checked in this branch's condition.
				let value_bytes = unsafe { from_raw_parts(value_buf.ptr, value_buf.len) }.to_vec();
				EncodedBytes(CowVec::new(value_bytes))
			} else {
				EncodedBytes(CowVec::new(Vec::new()))
			};

			// SAFETY: key_buf was allocated by the host with cap == len, so len is the size the free
			// callback expects; the bytes were copied above and the pointer is not used again.
			unsafe { ((*ctx.ctx).callbacks.memory.free)(key_buf.ptr as *mut u8, key_buf.len) };
			if !value_buf.ptr.is_null() && value_buf.len > 0 {
				// SAFETY: same host-allocated, cap == len contract as the key buffer.
				unsafe { ((*ctx.ctx).callbacks.memory.free)(value_buf.ptr as *mut u8, value_buf.len) };
			}

			results.push((key, value));
		}
	}

	// SAFETY: iterator is the open handle this call owns; the loop only breaks on the path that has not freed it,
	// so this frees it exactly once.
	unsafe { ((*ctx.ctx).callbacks.store.iterator_free)(iterator) };
	Span::current().record("result_count", results.len());
	Ok(results)
}
