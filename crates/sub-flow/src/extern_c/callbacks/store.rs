// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, ptr, slice::from_raw_parts};

use reifydb_abi::{
	constants::{
		EXTERN_C_END_OF_ITERATION, EXTERN_C_ERROR_ALLOC, EXTERN_C_ERROR_INTERNAL, EXTERN_C_ERROR_NULL_PTR, EXTERN_C_NOT_FOUND, EXTERN_C_OK,
	},
	context::{context::ExternCContext, iterators::ExternCStoreIterator},
	data::buffer::ExternCBuffer,
};
use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::interface::store::MultiVersionBatch;
use reifydb_extension::procedure::callbacks::extern_c::memory::{host_alloc, host_free};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::error::Error;

use super::{
	marshal::{encoded_key, write_buffer},
	store_iterator::{self, StoreIteratorHandle},
};
use crate::extern_c::context::get_transaction_mut;

#[repr(C)]
struct StoreIteratorInternal {
	handle: StoreIteratorHandle,
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_store_get(
	ctx: *mut ExternCContext,
	key_ptr: *const u8,
	key_len: usize,
	output: *mut ExternCBuffer,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || output.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx`, `key_ptr` and `output` are null-checked above; the guest must pass back the
	// ExternCContext the host handed it for this call, a `key_ptr` valid for `key_len` reads (discharging
	// encoded_key), and an `output` valid for one ExternCBuffer write that it frees via memory.free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let key = encoded_key(key_ptr, key_len);

		match flow_txn.get(&key) {
			Ok(Some(value)) => write_buffer(output, value.as_slice()),
			Ok(None) => EXTERN_C_NOT_FOUND,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_store_contains_key(
	ctx: *mut ExternCContext,
	key_ptr: *const u8,
	key_len: usize,
	result: *mut u8,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || result.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx`, `key_ptr` and `result` are null-checked above; the guest must pass back the
	// ExternCContext the host handed it for this call, a `key_ptr` valid for `key_len` reads (discharging
	// encoded_key), and a `result` valid for one u8 write.
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let key = encoded_key(key_ptr, key_len);

		match flow_txn.contains_key(&key) {
			Ok(exists) => {
				*result = if exists {
					1
				} else {
					0
				};
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_store_prefix(
	ctx: *mut ExternCContext,
	prefix_ptr: *const u8,
	prefix_len: usize,
	iterator_out: *mut *mut ExternCStoreIterator,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` and `iterator_out` are null-checked above; the guest must pass back the ExternCContext
	// the host handed it for this call, a `prefix_ptr` that is null or valid for `prefix_len` reads,
	// and an `iterator_out` valid for one pointer write; the handle is freed via store.iterator_free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let prefix_bytes = if prefix_ptr.is_null() {
			vec![]
		} else {
			from_raw_parts(prefix_ptr, prefix_len).to_vec()
		};
		let prefix = EncodedKey::new(prefix_bytes);

		let result = flow_txn.prefix(&prefix);
		match result {
			Ok(batch) => {
				let handle = store_iterator::create_iterator(batch);

				let iter_ptr =
					host_alloc(size_of::<StoreIteratorInternal>()) as *mut StoreIteratorInternal;
				if iter_ptr.is_null() {
					store_iterator::free_iterator(handle);
					return EXTERN_C_ERROR_ALLOC;
				}

				ptr::write(
					iter_ptr,
					StoreIteratorInternal {
						handle,
					},
				);

				*iterator_out = iter_ptr as *mut ExternCStoreIterator;
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_store_range(
	ctx: *mut ExternCContext,
	start_ptr: *const u8,
	start_len: usize,
	start_bound_type: u8,
	end_ptr: *const u8,
	end_len: usize,
	end_bound_type: u8,
	iterator_out: *mut *mut ExternCStoreIterator,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` and `iterator_out` are null-checked above, and each bound pointer is null-checked
	// on the arm that reads it; the guest must pass back the ExternCContext the host handed it for this
	// call, bound pointers valid for their stated lengths, and an `iterator_out` valid for one pointer
	// write; the handle is freed via store.iterator_free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let start_bound = match start_bound_type {
			BOUND_UNBOUNDED => Bound::Unbounded,
			BOUND_INCLUDED => {
				if start_ptr.is_null() {
					return EXTERN_C_ERROR_NULL_PTR;
				}
				let start_bytes = from_raw_parts(start_ptr, start_len).to_vec();
				Bound::Included(EncodedKey::new(start_bytes))
			}
			BOUND_EXCLUDED => {
				if start_ptr.is_null() {
					return EXTERN_C_ERROR_NULL_PTR;
				}
				let start_bytes = from_raw_parts(start_ptr, start_len).to_vec();
				Bound::Excluded(EncodedKey::new(start_bytes))
			}
			_ => return EXTERN_C_ERROR_INTERNAL,
		};

		let end_bound = match end_bound_type {
			BOUND_UNBOUNDED => Bound::Unbounded,
			BOUND_INCLUDED => {
				if end_ptr.is_null() {
					return EXTERN_C_ERROR_NULL_PTR;
				}
				let end_bytes = from_raw_parts(end_ptr, end_len).to_vec();
				Bound::Included(EncodedKey::new(end_bytes))
			}
			BOUND_EXCLUDED => {
				if end_ptr.is_null() {
					return EXTERN_C_ERROR_NULL_PTR;
				}
				let end_bytes = from_raw_parts(end_ptr, end_len).to_vec();
				Bound::Excluded(EncodedKey::new(end_bytes))
			}
			_ => return EXTERN_C_ERROR_INTERNAL,
		};

		let range = EncodedKeyRange::new(start_bound, end_bound);
		let result: Result<MultiVersionBatch, _> = (|| -> Result<_, Error> {
			let iter = flow_txn.range(range, RangeScope::All, 1024);
			let mut items = Vec::new();
			for res in iter {
				items.push(res?);
			}
			Ok(MultiVersionBatch {
				items,
				has_more: false,
			})
		})();

		match result {
			Ok(batch) => {
				let handle = store_iterator::create_iterator(batch);

				let iter_ptr =
					host_alloc(size_of::<StoreIteratorInternal>()) as *mut StoreIteratorInternal;
				if iter_ptr.is_null() {
					store_iterator::free_iterator(handle);
					return EXTERN_C_ERROR_ALLOC;
				}

				ptr::write(
					iter_ptr,
					StoreIteratorInternal {
						handle,
					},
				);

				*iterator_out = iter_ptr as *mut ExternCStoreIterator;
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_store_iterator_next(
	iterator: *mut ExternCStoreIterator,
	key_out: *mut ExternCBuffer,
	value_out: *mut ExternCBuffer,
) -> i32 {
	if iterator.is_null() || key_out.is_null() || value_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: all three pointers are null-checked above; `iterator` must be an unfreed handle this
	// host handed out, so it is a live StoreIteratorInternal-shaped block at align 8, and `key_out`
	// and `value_out` must be valid and aligned for one ExternCBuffer write each. On EXTERN_C_OK the guest owns
	// both buffers and must release them via memory.free with the reported lengths.
	unsafe {
		let iter_internal = iterator as *mut StoreIteratorInternal;
		let iter_handle = (*iter_internal).handle;

		match store_iterator::next_iterator(iter_handle) {
			Some((key, value)) => {
				let key_ptr = host_alloc(key.len());
				if key_ptr.is_null() {
					return EXTERN_C_ERROR_ALLOC;
				}
				ptr::copy_nonoverlapping(key.as_ptr(), key_ptr, key.len());
				(*key_out).ptr = key_ptr;
				(*key_out).len = key.len();
				(*key_out).cap = key.len();

				let value_ptr = host_alloc(value.len());
				if value_ptr.is_null() {
					host_free(key_ptr, key.len());
					return EXTERN_C_ERROR_ALLOC;
				}
				ptr::copy_nonoverlapping(value.as_ptr(), value_ptr, value.len());
				(*value_out).ptr = value_ptr;
				(*value_out).len = value.len();
				(*value_out).cap = value.len();

				EXTERN_C_OK
			}
			None => EXTERN_C_END_OF_ITERATION,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_store_iterator_free(iterator: *mut ExternCStoreIterator) {
	if iterator.is_null() {
		return;
	}

	// SAFETY: `iterator` is null-checked above and must be an unfreed handle this host handed out, so
	// it is a host_alloc block of exactly `size_of::<StoreIteratorInternal>()` bytes at align 8 >=
	// align_of::<StoreIteratorInternal>(), holding an initialised handle (discharges host_free).
	unsafe {
		let iter_internal = iterator as *mut StoreIteratorInternal;

		let handle = (*iter_internal).handle;
		store_iterator::free_iterator(handle);

		host_free(iter_internal as *mut u8, size_of::<StoreIteratorInternal>());
	}
}
