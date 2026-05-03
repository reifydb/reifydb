// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{ops::Bound, ptr, slice::from_raw_parts};

use reifydb_abi::{
	constants::{
		FFI_END_OF_ITERATION, FFI_ERROR_ALLOC, FFI_ERROR_INTERNAL, FFI_ERROR_NULL_PTR, FFI_NOT_FOUND, FFI_OK,
	},
	context::{context::ContextFFI, iterators::StoreIteratorFFI},
	data::buffer::BufferFFI,
};
use reifydb_core::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::store::MultiVersionBatch,
};
use reifydb_extension::procedure::ffi_callbacks::memory::{host_alloc, host_free};
use reifydb_type::{error::Error, util::cowvec::CowVec};

use super::store_iterator::{self, StoreIteratorHandle};
use crate::ffi::context::get_transaction_mut;

#[repr(C)]
struct StoreIteratorInternal {
	handle: StoreIteratorHandle,
}

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_get(
	ctx: *mut ContextFFI,
	key_ptr: *const u8,
	key_len: usize,
	output: *mut BufferFFI,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || output.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		match flow_txn.get(&key) {
			Ok(Some(value)) => {
				let value_bytes = value.as_slice();
				let value_ptr = host_alloc(value_bytes.len());
				if value_ptr.is_null() {
					return FFI_ERROR_ALLOC;
				}

				ptr::copy_nonoverlapping(value_bytes.as_ptr(), value_ptr, value_bytes.len());

				(*output).ptr = value_ptr;
				(*output).len = value_bytes.len();
				(*output).cap = value_bytes.len();

				FFI_OK
			}
			Ok(None) => FFI_NOT_FOUND,
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_contains_key(
	ctx: *mut ContextFFI,
	key_ptr: *const u8,
	key_len: usize,
	result: *mut u8,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || result.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		match flow_txn.contains_key(&key) {
			Ok(exists) => {
				*result = if exists {
					1
				} else {
					0
				};
				FFI_OK
			}
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_prefix(
	ctx: *mut ContextFFI,
	prefix_ptr: *const u8,
	prefix_len: usize,
	iterator_out: *mut *mut StoreIteratorFFI,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let prefix_bytes = if prefix_ptr.is_null() {
			vec![]
		} else {
			from_raw_parts(prefix_ptr, prefix_len).to_vec()
		};
		let prefix = EncodedKey(CowVec::new(prefix_bytes));

		let result = flow_txn.prefix(&prefix);
		match result {
			Ok(batch) => {
				let handle = store_iterator::create_iterator(batch);

				let iter_ptr =
					host_alloc(size_of::<StoreIteratorInternal>()) as *mut StoreIteratorInternal;
				if iter_ptr.is_null() {
					store_iterator::free_iterator(handle);
					return FFI_ERROR_ALLOC;
				}

				ptr::write(
					iter_ptr,
					StoreIteratorInternal {
						handle,
					},
				);

				*iterator_out = iter_ptr as *mut StoreIteratorFFI;
				FFI_OK
			}
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_range(
	ctx: *mut ContextFFI,
	start_ptr: *const u8,
	start_len: usize,
	start_bound_type: u8,
	end_ptr: *const u8,
	end_len: usize,
	end_bound_type: u8,
	iterator_out: *mut *mut StoreIteratorFFI,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let start_bound = match start_bound_type {
			BOUND_UNBOUNDED => Bound::Unbounded,
			BOUND_INCLUDED => {
				if start_ptr.is_null() {
					return FFI_ERROR_NULL_PTR;
				}
				let start_bytes = from_raw_parts(start_ptr, start_len).to_vec();
				Bound::Included(EncodedKey(CowVec::new(start_bytes)))
			}
			BOUND_EXCLUDED => {
				if start_ptr.is_null() {
					return FFI_ERROR_NULL_PTR;
				}
				let start_bytes = from_raw_parts(start_ptr, start_len).to_vec();
				Bound::Excluded(EncodedKey(CowVec::new(start_bytes)))
			}
			_ => return FFI_ERROR_INTERNAL,
		};

		let end_bound = match end_bound_type {
			BOUND_UNBOUNDED => Bound::Unbounded,
			BOUND_INCLUDED => {
				if end_ptr.is_null() {
					return FFI_ERROR_NULL_PTR;
				}
				let end_bytes = from_raw_parts(end_ptr, end_len).to_vec();
				Bound::Included(EncodedKey(CowVec::new(end_bytes)))
			}
			BOUND_EXCLUDED => {
				if end_ptr.is_null() {
					return FFI_ERROR_NULL_PTR;
				}
				let end_bytes = from_raw_parts(end_ptr, end_len).to_vec();
				Bound::Excluded(EncodedKey(CowVec::new(end_bytes)))
			}
			_ => return FFI_ERROR_INTERNAL,
		};

		let range = EncodedKeyRange::new(start_bound, end_bound);
		let result: Result<MultiVersionBatch, _> = (|| -> Result<_, Error> {
			let iter = flow_txn.range(range, 1024);
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
					return FFI_ERROR_ALLOC;
				}

				ptr::write(
					iter_ptr,
					StoreIteratorInternal {
						handle,
					},
				);

				*iterator_out = iter_ptr as *mut StoreIteratorFFI;
				FFI_OK
			}
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_iterator_next(
	iterator: *mut StoreIteratorFFI,
	key_out: *mut BufferFFI,
	value_out: *mut BufferFFI,
) -> i32 {
	if iterator.is_null() || key_out.is_null() || value_out.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let iter_internal = iterator as *mut StoreIteratorInternal;
		let iter_handle = (*iter_internal).handle;

		match store_iterator::next_iterator(iter_handle) {
			Some((key, value)) => {
				let key_ptr = host_alloc(key.len());
				if key_ptr.is_null() {
					return FFI_ERROR_ALLOC;
				}
				ptr::copy_nonoverlapping(key.as_ptr(), key_ptr, key.len());
				(*key_out).ptr = key_ptr;
				(*key_out).len = key.len();
				(*key_out).cap = key.len();

				let value_ptr = host_alloc(value.len());
				if value_ptr.is_null() {
					host_free(key_ptr, key.len());
					return FFI_ERROR_ALLOC;
				}
				ptr::copy_nonoverlapping(value.as_ptr(), value_ptr, value.len());
				(*value_out).ptr = value_ptr;
				(*value_out).len = value.len();
				(*value_out).cap = value.len();

				FFI_OK
			}
			None => FFI_END_OF_ITERATION,
		}
	}
}

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_iterator_free(iterator: *mut StoreIteratorFFI) {
	if iterator.is_null() {
		return;
	}

	unsafe {
		let iter_internal = iterator as *mut StoreIteratorInternal;

		let handle = (*iter_internal).handle;
		store_iterator::free_iterator(handle);

		host_free(iter_internal as *mut u8, size_of::<StoreIteratorInternal>());
	}
}
