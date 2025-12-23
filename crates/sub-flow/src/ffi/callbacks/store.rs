//! Store access callbacks for FFI operators
//!
//! Provides read-only access to the underlying store for operators,
//! including get, contains_key, prefix, and range operations.

use std::{ops::Bound, slice::from_raw_parts};

use reifydb_core::{EncodedKeyRange, util::CowVec, value::encoded::EncodedKey};
use reifydb_flow_operator_abi::{
	BufferFFI, FFI_END_OF_ITERATION, FFI_ERROR_ALLOC, FFI_ERROR_INTERNAL, FFI_ERROR_NULL_PTR, FFI_NOT_FOUND,
	FFI_OK, FFIContext, StoreIteratorFFI,
};
use tokio::{runtime::Handle, task::block_in_place};

use super::{
	memory::{host_alloc, host_free},
	store_iterator::{self, StoreIteratorHandle},
};
use crate::ffi::context::get_transaction_mut;

/// Internal structure for store iterators (stored behind StoreIteratorFFI pointer)
#[repr(C)]
struct StoreIteratorInternal {
	handle: StoreIteratorHandle,
}

/// Get a value from store by key
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_get(
	ctx: *mut FFIContext,
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

		// Convert raw bytes to EncodedKey
		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		// Get value from transaction - use block_in_place to avoid nested runtime panic
		match block_in_place(|| Handle::current().block_on(flow_txn.get(&key))) {
			Ok(Some(value)) => {
				// Copy value to output buffer
				let value_bytes = value.as_ref();
				let value_ptr = host_alloc(value_bytes.len());
				if value_ptr.is_null() {
					return FFI_ERROR_ALLOC;
				}

				std::ptr::copy_nonoverlapping(value_bytes.as_ptr(), value_ptr, value_bytes.len());

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

/// Check if a key exists in store
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_contains_key(
	ctx: *mut FFIContext,
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

		// Convert raw bytes to EncodedKey
		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		// Check if key exists in transaction - use block_in_place to avoid nested runtime panic
		match block_in_place(|| Handle::current().block_on(flow_txn.contains_key(&key))) {
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

/// Create an iterator for store keys with a given prefix
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_prefix(
	ctx: *mut FFIContext,
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

		// Get prefix bytes
		let prefix_bytes = if prefix_ptr.is_null() {
			vec![]
		} else {
			from_raw_parts(prefix_ptr, prefix_len).to_vec()
		};
		let prefix = EncodedKey(CowVec::new(prefix_bytes));

		// Use block_in_place to call async methods from sync FFI context
		let result = block_in_place(|| Handle::current().block_on(flow_txn.prefix(&prefix)));

		match result {
			Ok(batch) => {
				// Create iterator handle from batch
				// No unsafe transmute needed - batches own their data
				let handle = store_iterator::create_iterator(batch);

				// Allocate internal structure and store handle
				let iter_ptr = host_alloc(std::mem::size_of::<StoreIteratorInternal>())
					as *mut StoreIteratorInternal;
				if iter_ptr.is_null() {
					return FFI_ERROR_ALLOC;
				}

				// Initialize the iterator structure with the handle
				std::ptr::write(
					iter_ptr,
					StoreIteratorInternal {
						handle,
					},
				);

				// Cast to opaque StoreIteratorFFI pointer
				*iterator_out = iter_ptr as *mut StoreIteratorFFI;
				FFI_OK
			}
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

/// Bound type constants for FFI
const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

/// Create an iterator for store keys in a range
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_range(
	ctx: *mut FFIContext,
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

		// Decode start bound from type and bytes
		let start_bound = match start_bound_type {
			BOUND_UNBOUNDED => Bound::Unbounded,
			BOUND_INCLUDED => {
				if start_ptr.is_null() {
					return FFI_ERROR_NULL_PTR; // Invalid: Included bound requires key bytes
				}
				let start_bytes = from_raw_parts(start_ptr, start_len).to_vec();
				Bound::Included(EncodedKey(CowVec::new(start_bytes)))
			}
			BOUND_EXCLUDED => {
				if start_ptr.is_null() {
					return FFI_ERROR_NULL_PTR; // Invalid: Excluded bound requires key bytes
				}
				let start_bytes = from_raw_parts(start_ptr, start_len).to_vec();
				Bound::Excluded(EncodedKey(CowVec::new(start_bytes)))
			}
			_ => return FFI_ERROR_INTERNAL, // Invalid bound type
		};

		// Decode end bound from type and bytes
		let end_bound = match end_bound_type {
			BOUND_UNBOUNDED => Bound::Unbounded,
			BOUND_INCLUDED => {
				if end_ptr.is_null() {
					return FFI_ERROR_NULL_PTR; // Invalid: Included bound requires key bytes
				}
				let end_bytes = from_raw_parts(end_ptr, end_len).to_vec();
				Bound::Included(EncodedKey(CowVec::new(end_bytes)))
			}
			BOUND_EXCLUDED => {
				if end_ptr.is_null() {
					return FFI_ERROR_NULL_PTR; // Invalid: Excluded bound requires key bytes
				}
				let end_bytes = from_raw_parts(end_ptr, end_len).to_vec();
				Bound::Excluded(EncodedKey(CowVec::new(end_bytes)))
			}
			_ => return FFI_ERROR_INTERNAL, // Invalid bound type
		};

		// Create range from decoded bounds
		let range = EncodedKeyRange::new(start_bound, end_bound);

		// Use block_in_place to call async methods from sync FFI context
		let result = block_in_place(|| Handle::current().block_on(flow_txn.range(range)));

		match result {
			Ok(batch) => {
				// Create iterator handle from batch
				// No unsafe transmute needed - batches own their data
				let handle = store_iterator::create_iterator(batch);

				// Allocate internal structure and store handle
				let iter_ptr = host_alloc(std::mem::size_of::<StoreIteratorInternal>())
					as *mut StoreIteratorInternal;
				if iter_ptr.is_null() {
					return FFI_ERROR_ALLOC;
				}

				// Initialize the iterator structure with the handle
				std::ptr::write(
					iter_ptr,
					StoreIteratorInternal {
						handle,
					},
				);

				// Cast to opaque StoreIteratorFFI pointer
				*iterator_out = iter_ptr as *mut StoreIteratorFFI;
				FFI_OK
			}
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

/// Get the next key-value pair from a store iterator
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
		// Cast opaque pointer back to internal structure
		let iter_internal = iterator as *mut StoreIteratorInternal;
		let iter_handle = (*iter_internal).handle;

		// Get next item from iterator
		match store_iterator::next_iterator(iter_handle) {
			Some((key, value)) => {
				// Allocate and copy key
				let key_ptr = host_alloc(key.len());
				if key_ptr.is_null() {
					return FFI_ERROR_ALLOC;
				}
				std::ptr::copy_nonoverlapping(key.as_ptr(), key_ptr, key.len());
				(*key_out).ptr = key_ptr;
				(*key_out).len = key.len();
				(*key_out).cap = key.len();

				// Allocate and copy value
				let value_ptr = host_alloc(value.len());
				if value_ptr.is_null() {
					// Free the key we just allocated
					host_free(key_ptr, key.len());
					return FFI_ERROR_ALLOC;
				}
				std::ptr::copy_nonoverlapping(value.as_ptr(), value_ptr, value.len());
				(*value_out).ptr = value_ptr;
				(*value_out).len = value.len();
				(*value_out).cap = value.len();

				FFI_OK
			}
			None => FFI_END_OF_ITERATION,
		}
	}
}

/// Free a store iterator
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_store_iterator_free(iterator: *mut StoreIteratorFFI) {
	if iterator.is_null() {
		return;
	}

	unsafe {
		// Cast opaque pointer back to internal structure
		let iter_internal = iterator as *mut StoreIteratorInternal;

		// Get the handle and free the iterator from registry
		let handle = (*iter_internal).handle;
		store_iterator::free_iterator(handle);

		// Free the internal structure itself
		host_free(iter_internal as *mut u8, std::mem::size_of::<StoreIteratorInternal>());
	}
}
