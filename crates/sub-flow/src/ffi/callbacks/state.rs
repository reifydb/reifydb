//! State management callbacks for FFI operators
//!
//! Provides key-value state storage for operators, including get/set/remove/clear operations
//! and prefix-based iteration.

use std::slice::from_raw_parts;

use reifydb_core::{
	EncodedKeyRange,
	interface::{BoxedMultiVersionIter, FlowNodeId},
	util::CowVec,
	value::encoded::{EncodedKey, EncodedValues},
};
use reifydb_flow_operator_abi::{
	BufferFFI, FFI_END_OF_ITERATION, FFI_ERROR_ALLOC, FFI_ERROR_INTERNAL, FFI_ERROR_NULL_PTR, FFI_NOT_FOUND,
	FFI_OK, FFIContext, StateIteratorFFI,
};

use super::{
	memory::{host_alloc, host_free},
	state_iterator::{self, StateIteratorHandle},
};
use crate::ffi::context::get_transaction_mut;

/// Internal structure for state iterators (stored behind StateIteratorFFI pointer)
#[repr(C)]
struct StateIteratorInternal {
	handle: StateIteratorHandle,
}

/// Get state value for a specific operator and key
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_get(
	operator_id: u64,
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

		// Get state from transaction
		match flow_txn.state_get(FlowNodeId(operator_id), &key) {
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

/// Set state value for a specific operator and key
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_set(
	operator_id: u64,
	ctx: *mut FFIContext,
	key_ptr: *const u8,
	key_len: usize,
	value_ptr: *const u8,
	value_len: usize,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || value_ptr.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		// Convert raw bytes to EncodedKey and EncodedValues
		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		let value_bytes = from_raw_parts(value_ptr, value_len);
		let value = EncodedValues(CowVec::new(value_bytes.to_vec()));

		match flow_txn.state_set(FlowNodeId(operator_id), &key, value) {
			Ok(_) => FFI_OK,
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

/// Remove state value for a specific operator and key
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_remove(
	operator_id: u64,
	ctx: *mut FFIContext,
	key_ptr: *const u8,
	key_len: usize,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		// Convert raw bytes to EncodedKey
		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		// Remove state from transaction
		match flow_txn.state_remove(FlowNodeId(operator_id), &key) {
			Ok(_) => FFI_OK,
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

/// Clear all state for a specific operator
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_clear(operator_id: u64, ctx: *mut FFIContext) -> i32 {
	if ctx.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		// Clear all state for this operator
		match flow_txn.state_clear(FlowNodeId(operator_id)) {
			Ok(_) => FFI_OK,
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

/// Create an iterator for state with a specific prefix
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_prefix(
	operator_id: u64,
	ctx: *mut FFIContext,
	prefix_ptr: *const u8,
	prefix_len: usize,
	iterator_out: *mut *mut StateIteratorFFI,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);
		let node_id = FlowNodeId(operator_id);

		// Get prefix bytes (can be empty for full scan)
		let prefix_bytes = if prefix_ptr.is_null() {
			vec![]
		} else {
			from_raw_parts(prefix_ptr, prefix_len).to_vec()
		};

		// Create range query based on prefix
		let result = if prefix_bytes.is_empty() {
			// Empty prefix = full scan of all state for this operator
			flow_txn.state_scan(node_id)
		} else {
			// Prefix scan = range query using prefix
			let range = EncodedKeyRange::prefix(&prefix_bytes);
			flow_txn.state_range(node_id, range)
		};

		match result {
			Ok(iter) => {
				// Need to convert the iterator to 'static lifetime for storage
				// This is safe because the iterator is owned by the registry
				let static_iter: BoxedMultiVersionIter<'static> = std::mem::transmute(iter);

				// Create iterator handle
				let handle = state_iterator::create_iterator(static_iter);

				// Allocate internal structure and store handle
				let iter_ptr = host_alloc(std::mem::size_of::<StateIteratorInternal>())
					as *mut StateIteratorInternal;
				if iter_ptr.is_null() {
					return FFI_ERROR_ALLOC;
				}

				// Initialize the iterator structure with the handle
				std::ptr::write(
					iter_ptr,
					StateIteratorInternal {
						handle,
					},
				);

				// Cast to opaque StateIteratorFFI pointer
				*iterator_out = iter_ptr as *mut StateIteratorFFI;
				FFI_OK
			}
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

/// Get the next key-value pair from a state iterator
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_iterator_next(
	iterator: *mut StateIteratorFFI,
	key_out: *mut BufferFFI,
	value_out: *mut BufferFFI,
) -> i32 {
	if iterator.is_null() || key_out.is_null() || value_out.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		// Cast opaque pointer back to internal structure
		let iter_internal = iterator as *mut StateIteratorInternal;
		let iter_handle = (*iter_internal).handle;

		// Get next item from iterator
		match state_iterator::next_iterator(iter_handle) {
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

/// Free a state iterator
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_iterator_free(iterator: *mut StateIteratorFFI) {
	if iterator.is_null() {
		return;
	}

	unsafe {
		// Cast opaque pointer back to internal structure
		let iter_internal = iterator as *mut StateIteratorInternal;

		// Get the handle and free the iterator from registry
		let handle = (*iter_internal).handle;
		state_iterator::free_iterator(handle);

		// Free the internal structure itself
		host_free(iter_internal as *mut u8, std::mem::size_of::<StateIteratorInternal>());
	}
}
