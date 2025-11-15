//! State management callbacks for FFI operators
//!
//! Provides key-value state storage for operators, including get/set/remove/clear operations
//! and prefix-based iteration (prefix iteration is TODO).

use std::slice::from_raw_parts;

use reifydb_core::{
	interface::FlowNodeId,
	util::CowVec,
	value::encoded::{EncodedKey, EncodedValues},
};
use reifydb_flow_operator_abi::{BufferFFI, FFIContext, StateIteratorFFI};

use super::memory::{host_alloc, host_dealloc};
use crate::ffi::context::get_transaction_mut;

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
		return -1;
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
					return -2; // Allocation failed
				}

				std::ptr::copy_nonoverlapping(value_bytes.as_ptr(), value_ptr, value_bytes.len());

				(*output).ptr = value_ptr;
				(*output).len = value_bytes.len();
				(*output).cap = value_bytes.len();

				0 // Success, value found
			}
			Ok(None) => 1, // Key not found
			Err(_) => -1,  // Error
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
		return -1;
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
			Ok(_) => 0,   // Success
			Err(_) => -1, // Error
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
		return -1;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		// Convert raw bytes to EncodedKey
		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		// Remove state from transaction
		match flow_txn.state_remove(FlowNodeId(operator_id), &key) {
			Ok(_) => 0,   // Success
			Err(_) => -1, // Error
		}
	}
}

/// Clear all state for a specific operator
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_clear(operator_id: u64, ctx: *mut FFIContext) -> i32 {
	if ctx.is_null() {
		return -1;
	}

	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		// Clear all state for this operator
		match flow_txn.state_clear(FlowNodeId(operator_id)) {
			Ok(_) => 0,   // Success
			Err(_) => -1, // Error
		}
	}
}

/// Create an iterator for state with a specific prefix
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_prefix(
	_operator_id: u64,
	_ctx: *mut FFIContext,
	_prefix_ptr: *const u8,
	_prefix_len: usize,
	_iterator_out: *mut *mut StateIteratorFFI,
) -> i32 {
	// TODO: Implement prefix scan iterator
	// For now, return not supported
	-1
}

/// Get the next key-value pair from a state iterator
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_iterator_next(
	iterator: *mut StateIteratorFFI,
	_key_out: *mut BufferFFI,
	value_out: *mut BufferFFI,
) -> i32 {
	if iterator.is_null() || value_out.is_null() {
		return -1;
	}

	// TODO: Implement actual state iteration
	// For now, return end of iteration
	1 // End of iteration
}

/// Free a state iterator
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_state_iterator_free(iterator: *mut StateIteratorFFI) {
	if iterator.is_null() {
		return;
	}

	// TODO: Free iterator resources
	host_dealloc(iterator as *mut u8, std::mem::size_of::<StateIteratorFFI>());
}
