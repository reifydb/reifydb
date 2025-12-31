// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{
	context::{ContextFFI, StoreIteratorFFI},
	data::BufferFFI,
};

/// Store access callbacks (read-only access to underlying store)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct StoreCallbacks {
	/// Get a value from store by key
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `key`: Key bytes
	/// - `key_len`: Length of key
	/// - `output`: Buffer to receive value
	///
	/// # Returns
	/// - 0 if value exists and retrieved, 1 if key not found, negative on error
	pub get: extern "C" fn(ctx: *mut ContextFFI, key: *const u8, key_len: usize, output: *mut BufferFFI) -> i32,

	/// Check if a key exists in store
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `key`: Key bytes
	/// - `key_len`: Length of key
	/// - `result`: Pointer to receive result (1 if exists, 0 if not)
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub contains_key: extern "C" fn(ctx: *mut ContextFFI, key: *const u8, key_len: usize, result: *mut u8) -> i32,

	/// Create an iterator for store keys with a given prefix
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `prefix`: Prefix bytes
	/// - `prefix_len`: Length of prefix
	/// - `iterator_out`: Pointer to receive iterator handle
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub prefix: extern "C" fn(
		ctx: *mut ContextFFI,
		prefix: *const u8,
		prefix_len: usize,
		iterator_out: *mut *mut StoreIteratorFFI,
	) -> i32,

	/// Create an iterator for store keys in a range
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `start`: Start key bytes
	/// - `start_len`: Length of start key
	/// - `start_bound_type`: Bound type for start (0=Unbounded, 1=Included, 2=Excluded)
	/// - `end`: End key bytes
	/// - `end_len`: Length of end key
	/// - `end_bound_type`: Bound type for end (0=Unbounded, 1=Included, 2=Excluded)
	/// - `iterator_out`: Pointer to receive iterator handle
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub range: extern "C" fn(
		ctx: *mut ContextFFI,
		start: *const u8,
		start_len: usize,
		start_bound_type: u8,
		end: *const u8,
		end_len: usize,
		end_bound_type: u8,
		iterator_out: *mut *mut StoreIteratorFFI,
	) -> i32,

	/// Get the next key-value pair from a store iterator
	///
	/// # Parameters
	/// - `iterator`: Iterator handle
	/// - `key_out`: Buffer to receive key
	/// - `value_out`: Buffer to receive value
	///
	/// # Returns
	/// - 0 on success, 1 if end of iteration, negative on error
	pub iterator_next: extern "C" fn(
		iterator: *mut StoreIteratorFFI,
		key_out: *mut BufferFFI,
		value_out: *mut BufferFFI,
	) -> i32,

	/// Free a store iterator
	///
	/// # Parameters
	/// - `iterator`: Iterator to free
	pub iterator_free: extern "C" fn(iterator: *mut StoreIteratorFFI),
}
