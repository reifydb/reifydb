use crate::{
	context::{ContextFFI, StateIteratorFFI},
	data::BufferFFI,
};

/// State management callbacks
#[repr(C)]
#[derive(Clone, Copy)]
pub struct StateCallbacks {
	/// Get a value from operator state
	///
	/// # Parameters
	/// - `operator_id`: Operator ID
	/// - `ctx`: FFI context
	/// - `key`: Key bytes
	/// - `key_len`: Length of key
	/// - `output`: Buffer to receive value
	///
	/// # Returns
	/// - 0 if value exists and retrieved, 1 if key not found, negative on error
	pub get: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		key: *const u8,
		key_len: usize,
		output: *mut BufferFFI,
	) -> i32,

	/// Set a value in operator state
	///
	/// # Parameters
	/// - `operator_id`: Operator ID
	/// - `ctx`: FFI context
	/// - `key`: Key bytes
	/// - `key_len`: Length of key
	/// - `value`: Value bytes
	/// - `value_len`: Length of value
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub set: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		key: *const u8,
		key_len: usize,
		value: *const u8,
		value_len: usize,
	) -> i32,

	/// Remove a value from operator state
	///
	/// # Parameters
	/// - `operator_id`: Operator ID
	/// - `ctx`: FFI context
	/// - `key`: Key bytes
	/// - `key_len`: Length of key
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub remove: extern "C" fn(operator_id: u64, ctx: *mut ContextFFI, key: *const u8, key_len: usize) -> i32,

	/// Clear all state for an operator
	///
	/// # Parameters
	/// - `operator_id`: Operator ID
	/// - `ctx`: FFI context
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub clear: extern "C" fn(operator_id: u64, ctx: *mut ContextFFI) -> i32,

	/// Create an iterator for state keys with a given prefix
	///
	/// # Parameters
	/// - `operator_id`: Operator ID
	/// - `ctx`: FFI context
	/// - `prefix`: Prefix bytes
	/// - `prefix_len`: Length of prefix
	/// - `iterator_out`: Pointer to receive iterator handle
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub prefix: extern "C" fn(
		operator_id: u64,
		ctx: *mut ContextFFI,
		prefix: *const u8,
		prefix_len: usize,
		iterator_out: *mut *mut StateIteratorFFI,
	) -> i32,

	/// Get the next key-value pair from a state iterator
	///
	/// # Parameters
	/// - `iterator`: Iterator handle
	/// - `key_out`: Buffer to receive key
	/// - `value_out`: Buffer to receive value
	///
	/// # Returns
	/// - 0 on success, 1 if end of iteration, negative on error
	pub iterator_next: extern "C" fn(
		iterator: *mut StateIteratorFFI,
		key_out: *mut BufferFFI,
		value_out: *mut BufferFFI,
	) -> i32,

	/// Free a state iterator
	///
	/// # Parameters
	/// - `iterator`: Iterator to free
	pub iterator_free: extern "C" fn(iterator: *mut StateIteratorFFI),
}
