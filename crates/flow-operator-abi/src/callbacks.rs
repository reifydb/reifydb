//! Host callback definitions for FFI operators

use crate::types::*;

/// Memory management callbacks
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MemoryCallbacks {
	/// Allocate memory from the host
	///
	/// # Parameters
	/// - `size`: Number of bytes to allocate
	///
	/// # Returns
	/// - Pointer to allocated memory, or null on failure
	pub alloc: extern "C" fn(size: usize) -> *mut u8,

	/// Deallocate memory previously allocated by alloc
	///
	/// # Parameters
	/// - `ptr`: Pointer to memory to deallocate
	/// - `size`: Size of allocation (must match original alloc size)
	pub dealloc: extern "C" fn(ptr: *mut u8, size: usize),

	/// Reallocate memory
	///
	/// # Parameters
	/// - `ptr`: Current pointer (may be null)
	/// - `old_size`: Current size (0 if ptr is null)
	/// - `new_size`: Desired new size
	///
	/// # Returns
	/// - Pointer to reallocated memory, or null on failure
	pub realloc: extern "C" fn(ptr: *mut u8, old_size: usize, new_size: usize) -> *mut u8,
}

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
		ctx: *mut FFIContext,
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
		ctx: *mut FFIContext,
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
	pub remove: extern "C" fn(operator_id: u64, ctx: *mut FFIContext, key: *const u8, key_len: usize) -> i32,

	/// Clear all state for an operator
	///
	/// # Parameters
	/// - `operator_id`: Operator ID
	/// - `ctx`: FFI context
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub clear: extern "C" fn(operator_id: u64, ctx: *mut FFIContext) -> i32,

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
		ctx: *mut FFIContext,
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

/// Logging callbacks
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LogCallbacks {
	/// Log a message
	///
	/// # Parameters
	/// - `operator_id`: Operator ID for identifying the logging operator
	/// - `level`: Log level (0=trace, 1=debug, 2=info, 3=warn, 4=error)
	/// - `message`: Message bytes
	/// - `message_len`: Length of message in bytes
	pub message: extern "C" fn(operator_id: u64, level: u32, message: *const u8, message_len: usize),
}

/// Host-provided callbacks for FFI operators
///
/// These callbacks allow operators to request services from the host system
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HostCallbacks {
	pub memory: MemoryCallbacks,
	pub state: StateCallbacks,
	pub log: LogCallbacks,
}
