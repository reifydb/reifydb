//! Host callback definitions for FFI operators

use crate::types::*;

/// Host-provided callbacks for FFI operators
///
/// These callbacks allow operators to request services from the host system,
/// including memory management, expression evaluation, and state operations.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HostCallbacks {
	// ==================== Memory Management ====================
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

	// ==================== Expression Evaluation ====================
	/// Evaluate an expression against a row
	///
	/// # Parameters
	/// - `expr`: Expression handle (managed by host)
	/// - `row`: Row to evaluate against
	///
	/// # Returns
	/// - Result value from expression evaluation
	pub eval_expression: extern "C" fn(expr: *const ExpressionHandle, row: *const RowFFI) -> ValueFFI,

	// ==================== Row Operations ====================
	/// Create a new row
	///
	/// # Parameters
	/// - `row_number`: Row number identifier
	/// - `encoded`: Encoded row bytes
	/// - `encoded_len`: Length of encoded bytes
	/// - `layout`: Layout metadata for row structure
	///
	/// # Returns
	/// - Pointer to created row, or null on failure
	pub create_row: extern "C" fn(
		row_number: u64,
		encoded: *const u8,
		encoded_len: usize,
		layout: *const LayoutFFI,
	) -> *mut RowFFI,

	/// Clone an existing row
	///
	/// # Parameters
	/// - `row`: Row to clone
	///
	/// # Returns
	/// - Pointer to cloned row, or null on failure
	pub clone_row: extern "C" fn(row: *const RowFFI) -> *mut RowFFI,

	/// Free a row created by create_row or clone_row
	///
	/// # Parameters
	/// - `row`: Row to free
	pub free_row: extern "C" fn(row: *mut RowFFI),

	// ==================== Value Operations ====================
	/// Encode multiple values as a key (for keyed state)
	///
	/// # Parameters
	/// - `values`: Array of values to encode
	/// - `value_count`: Number of values
	/// - `output`: Buffer to receive encoded key
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub encode_values_as_key:
		extern "C" fn(values: *const ValueFFI, value_count: usize, output: *mut BufferFFI) -> i32,

	/// Free a value created by the host
	///
	/// # Parameters
	/// - `value`: Value to free
	pub free_value: extern "C" fn(value: *mut ValueFFI),

	// ==================== Iterator Operations ====================
	/// Get next item from a state iterator
	///
	/// # Parameters
	/// - `iterator`: Iterator handle
	/// - `key_out`: Buffer to receive key (may be null)
	/// - `value_out`: Buffer to receive value
	///
	/// # Returns
	/// - 0 if item retrieved, 1 if end of iteration, negative on error
	pub state_iterator_next: extern "C" fn(
		iterator: *mut StateIteratorFFI,
		key_out: *mut BufferFFI,
		value_out: *mut BufferFFI,
	) -> i32,

	/// Free a state iterator
	///
	/// # Parameters
	/// - `iterator`: Iterator to free
	pub state_iterator_free: extern "C" fn(iterator: *mut StateIteratorFFI),

	// ==================== Logging & Debugging ====================
	/// Log a message
	///
	/// # Parameters
	/// - `level`: Log level (0=trace, 1=debug, 2=info, 3=warn, 4=error)
	/// - `message`: Null-terminated message string
	pub log_message: extern "C" fn(level: u32, message: *const u8),

	// ==================== State Operations ====================
	/// Get a value from operator state
	///
	/// # Parameters
	/// - `node_id`: Operator node ID for namespacing
	/// - `txn`: Transaction handle
	/// - `key`: Key bytes
	/// - `key_len`: Length of key
	/// - `output`: Buffer to receive value
	///
	/// # Returns
	/// - 0 if value exists and retrieved, 1 if key not found, negative on error
	pub state_get: extern "C" fn(
		node_id: u64,
		txn: *mut TransactionHandle,
		key: *const u8,
		key_len: usize,
		output: *mut BufferFFI,
	) -> i32,

	/// Set a value in operator state
	///
	/// # Parameters
	/// - `node_id`: Operator node ID for namespacing
	/// - `txn`: Transaction handle
	/// - `key`: Key bytes
	/// - `key_len`: Length of key
	/// - `value`: Value bytes
	/// - `value_len`: Length of value
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub state_set: extern "C" fn(
		node_id: u64,
		txn: *mut TransactionHandle,
		key: *const u8,
		key_len: usize,
		value: *const u8,
		value_len: usize,
	) -> i32,

	/// Remove a value from operator state
	///
	/// # Parameters
	/// - `node_id`: Operator node ID for namespacing
	/// - `txn`: Transaction handle
	/// - `key`: Key bytes
	/// - `key_len`: Length of key
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub state_remove:
		extern "C" fn(node_id: u64, txn: *mut TransactionHandle, key: *const u8, key_len: usize) -> i32,

	/// Clear all state for an operator
	///
	/// # Parameters
	/// - `node_id`: Operator node ID for namespacing
	/// - `txn`: Transaction handle
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub state_clear: extern "C" fn(node_id: u64, txn: *mut TransactionHandle) -> i32,

	/// Create an iterator for state keys with a given prefix
	///
	/// # Parameters
	/// - `node_id`: Operator node ID for namespacing
	/// - `txn`: Transaction handle
	/// - `prefix`: Prefix bytes
	/// - `prefix_len`: Length of prefix
	/// - `iterator_out`: Pointer to receive iterator handle
	///
	/// # Returns
	/// - 0 on success, negative error code on failure
	pub state_prefix: extern "C" fn(
		node_id: u64,
		txn: *mut TransactionHandle,
		prefix: *const u8,
		prefix_len: usize,
		iterator_out: *mut *mut StateIteratorFFI,
	) -> i32,
}
