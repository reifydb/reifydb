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

	/// Free memory previously allocated by alloc
	///
	/// # Parameters
	/// - `ptr`: Pointer to memory to free
	/// - `size`: Size of allocation (must match original alloc size)
	pub free: extern "C" fn(ptr: *mut u8, size: usize),

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
	pub get: extern "C" fn(ctx: *mut FFIContext, key: *const u8, key_len: usize, output: *mut BufferFFI) -> i32,

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
	pub contains_key: extern "C" fn(ctx: *mut FFIContext, key: *const u8, key_len: usize, result: *mut u8) -> i32,

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
		ctx: *mut FFIContext,
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
		ctx: *mut FFIContext,
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

/// Catalog access callbacks (read-only access to catalog system)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CatalogCallbacks {
	/// Find a namespace by ID at specific version
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `namespace_id`: Namespace ID
	/// - `version`: Commit version for time-travel queries
	/// - `output`: Pointer to receive namespace definition
	///
	/// # Returns
	/// - 0 if namespace exists, 1 if not found, negative on error
	pub find_namespace: extern "C" fn(
		ctx: *mut FFIContext,
		namespace_id: u64,
		version: u64,
		output: *mut crate::ffi::FFINamespaceDef,
	) -> i32,

	/// Find a namespace by name at specific version
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `name_ptr`: Namespace name bytes (UTF-8)
	/// - `name_len`: Length of name
	/// - `version`: Commit version for time-travel queries
	/// - `output`: Pointer to receive namespace definition
	///
	/// # Returns
	/// - 0 if namespace exists, 1 if not found, negative on error
	pub find_namespace_by_name: extern "C" fn(
		ctx: *mut FFIContext,
		name_ptr: *const u8,
		name_len: usize,
		version: u64,
		output: *mut crate::ffi::FFINamespaceDef,
	) -> i32,

	/// Find a table by ID at specific version
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `table_id`: Table ID
	/// - `version`: Commit version for time-travel queries
	/// - `output`: Pointer to receive table definition
	///
	/// # Returns
	/// - 0 if table exists, 1 if not found, negative on error
	pub find_table: extern "C" fn(
		ctx: *mut FFIContext,
		table_id: u64,
		version: u64,
		output: *mut crate::ffi::FFITableDef,
	) -> i32,

	/// Find a table by name in a namespace at specific version
	///
	/// # Parameters
	/// - `ctx`: FFI context
	/// - `namespace_id`: Namespace ID
	/// - `name_ptr`: Table name bytes (UTF-8)
	/// - `name_len`: Length of name
	/// - `version`: Commit version for time-travel queries
	/// - `output`: Pointer to receive table definition
	///
	/// # Returns
	/// - 0 if table exists, 1 if not found, negative on error
	pub find_table_by_name: extern "C" fn(
		ctx: *mut FFIContext,
		namespace_id: u64,
		name_ptr: *const u8,
		name_len: usize,
		version: u64,
		output: *mut crate::ffi::FFITableDef,
	) -> i32,

	/// Free a namespace definition allocated by the host
	///
	/// # Parameters
	/// - `namespace`: Pointer to namespace to free
	pub free_namespace: extern "C" fn(namespace: *mut crate::ffi::FFINamespaceDef),

	/// Free a table definition allocated by the host
	///
	/// # Parameters
	/// - `table`: Pointer to table to free
	pub free_table: extern "C" fn(table: *mut crate::ffi::FFITableDef),
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
	pub store: StoreCallbacks,
	pub catalog: CatalogCallbacks,
}
