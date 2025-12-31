// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

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
