// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Memory management callbacks
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MemoryCallbacks {
	pub alloc: extern "C" fn(size: usize) -> *mut u8,

	pub free: unsafe extern "C" fn(ptr: *mut u8, size: usize),

	pub realloc: unsafe extern "C" fn(ptr: *mut u8, old_size: usize, new_size: usize) -> *mut u8,
}
