// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Memory arena for FFI allocations
//!
//! Provides a simple arena allocator that is automatically cleaned up
//! after each FFI operator invocation.

use std::{
	alloc::Layout,
	ptr::{copy_nonoverlapping, null_mut},
};

use bumpalo::Bump;

/// Memory arena for FFI allocations
///
/// All memory allocated from this arena is freed when the arena is dropped
/// or when `clear()` is called.
pub struct Arena {
	bump: Bump,
}

impl Arena {
	/// Create a new empty arena
	pub fn new() -> Self {
		Self {
			bump: Bump::new(),
		}
	}

	/// Allocate memory from the arena
	///
	/// Returns null for zero-sized allocations
	pub fn alloc(&self, size: usize) -> *mut u8 {
		if size == 0 {
			return null_mut();
		}
		let layout = Layout::from_size_align(size, 8).unwrap();
		self.bump.alloc_layout(layout).as_ptr()
	}

	/// Allocate and copy bytes into the arena
	pub fn copy_bytes(&self, bytes: &[u8]) -> *mut u8 {
		if bytes.is_empty() {
			return null_mut();
		}
		let ptr = self.alloc(bytes.len());
		if !ptr.is_null() {
			unsafe {
				copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
			}
		}
		ptr
	}

	/// Clear all allocations, keeping underlying memory for reuse
	pub fn clear(&mut self) {
		self.bump.reset();
	}
}

impl Default for Arena {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_arena_basic() {
		let arena = Arena::new();

		let ptr1 = arena.alloc(100);
		assert!(!ptr1.is_null());

		let ptr2 = arena.alloc(200);
		assert!(!ptr2.is_null());
	}

	#[test]
	fn test_arena_copy_bytes() {
		let arena = Arena::new();

		let data = vec![1u8, 2, 3, 4, 5];
		let ptr = arena.copy_bytes(&data);
		assert!(!ptr.is_null());

		unsafe {
			for i in 0..5 {
				assert_eq!(*ptr.add(i), data[i]);
			}
		}
	}

	#[test]
	fn test_arena_clear() {
		let mut arena = Arena::new();

		arena.alloc(100);
		arena.alloc(200);

		arena.clear();
		// After clear, we can allocate again
		let ptr = arena.alloc(50);
		assert!(!ptr.is_null());
	}

	#[test]
	fn test_arena_zero_size() {
		let arena = Arena::new();
		let ptr = arena.alloc(0);
		assert!(ptr.is_null());
	}

	#[test]
	fn test_arena_empty_bytes() {
		let arena = Arena::new();
		let ptr = arena.copy_bytes(&[]);
		assert!(ptr.is_null());
	}
}
