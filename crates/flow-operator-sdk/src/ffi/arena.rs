//! Memory arena for FFI allocations
//!
//! Provides a simple arena allocator that is automatically cleaned up
//! after each FFI operator invocation.

use std::{
	alloc::{Layout, alloc, dealloc},
	ptr,
};

/// Memory arena for FFI allocations
///
/// All memory allocated from this arena is freed when the arena is dropped.
pub struct Arena {
	allocations: Vec<Allocation>,
}

struct Allocation {
	ptr: *mut u8,
	layout: Layout,
}

// SAFETY: Arena manages raw pointers but ensures proper cleanup
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
	/// Create a new empty arena
	pub fn new() -> Self {
		Self {
			allocations: Vec::new(),
		}
	}

	/// Allocate memory from the arena
	///
	/// Returns null on allocation failure
	pub fn alloc(&mut self, size: usize) -> *mut u8 {
		if size == 0 {
			return ptr::null_mut();
		}

		// Align to 8 bytes for safety
		let layout = match Layout::from_size_align(size, 8) {
			Ok(layout) => layout,
			Err(_) => return ptr::null_mut(),
		};

		// SAFETY: We're allocating with a valid layout
		let ptr = unsafe { alloc(layout) };

		if ptr.is_null() {
			return ptr::null_mut();
		}

		self.allocations.push(Allocation {
			ptr,
			layout,
		});
		ptr
	}

	/// Allocate and zero-initialize memory from the arena
	pub fn alloc_zeroed(&mut self, size: usize) -> *mut u8 {
		let ptr = self.alloc(size);
		if !ptr.is_null() {
			// SAFETY: We just allocated this memory
			unsafe {
				ptr::write_bytes(ptr, 0, size);
			}
		}
		ptr
	}

	/// Allocate memory for a type T
	pub fn alloc_type<T>(&mut self) -> *mut T {
		self.alloc(std::mem::size_of::<T>()) as *mut T
	}

	/// Allocate and copy bytes into the arena
	pub fn copy_bytes(&mut self, bytes: &[u8]) -> *mut u8 {
		if bytes.is_empty() {
			return ptr::null_mut();
		}

		let ptr = self.alloc(bytes.len());
		if !ptr.is_null() {
			// SAFETY: We just allocated sufficient memory
			unsafe {
				ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
			}
		}
		ptr
	}

	/// Clear all allocations
	pub fn clear(&mut self) {
		// Free all allocations
		for allocation in self.allocations.drain(..) {
			// SAFETY: We allocated this memory with the same layout
			unsafe {
				dealloc(allocation.ptr, allocation.layout);
			}
		}
	}

	/// Get the number of allocations
	pub fn allocation_count(&self) -> usize {
		self.allocations.len()
	}

	/// Get the total allocated size
	pub fn total_size(&self) -> usize {
		self.allocations.iter().map(|a| a.layout.size()).sum()
	}
}

impl Default for Arena {
	fn default() -> Self {
		Self::new()
	}
}

impl Drop for Arena {
	fn drop(&mut self) {
		self.clear();
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_arena_basic() {
		let mut arena = Arena::new();

		let ptr1 = arena.alloc(100);
		assert!(!ptr1.is_null());

		let ptr2 = arena.alloc(200);
		assert!(!ptr2.is_null());

		assert_eq!(arena.allocation_count(), 2);
		assert_eq!(arena.total_size(), 300);
	}

	#[test]
	fn test_arena_zeroed() {
		let mut arena = Arena::new();

		let ptr = arena.alloc_zeroed(100);
		assert!(!ptr.is_null());

		unsafe {
			for i in 0..100 {
				assert_eq!(*ptr.add(i), 0);
			}
		}
	}

	#[test]
	fn test_arena_copy_bytes() {
		let mut arena = Arena::new();

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
		assert_eq!(arena.allocation_count(), 2);

		arena.clear();
		assert_eq!(arena.allocation_count(), 0);
	}
}
