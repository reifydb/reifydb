//! Memory management callbacks for FFI operators
//!
//! Provides arena-based memory allocation for FFI operators with fallback to system allocator.

use std::{
	alloc::{Layout, alloc, dealloc, realloc as system_realloc},
	cell::RefCell,
};

use reifydb_flow_operator_sdk::ffi::Arena;

// Thread-local storage for the current arena
// All allocations during an FFI operation will use this arena
thread_local! {
	static CURRENT_ARENA: RefCell<Option<*mut Arena>> = RefCell::new(None);
}

/// Set the current arena for this thread
pub fn set_current_arena(arena: *mut Arena) {
	CURRENT_ARENA.with(|a| {
		*a.borrow_mut() = Some(arena);
	});
}

/// Clear the current arena for this thread
pub fn clear_current_arena() {
	CURRENT_ARENA.with(|a| {
		*a.borrow_mut() = None;
	});
}

/// Allocate memory from the current arena or system allocator
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_alloc(size: usize) -> *mut u8 {
	if size == 0 {
		return std::ptr::null_mut();
	}

	// Try to use the thread-local arena first
	CURRENT_ARENA.with(|a| {
		if let Some(arena_ptr) = *a.borrow() {
			unsafe { (*arena_ptr).alloc(size) }
		} else {
			// Fallback to system allocator if no arena set
			let layout = match Layout::from_size_align(size, 8) {
				Ok(layout) => layout,
				Err(_) => return std::ptr::null_mut(),
			};
			unsafe { alloc(layout) }
		}
	})
}

/// Free memory (no-op for arena memory, system free otherwise)
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_free(ptr: *mut u8, size: usize) {
	if ptr.is_null() || size == 0 {
		return;
	}

	// Check if this is arena-allocated memory
	CURRENT_ARENA.with(|a| {
		if (*a.borrow()).is_some() {
			// Arena memory is freed automatically - do nothing
			return;
		}
	});

	// Otherwise use system deallocator
	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return,
	};
	unsafe { dealloc(ptr, layout) }
}

/// Reallocate memory (allocates new for arena, uses system realloc otherwise)
#[unsafe(no_mangle)]
pub(super) extern "C" fn host_realloc(ptr: *mut u8, old_size: usize, new_size: usize) -> *mut u8 {
	// For arena allocations, we can't realloc in place, so alloc new and copy
	if ptr.is_null() {
		return host_alloc(new_size);
	}

	if new_size == 0 {
		host_free(ptr, old_size);
		return std::ptr::null_mut();
	}

	// Check if using arena
	CURRENT_ARENA.with(|a| {
		if let Some(arena_ptr) = *a.borrow() {
			// Arena can't realloc, so alloc new and copy
			let new_ptr = unsafe { (*arena_ptr).alloc(new_size) };
			if !new_ptr.is_null() {
				let copy_size = old_size.min(new_size);
				unsafe {
					std::ptr::copy_nonoverlapping(ptr, new_ptr, copy_size);
				}
			}
			// Note: old arena memory will be freed when arena is cleared
			new_ptr
		} else {
			// Use system realloc
			let old_layout = match Layout::from_size_align(old_size, 8) {
				Ok(layout) => layout,
				Err(_) => return std::ptr::null_mut(),
			};
			let new_layout = match Layout::from_size_align(new_size, 8) {
				Ok(layout) => layout,
				Err(_) => return std::ptr::null_mut(),
			};
			unsafe { system_realloc(ptr, old_layout, new_layout.size()) }
		}
	})
}
