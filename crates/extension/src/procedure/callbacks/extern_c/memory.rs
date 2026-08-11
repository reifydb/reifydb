// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	alloc::{Layout, alloc, dealloc, realloc as system_realloc},
	cell::RefCell,
	ptr,
};

use reifydb_sdk::extern_c::arena::Arena;

thread_local! {
	static CURRENT_ARENA: RefCell<Option<*mut Arena>> = const { RefCell::new(None) };
}

pub fn set_current_arena(arena: *mut Arena) {
	CURRENT_ARENA.with(|a| {
		*a.borrow_mut() = Some(arena);
	});
}

pub fn clear_current_arena() {
	CURRENT_ARENA.with(|a| {
		*a.borrow_mut() = None;
	});
}

#[unsafe(no_mangle)]
pub extern "C" fn host_alloc(size: usize) -> *mut u8 {
	if size == 0 {
		return ptr::null_mut();
	}

	CURRENT_ARENA.with(|a| {
		if let Some(arena_ptr) = *a.borrow() {
			// SAFETY: set_current_arena's caller must keep the arena alive until it clears it.
			unsafe { (*arena_ptr).alloc(size) }
		} else {
			let layout = match Layout::from_size_align(size, 8) {
				Ok(layout) => layout,
				Err(_) => return ptr::null_mut(),
			};
			// SAFETY: size is non-zero (checked above), so the layout has non-zero size.
			unsafe { alloc(layout) }
		}
	})
}

/// # Safety
///
/// - `ptr` must have been previously returned by `host_alloc` or `host_realloc`, or be null.
/// - `size` must match the size used in the corresponding allocation.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_free(ptr: *mut u8, size: usize) {
	if ptr.is_null() || size == 0 {
		return;
	}

	CURRENT_ARENA.with(|a| if (*a.borrow()).is_some() {});

	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return,
	};
	// SAFETY: unconditionally a global-allocator free, so ptr must not have come from an arena.
	unsafe { dealloc(ptr, layout) }
}

/// # Safety
///
/// - `ptr` must have been previously returned by `host_alloc` or `host_realloc`, or be null.
/// - `old_size` must match the size of the current allocation at `ptr`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_realloc(ptr: *mut u8, old_size: usize, new_size: usize) -> *mut u8 {
	if ptr.is_null() {
		return host_alloc(new_size);
	}

	if new_size == 0 {
		// SAFETY: forwards the caller's own guarantee that ptr/old_size describe a live allocation.
		unsafe { host_free(ptr, old_size) };
		return ptr::null_mut();
	}

	CURRENT_ARENA.with(|a| {
		if let Some(arena_ptr) = *a.borrow() {
			// SAFETY: set_current_arena's caller must keep the arena alive until it clears it.
			let new_ptr = unsafe { (*arena_ptr).alloc(new_size) };
			if !new_ptr.is_null() {
				let copy_size = old_size.min(new_size);
				// SAFETY: copy_size fits both blocks and a fresh arena block cannot overlap.
				unsafe {
					ptr::copy_nonoverlapping(ptr, new_ptr, copy_size);
				}
			}

			new_ptr
		} else {
			let old_layout = match Layout::from_size_align(old_size, 8) {
				Ok(layout) => layout,
				Err(_) => return ptr::null_mut(),
			};
			let new_layout = match Layout::from_size_align(new_size, 8) {
				Ok(layout) => layout,
				Err(_) => return ptr::null_mut(),
			};
			// SAFETY: reached only with no arena installed, so ptr must be a global-allocator
			// block matching old_layout; new_size is non-zero (checked above).
			unsafe { system_realloc(ptr, old_layout, new_layout.size()) }
		}
	})
}
