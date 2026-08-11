// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	alloc::{Layout, alloc, dealloc},
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
/// - `ptr` must have been previously returned by `host_alloc`, or be null.
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
