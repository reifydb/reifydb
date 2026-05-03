// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	alloc::{Layout, alloc, dealloc, realloc as system_realloc},
	cell::RefCell,
	ptr,
};

use reifydb_sdk::ffi::arena::Arena;

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
			unsafe { (*arena_ptr).alloc(size) }
		} else {
			let layout = match Layout::from_size_align(size, 8) {
				Ok(layout) => layout,
				Err(_) => return ptr::null_mut(),
			};
			unsafe { alloc(layout) }
		}
	})
}

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
	unsafe { dealloc(ptr, layout) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_realloc(ptr: *mut u8, old_size: usize, new_size: usize) -> *mut u8 {
	if ptr.is_null() {
		return host_alloc(new_size);
	}

	if new_size == 0 {
		unsafe { host_free(ptr, old_size) };
		return ptr::null_mut();
	}

	CURRENT_ARENA.with(|a| {
		if let Some(arena_ptr) = *a.borrow() {
			let new_ptr = unsafe { (*arena_ptr).alloc(new_size) };
			if !new_ptr.is_null() {
				let copy_size = old_size.min(new_size);
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
			unsafe { system_realloc(ptr, old_layout, new_layout.size()) }
		}
	})
}
