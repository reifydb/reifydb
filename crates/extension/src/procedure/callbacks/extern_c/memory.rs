// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	alloc::{Layout, alloc, dealloc},
	ptr,
};

#[unsafe(no_mangle)]
pub extern "C" fn host_alloc(size: usize) -> *mut u8 {
	if size == 0 {
		return ptr::null_mut();
	}

	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return ptr::null_mut(),
	};
	// SAFETY: size is non-zero (checked above), so the layout has non-zero size.
	unsafe { alloc(layout) }
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

	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return,
	};
	// SAFETY: every host_alloc block comes from the global allocator at align 8, and the caller passes back the
	// non-null pointer and the size host_alloc returned it for, so this layout is the one it was allocated with.
	unsafe { dealloc(ptr, layout) }
}
