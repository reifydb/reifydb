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

#[cfg(test)]
mod tests {
	use std::ptr;

	use super::{host_alloc, host_free};

	#[test]
	fn every_block_host_alloc_hands_out_is_freed_by_host_free() {
		// A foreign block must never reach host_free, otherwise the free corrupts the heap.
		let sizes = (1..=4096).chain([8191, 8192, 65536, 1 << 20]);
		for size in sizes {
			let block = host_alloc(size);
			assert!(!block.is_null(), "a {size} byte request must be served");
			assert_eq!(block as usize % 8, 0, "a {size} byte block must be 8 byte aligned");
			// SAFETY: block is a live host_alloc block of exactly size bytes, written in bounds then freed
			// once with the size it was allocated with.
			unsafe {
				ptr::write_bytes(block, 0xA5, size);
				assert_eq!(*block.add(size - 1), 0xA5);
				host_free(block, size);
			}
		}
		assert!(host_alloc(0).is_null(), "a zero byte request must allocate nothing");
	}
}
