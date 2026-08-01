// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BufferFFI {
	pub ptr: *const u8,

	pub len: usize,

	pub cap: usize,
}

impl BufferFFI {
	pub const fn empty() -> Self {
		Self {
			ptr: core::ptr::null(),
			len: 0,
			cap: 0,
		}
	}

	pub fn from_slice(data: &[u8]) -> Self {
		Self {
			ptr: data.as_ptr(),
			len: data.len(),
			cap: data.len(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0 || self.ptr.is_null()
	}

	/// # Safety
	/// `ptr` must be valid for reads of `len` bytes, not freed, and unaliased mutably for the returned lifetime.
	pub unsafe fn as_slice(&self) -> &[u8] {
		if self.is_empty() {
			&[]
		} else {
			// SAFETY: ptr is non-null here and the caller guarantees len readable bytes.
			unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
		}
	}
}
