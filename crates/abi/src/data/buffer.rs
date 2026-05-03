// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// FFI-safe buffer representing a slice of bytes.
///
/// **Ownership / borrow sentinel.** `cap == 0` is the borrow sentinel: the
/// pointer is borrowed from the host's native column storage, valid only for
/// the duration of the current FFI call (`apply` / `pull` / `tick`). The
/// guest must not retain the pointer past return, must not write through it,
/// and must not free it. `cap >= len` (typically `cap == len`) means the
/// buffer is owned by whoever produced it.
///
/// The host marshal path emits `cap == 0` for every borrowed column buffer
/// (numerics, bools, temporals, ids, var-len data + offsets, etc.).
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

	pub unsafe fn as_slice(&self) -> &[u8] {
		if self.is_empty() {
			&[]
		} else {
			// SAFETY: Caller must ensure pointer validity and lifetime
			unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
		}
	}
}
