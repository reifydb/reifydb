// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pure Rust SHA1 implementation for WASM builds
//! Uses sha1 crate as a drop-in replacement for C bindings

use sha1::{Digest, Sha1 as Sha1Hasher};

/// SHA1_CTX structure matching C FFI layout
/// This is used for incremental hashing
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct SHA1_CTX {
	pub state: [u32; 5],
	pub count: [u32; 2],
	pub buffer: [u8; 64],
}

/// Initialize SHA1 context
pub(crate) fn SHA1Init(context: *mut SHA1_CTX) {
	// For the pure Rust implementation, we don't need to initialize the context
	// as we'll create fresh hashers when needed. But we zero it for consistency.
	unsafe {
		*context = SHA1_CTX {
			state: [0; 5],
			count: [0; 2],
			buffer: [0; 64],
		};
	}
}

/// Update SHA1 context with data
/// Note: This implementation doesn't truly use incremental hashing from the context
/// because we can't store the hasher state in the C-compatible struct.
/// For now, this is a limitation of the WASM implementation.
pub(crate) fn SHA1Update(_context: *mut SHA1_CTX, _data: *const u8, _len: u32) {
	// This function is part of the FFI interface but is not fully compatible
	// with incremental updates in the WASM build. Users should use SHA1Final
	// after collecting all data, or use the high-level Sha1 wrapper.
	panic!("SHA1Update incremental updates not supported in WASM build. Use high-level Sha1 API instead.");
}

/// Finalize SHA1 hash
pub(crate) fn SHA1Final(_digest: *mut u8, _context: *mut SHA1_CTX) {
	// Same limitation as SHA1Update
	panic!("SHA1Final not supported in WASM build. Use SHA1 one-shot function or high-level Sha1 API instead.");
}

/// One-shot SHA1 hash function - pure Rust implementation
pub(crate) fn SHA1(hash_out: *mut u8, str: *const u8, len: u32) {
	let slice = unsafe { std::slice::from_raw_parts(str, len as usize) };
	let mut hasher = Sha1Hasher::new();
	hasher.update(slice);
	let result = hasher.finalize();

	unsafe {
		std::ptr::copy_nonoverlapping(result.as_ptr(), hash_out, 20);
	}
}

// High-level wrapper for incremental SHA1 hashing in WASM
// This provides proper incremental hashing support
pub struct Sha1 {
	hasher: Sha1Hasher,
}

impl Sha1 {
	pub fn new() -> Self {
		Self {
			hasher: Sha1Hasher::new(),
		}
	}

	pub fn update(&mut self, data: &[u8]) {
		self.hasher.update(data);
	}

	pub fn finalize(self) -> crate::Hash160 {
		let result = self.hasher.finalize();
		let mut digest = [0u8; 20];
		digest.copy_from_slice(&result);
		crate::Hash160(digest)
	}
}

impl Default for Sha1 {
	fn default() -> Self {
		Self::new()
	}
}
