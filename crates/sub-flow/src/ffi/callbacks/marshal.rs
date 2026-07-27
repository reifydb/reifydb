// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ptr, slice::from_raw_parts};

use reifydb_abi::{
	constants::{FFI_ERROR_ALLOC, FFI_OK},
	data::{buffer::BufferFFI, key_ref::KeyRefFFI},
};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::key::operator_state::StateKey;
use reifydb_extension::procedure::ffi_callbacks::memory::host_alloc;
use reifydb_value::util::cowvec::CowVec;

// SAFETY: `ptr` must be valid for reads of `len` bytes.
pub(super) unsafe fn encoded_key(ptr: *const u8, len: usize) -> EncodedKey {
	EncodedKey::new(unsafe { from_raw_parts(ptr, len) })
}

/// The FFI trust boundary for operator state keys. A dylib operator that hands over unframed bytes
/// gets an error code rather than a key that would address - and be reclaimed with - another group.
///
/// # Safety
/// Same contract as [`encoded_key`]: `ptr` must be valid for reads of `len` bytes.
pub(super) unsafe fn state_key(ptr: *const u8, len: usize) -> Option<StateKey> {
	StateKey::from_framed(unsafe { encoded_key(ptr, len) })
}

// SAFETY: `keys` must be valid for reads of `len` KeyRefFFI entries, and each entry's `ptr` must be

pub(super) unsafe fn encoded_keys(keys: *const KeyRefFFI, len: usize) -> Option<Vec<EncodedKey>> {
	if len == 0 {
		return Some(Vec::new());
	}
	let refs = unsafe { from_raw_parts(keys, len) };
	let mut encoded = Vec::with_capacity(len);
	for key in refs {
		if key.len == 0 {
			encoded.push(EncodedKey::new(Vec::new()));
			continue;
		}
		if key.ptr.is_null() {
			return None;
		}
		encoded.push(EncodedKey::new(unsafe { from_raw_parts(key.ptr, key.len) }));
	}
	Some(encoded)
}

// SAFETY: `ptr` must be valid for reads of `len` bytes.
pub(super) unsafe fn encoded_row(ptr: *const u8, len: usize) -> EncodedRow {
	EncodedRow(CowVec::new(unsafe { from_raw_parts(ptr, len) }.to_vec()))
}

// SAFETY: `output` must be a valid, writable pointer to a BufferFFI. Returns FFI_OK on success

pub(super) unsafe fn write_buffer(output: *mut BufferFFI, bytes: &[u8]) -> i32 {
	let dst = host_alloc(bytes.len());
	if dst.is_null() {
		return FFI_ERROR_ALLOC;
	}
	unsafe {
		ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
		(*output).ptr = dst;
		(*output).len = bytes.len();
		(*output).cap = bytes.len();
	}
	FFI_OK
}
