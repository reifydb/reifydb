// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ptr, slice::from_raw_parts};

use reifydb_abi::{
	constants::{FFI_ERROR_ALLOC, FFI_OK},
	data::{buffer::BufferFFI, key_ref::KeyRefFFI},
};
use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::key::operator_state::GroupStateKey;
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
pub(super) unsafe fn state_key(ptr: *const u8, len: usize) -> Option<GroupStateKey> {
	// SAFETY: forwards this function's own contract to encoded_key unchanged.
	GroupStateKey::from_framed(unsafe { encoded_key(ptr, len) })
}

// SAFETY: `keys` must be valid for reads of `len` KeyRefFFI entries, and every entry with a
// non-null `ptr` and non-zero `len` must be valid for reads of that many bytes. A null `ptr` on a
// non-empty entry is rejected rather than read.
pub(super) unsafe fn encoded_keys(keys: *const KeyRefFFI, len: usize) -> Option<Vec<EncodedKey>> {
	if len == 0 {
		return Some(Vec::new());
	}
	// SAFETY: `len` is non-zero here, so this function's contract makes `keys` non-null, aligned and
	// valid for reads of `len` KeyRefFFI entries.
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
		// SAFETY: `key.len` is non-zero and `key.ptr` non-null on this path, so the entry is valid
		// for reads of `key.len` bytes per this function's contract; the bytes are copied out here.
		encoded.push(EncodedKey::new(unsafe { from_raw_parts(key.ptr, key.len) }));
	}
	Some(encoded)
}

// SAFETY: `ptr` must be valid for reads of `len` bytes.
pub(super) unsafe fn encoded_bytes(ptr: *const u8, len: usize) -> EncodedBytes {
	EncodedBytes(CowVec::new(unsafe { from_raw_parts(ptr, len) }.to_vec()))
}

// SAFETY: `output` must be valid for writes of a BufferFFI and properly aligned. Ownership of the
// host-allocated `ptr` it is given transfers to the caller.
pub(super) unsafe fn write_buffer(output: *mut BufferFFI, bytes: &[u8]) -> i32 {
	let dst = host_alloc(bytes.len());
	if dst.is_null() {
		return FFI_ERROR_ALLOC;
	}
	// SAFETY: `dst` is a fresh host_alloc block of `bytes.len()` bytes (null return handled above) so
	// it cannot overlap `bytes`, and this function's contract makes `output` valid and aligned for one
	// BufferFFI write; BufferFFI is Copy, so the field writes drop nothing uninitialised.
	unsafe {
		ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
		(*output).ptr = dst;
		(*output).len = bytes.len();
		(*output).cap = bytes.len();
	}
	FFI_OK
}
