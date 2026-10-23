// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ptr, slice::from_raw_parts};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_sdk::common::extern_c::wire::{
	buffer::ExternCBuffer,
	key_ref::ExternCKeyRef,
	status::{EXTERN_C_ERROR_ALLOC, EXTERN_C_OK},
};

use crate::procedure::callbacks::extern_c::memory::host_alloc;

// SAFETY: `keys` must be valid for reads of `len` ExternCKeyRef entries, and every entry with a
// non-null `ptr` and non-zero `len` must be valid for reads of that many bytes. A null `ptr` on a
// non-empty entry is rejected rather than read.
pub(super) unsafe fn encoded_keys(keys: *const ExternCKeyRef, len: usize) -> Option<Vec<EncodedKey>> {
	if len == 0 {
		return Some(Vec::new());
	}
	// SAFETY: `len` is non-zero here, so this function's contract makes `keys` non-null, aligned and
	// valid for reads of `len` ExternCKeyRef entries.
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

// SAFETY: `output` must be valid for writes of a ExternCBuffer and properly aligned. Ownership of the
// host-allocated `ptr` it is given transfers to the caller.
pub(super) unsafe fn write_buffer(output: *mut ExternCBuffer, bytes: &[u8]) -> i32 {
	if bytes.is_empty() {
		// SAFETY: this function's contract makes `output` valid and aligned for one ExternCBuffer write.
		unsafe {
			*output = ExternCBuffer::empty();
		}
		return EXTERN_C_OK;
	}
	let dst = host_alloc(bytes.len());
	if dst.is_null() {
		return EXTERN_C_ERROR_ALLOC;
	}
	// SAFETY: `dst` is a fresh host_alloc block of `bytes.len()` bytes (null return handled above) so
	// it cannot overlap `bytes`, and this function's contract makes `output` valid and aligned for one
	// ExternCBuffer write; ExternCBuffer is Copy, so the field writes drop nothing uninitialised.
	unsafe {
		ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
		(*output).ptr = dst;
		(*output).len = bytes.len();
		(*output).cap = bytes.len();
	}
	EXTERN_C_OK
}
