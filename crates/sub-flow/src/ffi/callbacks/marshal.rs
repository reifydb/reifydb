// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ptr, slice::from_raw_parts};

use reifydb_abi::{
	constants::{FFI_ERROR_ALLOC, FFI_OK},
	data::buffer::BufferFFI,
};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_extension::procedure::ffi_callbacks::memory::host_alloc;
use reifydb_value::util::cowvec::CowVec;

// SAFETY: `ptr` must be valid for reads of `len` bytes.
pub(super) unsafe fn encoded_key(ptr: *const u8, len: usize) -> EncodedKey {
	EncodedKey::new(unsafe { from_raw_parts(ptr, len) }.to_vec())
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
