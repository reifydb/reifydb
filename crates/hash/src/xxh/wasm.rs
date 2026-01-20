// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pure Rust xxhash implementation for WASM builds
//! Uses xxhash-rust crate as a drop-in replacement for C bindings

use xxhash_rust::{xxh32, xxh64, xxh3};

/// XXH32 hash function - pure Rust implementation
#[inline(always)]
pub fn XXH32(input: *const u8, length: usize, seed: u32) -> u32 {
	let slice = unsafe { std::slice::from_raw_parts(input, length) };
	xxh32::xxh32(slice, seed)
}

/// XXH64 hash function - pure Rust implementation
#[inline(always)]
pub fn XXH64(input: *const u8, length: usize, seed: u64) -> u64 {
	let slice = unsafe { std::slice::from_raw_parts(input, length) };
	xxh64::xxh64(slice, seed)
}

/// XXH3 64-bit hash function - pure Rust implementation
#[inline(always)]
pub fn XXH3_64bits(data: *const u8, len: usize) -> u64 {
	let slice = unsafe { std::slice::from_raw_parts(data, len) };
	xxh3::xxh3_64(slice)
}

/// Hash128 structure matching C FFI layout
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub(crate) struct Hash128 {
	pub low: u64,
	pub high: u64,
}

/// XXH3 128-bit hash function - pure Rust implementation
#[inline(always)]
pub(crate) fn XXH3_128bits(data: *const u8, len: usize) -> Hash128 {
	let slice = unsafe { std::slice::from_raw_parts(data, len) };
	let hash = xxh3::xxh3_128(slice);
	Hash128 {
		low: hash as u64,
		high: (hash >> 64) as u64,
	}
}
