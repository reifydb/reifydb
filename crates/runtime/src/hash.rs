// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Hash types and functions for ReifyDB.
//!
//! Provides xxHash3 hashing using pure Rust implementation that works
//! on both native and WASM targets.

use core::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash64(pub u64);

impl From<u64> for Hash64 {
	fn from(value: u64) -> Self {
		Hash64(value)
	}
}

impl From<Hash64> for u64 {
	fn from(hash: Hash64) -> Self {
		hash.0
	}
}

impl Hash for Hash64 {
	fn hash<H: Hasher>(&self, state: &mut H) {
		state.write_u64(self.0)
	}
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash128(pub u128);

impl From<u128> for Hash128 {
	fn from(value: u128) -> Self {
		Hash128(value)
	}
}

impl From<Hash128> for u128 {
	fn from(hash: Hash128) -> Self {
		hash.0
	}
}

impl Hash for Hash128 {
	fn hash<H: Hasher>(&self, state: &mut H) {
		state.write_u128(self.0)
	}
}

/// Compute xxHash3 64-bit hash of data.
#[inline]
pub fn xxh3_64(data: &[u8]) -> Hash64 {
	Hash64(xxh3::xxh3_64(data))
}

/// Compute xxHash3 128-bit hash of data.
#[inline]
pub fn xxh3_128(data: &[u8]) -> Hash128 {
	Hash128(xxh3::xxh3_128(data))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_xxh3_64() {
		let data = b"hello world";
		let hash = xxh3_64(data);
		// xxh3_64 should be deterministic
		assert_eq!(hash, xxh3_64(data));
		assert_ne!(hash, xxh3_64(b"different data"));
	}

	#[test]
	fn test_xxh3_128() {
		let data = b"hello world";
		let hash = xxh3_128(data);
		// xxh3_128 should be deterministic
		assert_eq!(hash, xxh3_128(data));
		assert_ne!(hash, xxh3_128(b"different data"));
	}

	#[test]
	fn test_hash64_conversions() {
		let value: u64 = 12345;
		let hash = Hash64::from(value);
		assert_eq!(u64::from(hash), value);
	}

	#[test]
	fn test_hash128_conversions() {
		let value: u128 = 123456789;
		let hash = Hash128::from(value);
		assert_eq!(u128::from(hash), value);
	}
}
