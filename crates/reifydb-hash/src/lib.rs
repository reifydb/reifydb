// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![no_std]
extern crate alloc;

mod xxh;

use core::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
pub use xxh::{xxh3_64, xxh3_128, xxh32, xxh64};

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash32(pub u32);

impl From<u32> for Hash32 {
	fn from(value: u32) -> Self {
		Hash32(value)
	}
}

impl From<Hash32> for u32 {
	fn from(hash: Hash32) -> Self {
		hash.0
	}
}

impl Hash for Hash32 {
	fn hash<H: Hasher>(&self, state: &mut H) {
		state.write_u32(self.0)
	}
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
