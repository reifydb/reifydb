// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod sha1;
pub mod xxh;

use core::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

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

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash160(pub [u8; 20]);

impl From<[u8; 20]> for Hash160 {
	fn from(value: [u8; 20]) -> Self {
		Hash160(value)
	}
}

impl From<Hash160> for [u8; 20] {
	fn from(hash: Hash160) -> Self {
		hash.0
	}
}

impl Hash for Hash160 {
	fn hash<H: Hasher>(&self, state: &mut H) {
		state.write(&self.0)
	}
}
