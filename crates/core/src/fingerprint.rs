// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use reifydb_runtime::hash::Hash128;
use serde::{Deserialize, Serialize};

/// Stable identity for a query pattern, independent of literal values.
///
/// Two executions of `FROM t FILTER {x == 1}` and
/// `FROM t FILTER {x == 2}` produce the same fingerprint.
#[repr(transparent)]
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StatementFingerprint(pub Hash128);

impl Deref for StatementFingerprint {
	type Target = u128;

	fn deref(&self) -> &Self::Target {
		&self.0.0
	}
}

impl StatementFingerprint {
	#[inline]
	pub const fn new(value: u128) -> Self {
		Self(Hash128(value))
	}

	#[inline]
	pub const fn as_u128(&self) -> u128 {
		self.0.0
	}

	#[inline]
	pub const fn to_le_bytes(&self) -> [u8; 16] {
		self.0.0.to_le_bytes()
	}

	#[inline]
	pub const fn from_le_bytes(bytes: [u8; 16]) -> Self {
		Self(Hash128(u128::from_le_bytes(bytes)))
	}

	#[inline]
	pub fn to_hex(&self) -> String {
		self.0.to_hex_string_prefixed()
	}
}

impl From<Hash128> for StatementFingerprint {
	fn from(hash: Hash128) -> Self {
		Self(hash)
	}
}

impl From<StatementFingerprint> for Hash128 {
	fn from(fp: StatementFingerprint) -> Self {
		fp.0
	}
}

impl From<u128> for StatementFingerprint {
	fn from(value: u128) -> Self {
		Self(Hash128(value))
	}
}

/// Stable identity for an entire request (batch of statements).
///
/// Computed by combining the fingerprints of all individual statements
/// in the request, preserving order.
#[repr(transparent)]
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestFingerprint(pub Hash128);

impl Deref for RequestFingerprint {
	type Target = u128;

	fn deref(&self) -> &Self::Target {
		&self.0.0
	}
}

impl RequestFingerprint {
	#[inline]
	pub const fn new(value: u128) -> Self {
		Self(Hash128(value))
	}

	#[inline]
	pub const fn as_u128(&self) -> u128 {
		self.0.0
	}

	#[inline]
	pub const fn to_le_bytes(&self) -> [u8; 16] {
		self.0.0.to_le_bytes()
	}

	#[inline]
	pub const fn from_le_bytes(bytes: [u8; 16]) -> Self {
		Self(Hash128(u128::from_le_bytes(bytes)))
	}

	#[inline]
	pub fn to_hex(&self) -> String {
		self.0.to_hex_string_prefixed()
	}
}

impl From<Hash128> for RequestFingerprint {
	fn from(hash: Hash128) -> Self {
		Self(hash)
	}
}

impl From<RequestFingerprint> for Hash128 {
	fn from(fp: RequestFingerprint) -> Self {
		fp.0
	}
}

impl From<u128> for RequestFingerprint {
	fn from(value: u128) -> Self {
		Self(Hash128(value))
	}
}

/// Stable identity for a compiled query, keyed by the raw source text.
///
/// Used as the cache key for compiled instruction sequences, allowing
/// identical query strings to skip parsing and planning.
#[repr(transparent)]
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CompilationFingerprint(pub Hash128);

impl Deref for CompilationFingerprint {
	type Target = u128;

	fn deref(&self) -> &Self::Target {
		&self.0.0
	}
}

impl CompilationFingerprint {
	#[inline]
	pub const fn new(value: u128) -> Self {
		Self(Hash128(value))
	}

	#[inline]
	pub const fn as_u128(&self) -> u128 {
		self.0.0
	}

	#[inline]
	pub const fn to_le_bytes(&self) -> [u8; 16] {
		self.0.0.to_le_bytes()
	}

	#[inline]
	pub const fn from_le_bytes(bytes: [u8; 16]) -> Self {
		Self(Hash128(u128::from_le_bytes(bytes)))
	}

	#[inline]
	pub fn to_hex(&self) -> String {
		self.0.to_hex_string_prefixed()
	}
}

impl From<Hash128> for CompilationFingerprint {
	fn from(hash: Hash128) -> Self {
		Self(hash)
	}
}

impl From<CompilationFingerprint> for Hash128 {
	fn from(fp: CompilationFingerprint) -> Self {
		fp.0
	}
}

impl From<u128> for CompilationFingerprint {
	fn from(value: u128) -> Self {
		Self(Hash128(value))
	}
}
