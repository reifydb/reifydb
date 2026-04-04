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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
		format!("0x{:032x}", self.0.0)
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
