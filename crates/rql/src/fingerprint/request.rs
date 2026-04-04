// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use reifydb_core::fingerprint::StatementFingerprint;
use reifydb_runtime::hash::{Hash128, xxh3_128};
use serde::{Deserialize, Serialize};

/// Stable identity for an entire request (batch of statements).
///
/// Computed by combining the fingerprints of all individual statements
/// in the request, preserving order.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
		format!("0x{:032x}", self.0.0)
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

/// Compute a request fingerprint from individual statement fingerprints.
///
/// The request fingerprint combines all statement fingerprints in order,
/// so the same set of statements always produces the same request fingerprint.
pub fn fingerprint_request(statements: &[StatementFingerprint]) -> RequestFingerprint {
	let mut buf = Vec::with_capacity(statements.len() * 16);
	for fp in statements {
		buf.extend_from_slice(&StatementFingerprint::to_le_bytes(fp));
	}
	RequestFingerprint(xxh3_128(&buf))
}
