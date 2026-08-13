// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::policy::PolicyId;

#[derive(Debug, Clone, PartialEq)]
pub struct PolicyOpKey {
	pub policy: PolicyId,
	pub op_index: u64,
}

impl PolicyOpKey {
	pub fn new(policy: PolicyId, op_index: u64) -> Self {
		Self {
			policy,
			op_index,
		}
	}

	pub fn encoded(policy: PolicyId, op_index: u64) -> EncodedKey {
		Self::new(policy, op_index).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(1);
		start.extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(1);
		end.extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}

	pub fn policy_scan(policy: PolicyId) -> EncodedKeyRange {
		let mut prefix = KeySerializer::with_capacity(9);
		prefix.extend_u8(Self::KIND as u8).extend_u64(policy);
		EncodedKeyRange::prefix(prefix.to_encoded_key().as_slice())
	}
}

impl EncodableKey for PolicyOpKey {
	const KIND: KeyKind = KeyKind::PolicyOp;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(17);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.policy).extend_u64(self.op_index);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}
		let policy = de.read_u64().ok()?;
		let op_index = de.read_u64().ok()?;
		Some(Self {
			policy,
			op_index,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use super::*;

	#[test]
	fn policy_scan_holds_every_op_index_of_that_policy() {
		// A fixed 0xFF-padded end bound only covers suffixes of exactly its own width.
		let range = PolicyOpKey::policy_scan(7);

		for op_index in [0u64, 1, 2, u64::MAX] {
			let key = PolicyOpKey::encoded(7, op_index);
			assert!(range.contains(&key), "op index {op_index} must fall inside the policy scan");
		}
	}

	#[test]
	fn policy_scan_excludes_a_neighbouring_policy() {
		// The scan must not widen into the next policy when the bound is carry-incremented.
		let range = PolicyOpKey::policy_scan(7);

		assert!(!range.contains(&PolicyOpKey::encoded(6, 1)));
		assert!(!range.contains(&PolicyOpKey::encoded(8, 1)));
	}
}
