// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::policy::PolicyId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

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
		let mut start = KeySerializer::with_capacity(2);
		start.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(2);
		end.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}

	pub fn policy_scan(policy: PolicyId) -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(10);
		start.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(policy);
		let mut end = KeySerializer::with_capacity(18);
		end.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(policy);
		let start_key = start.to_encoded_key();
		let mut end_bytes = end.to_encoded_key().to_vec();
		// Append 8 0xFF bytes to cover the full op_index field range
		end_bytes.extend_from_slice(&[0xFF; 8]);
		EncodedKeyRange::start_end(Some(start_key), Some(EncodedKey::new(end_bytes)))
	}
}

impl EncodableKey for PolicyOpKey {
	const KIND: KeyKind = KeyKind::PolicyOp;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.policy)
			.extend_u64(self.op_index);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}
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
