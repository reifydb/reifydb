// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::policy::SecurityPolicyId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct SecurityPolicyKey {
	pub policy: SecurityPolicyId,
}

impl SecurityPolicyKey {
	pub fn new(policy: SecurityPolicyId) -> Self {
		Self {
			policy,
		}
	}

	pub fn encoded(policy: SecurityPolicyId) -> EncodedKey {
		Self::new(policy).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(2);
		start.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(2);
		end.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}
}

impl EncodableKey for SecurityPolicyKey {
	const KIND: KeyKind = KeyKind::SecurityPolicy;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.policy);
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
		Some(Self {
			policy,
		})
	}
}
