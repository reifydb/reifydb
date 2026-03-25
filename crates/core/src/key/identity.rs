// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::identity::IdentityId;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct IdentityKey {
	pub identity: IdentityId,
}

impl IdentityKey {
	pub fn new(identity: IdentityId) -> Self {
		Self {
			identity,
		}
	}

	pub fn encoded(identity: IdentityId) -> EncodedKey {
		Self::new(identity).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(2);
		start.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(2);
		end.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}
}

impl EncodableKey for IdentityKey {
	const KIND: KeyKind = KeyKind::Identity;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_identity_id(&self.identity);
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
		let identity = de.read_identity_id().ok()?;
		Some(Self {
			identity,
		})
	}
}
