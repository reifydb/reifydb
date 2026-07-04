// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::identity::IdentityId;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::identity::IdentityAttributeId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct IdentityAttributeValueKey {
	pub identity: IdentityId,
	pub attribute: IdentityAttributeId,
}

impl IdentityAttributeValueKey {
	pub fn new(identity: IdentityId, attribute: IdentityAttributeId) -> Self {
		Self {
			identity,
			attribute,
		}
	}

	pub fn encoded(identity: IdentityId, attribute: IdentityAttributeId) -> EncodedKey {
		Self::new(identity, attribute).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(1);
		start.extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(1);
		end.extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}

	pub fn identity_scan(identity: IdentityId) -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(17);
		start.extend_u8(Self::KIND as u8).extend_identity_id(&identity);
		let mut end = KeySerializer::with_capacity(17);
		end.extend_u8(Self::KIND as u8).extend_identity_id(&identity);

		let start_key = start.to_encoded_key();
		let mut end_bytes = end.to_encoded_key().to_vec();
		end_bytes.push(0xFF);
		EncodedKeyRange::start_end(Some(start_key), Some(EncodedKey::new(end_bytes)))
	}
}

impl EncodableKey for IdentityAttributeValueKey {
	const KIND: KeyKind = KeyKind::IdentityAttributeValue;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(25);
		serializer.extend_u8(Self::KIND as u8).extend_identity_id(&self.identity).extend_u64(self.attribute);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}
		let identity = de.read_identity_id().ok()?;
		let attribute = de.read_u64().ok()?;
		Some(Self {
			identity,
			attribute,
		})
	}
}
