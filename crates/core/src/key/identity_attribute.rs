// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::identity::IdentityAttributeId;

#[derive(Debug, Clone, PartialEq)]
pub struct IdentityAttributeKey {
	pub attribute: IdentityAttributeId,
}

impl IdentityAttributeKey {
	pub fn new(attribute: IdentityAttributeId) -> Self {
		Self {
			attribute,
		}
	}

	pub fn encoded(attribute: IdentityAttributeId) -> EncodedKey {
		Self::new(attribute).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(1);
		start.extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(1);
		end.extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}
}

impl EncodableKey for IdentityAttributeKey {
	const KIND: KeyKind = KeyKind::IdentityAttribute;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.attribute);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}
		let attribute = de.read_u64().ok()?;
		Some(Self {
			attribute,
		})
	}
}
