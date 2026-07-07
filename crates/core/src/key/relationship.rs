// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::id::RelationshipId;

#[derive(Debug, Clone)]
pub struct RelationshipKey {
	pub relationship: RelationshipId,
}

impl EncodableKey for RelationshipKey {
	const KIND: KeyKind = KeyKind::Relationship;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.relationship);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let relationship = de.read_u64().ok()?;

		Some(Self {
			relationship: RelationshipId(relationship),
		})
	}
}

impl RelationshipKey {
	pub fn encoded(relationship: impl Into<RelationshipId>) -> EncodedKey {
		Self {
			relationship: relationship.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::relationship_start()), Some(Self::relationship_end()))
	}

	fn relationship_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn relationship_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}
