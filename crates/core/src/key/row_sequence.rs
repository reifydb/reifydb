// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::object::ObjectId,
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
pub struct RowSequenceKey {
	pub object: ObjectId,
}

impl EncodableKey for RowSequenceKey {
	const KIND: KeyKind = KeyKind::RowSequence;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.object);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let object = de.read_object_id().ok()?;

		Some(Self {
			object,
		})
	}
}

impl RowSequenceKey {
	pub fn encoded(object: impl Into<ObjectId>) -> EncodedKey {
		Self {
			object: object.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::sequence_start()), Some(Self::sequence_end()))
	}

	fn sequence_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn sequence_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, RowSequenceKey};
	use crate::interface::catalog::object::ObjectId;

	#[test]
	fn test_encode_decode() {
		let key = RowSequenceKey {
			object: ObjectId::table(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xF7, 0x01, 0x3F, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = RowSequenceKey::decode(&encoded).unwrap();
		assert_eq!(key.object, ObjectId::table(0xABCD));
	}
}
