// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::storage::StorageId,
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
pub struct RowSequenceKey {
	pub storage: StorageId,
}

impl EncodableKey for RowSequenceKey {
	const KIND: KeyKind = KeyKind::RowSequence;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.storage);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let storage = StorageId::from_object(de.read_object_id().ok()?)?;

		Some(Self {
			storage,
		})
	}
}

impl RowSequenceKey {
	pub fn encoded(storage: impl Into<StorageId>) -> EncodedKey {
		Self {
			storage: storage.into(),
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
	use crate::interface::catalog::storage::StorageId;

	#[test]
	fn test_encode_decode() {
		let key = RowSequenceKey {
			storage: StorageId::table(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xF7, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = RowSequenceKey::decode(&encoded).unwrap();
		assert_eq!(key.storage, StorageId::table(0xABCD));
	}

	#[test]
	fn test_encode_decode_view() {
		// A view owns its row numbering; the view tag must survive the narrowing back through decode.
		let key = RowSequenceKey {
			storage: StorageId::view(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xF7, 0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = RowSequenceKey::decode(&encoded).unwrap();
		assert_eq!(key.storage, StorageId::view(0xABCD));
	}
}
