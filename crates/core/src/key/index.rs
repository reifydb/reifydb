// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::Bound;

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::{
		id::{IndexId, PrimaryKeyId},
		schema::SchemaId,
	},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexKey {
	pub object: SchemaId,
	pub index: IndexId,
}

impl EncodableKey for IndexKey {
	const KIND: KeyKind = KeyKind::Index;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(19);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_schema_id(self.object)
			.extend_u64(self.index);
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

		let object = de.read_schema_id().ok()?;
		let index_value = de.read_u64().ok()?;

		Some(Self {
			object,
			index: IndexId::Primary(PrimaryKeyId(index_value)),
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaIndexKeyRange {
	pub object: SchemaId,
}

impl SchemaIndexKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let object = de.read_schema_id().ok()?;

		Some(SchemaIndexKeyRange {
			object,
		})
	}
}

impl EncodableKeyRange for SchemaIndexKeyRange {
	const KIND: KeyKind = KeyKind::Index;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_schema_id(self.object);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_schema_id(self.object.prev());
		Some(serializer.to_encoded_key())
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
		let start_key = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		let end_key = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		(start_key, end_key)
	}
}

impl IndexKey {
	pub fn encoded(object: impl Into<SchemaId>, index: impl Into<IndexId>) -> EncodedKey {
		Self {
			object: object.into(),
			index: index.into(),
		}
		.encode()
	}

	pub fn full_scan(object: impl Into<SchemaId>) -> EncodedKeyRange {
		let object = object.into();
		EncodedKeyRange::start_end(Some(Self::object_start(object)), Some(Self::object_end(object)))
	}

	pub fn object_start(object: impl Into<SchemaId>) -> EncodedKey {
		let object = object.into();
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_schema_id(object);
		serializer.to_encoded_key()
	}

	pub fn object_end(object: impl Into<SchemaId>) -> EncodedKey {
		let object = object.into();
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_schema_id(object.prev());
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{EncodableKey, IndexKey};
	use crate::interface::catalog::{id::IndexId, schema::SchemaId};

	#[test]
	fn test_encode_decode() {
		let key = IndexKey {
			object: SchemaId::table(0xABCD),
			index: IndexId::primary(0x123456789ABCDEF0u64),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xF3, // kind
			0x01, // SchemaId type discriminator (Table)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, // object id bytes
			0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F, // index id bytes
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = IndexKey::decode(&encoded).unwrap();
		assert_eq!(key.object, 0xABCD);
		assert_eq!(key.index, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = IndexKey {
			object: SchemaId::table(1),
			index: IndexId::primary(100),
		};
		let key2 = IndexKey {
			object: SchemaId::table(1),
			index: IndexId::primary(200),
		};
		let key3 = IndexKey {
			object: SchemaId::table(2),
			index: IndexId::primary(50),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
