// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::{id::ColumnId, schema::SchemaId},
	key::{EncodableKey, KeyKind},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnKey {
	pub object: SchemaId,
	pub column: ColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnKey {
	const KIND: KeyKind = KeyKind::Column;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(19); // 1 + 1 + 9 + 8
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_schema_id(self.object)
			.extend_u64(self.column);
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
		let column = de.read_u64().ok()?;

		Some(Self {
			object,
			column: ColumnId(column),
		})
	}
}

impl ColumnKey {
	pub fn encoded(object: impl Into<SchemaId>, column: impl Into<ColumnId>) -> EncodedKey {
		Self {
			object: object.into(),
			column: column.into(),
		}
		.encode()
	}

	pub fn full_scan(object: impl Into<SchemaId>) -> EncodedKeyRange {
		let object = object.into();
		EncodedKeyRange::start_end(Some(Self::start(object)), Some(Self::end(object)))
	}

	fn start(object: SchemaId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_schema_id(object);
		serializer.to_encoded_key()
	}

	fn end(object: SchemaId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_schema_id(object.prev());
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::EncodableKey;
	use crate::{
		interface::catalog::{id::ColumnId, schema::SchemaId},
		key::ColumnKey,
	};

	#[test]
	fn test_encode_decode() {
		let key = ColumnKey {
			object: SchemaId::table(0xABCD),
			column: ColumnId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xF8, // kind
			0x01, // SchemaId type discriminator (Table)
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, // object id bytes
			0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F, // column id bytes
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = ColumnKey::decode(&encoded).unwrap();
		assert_eq!(key.object, 0xABCD);
		assert_eq!(key.column, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = ColumnKey {
			object: SchemaId::table(1),
			column: ColumnId(100),
		};
		let key2 = ColumnKey {
			object: SchemaId::table(1),
			column: ColumnId(200),
		};
		let key3 = ColumnKey {
			object: SchemaId::table(2),
			column: ColumnId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
