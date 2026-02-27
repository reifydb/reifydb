// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::{ColumnId, ColumnPropertyId},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnPropertyKey {
	pub column: ColumnId,
	pub property: ColumnPropertyId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnPropertyKey {
	const KIND: KeyKind = KeyKind::ColumnProperty;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.column)
			.extend_u64(self.property);
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

		let column = de.read_u64().ok()?;
		let policy = de.read_u64().ok()?;

		Some(Self {
			column: ColumnId(column),
			property: ColumnPropertyId(policy),
		})
	}
}

impl ColumnPropertyKey {
	pub fn encoded(column: impl Into<ColumnId>, property: impl Into<ColumnPropertyId>) -> EncodedKey {
		Self {
			column: column.into(),
			property: property.into(),
		}
		.encode()
	}

	pub fn full_scan(column: ColumnId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(column)), Some(Self::link_end(column)))
	}

	fn link_start(column: ColumnId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(column);
		serializer.to_encoded_key()
	}

	fn link_end(column: ColumnId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*column - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{ColumnPropertyKey, EncodableKey};
	use crate::interface::catalog::id::{ColumnId, ColumnPropertyId};

	#[test]
	fn test_encode_decode() {
		let key = ColumnPropertyKey {
			column: ColumnId(0xABCD),
			property: ColumnPropertyId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFE, // version
			0xF6, // kind
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = ColumnPropertyKey::decode(&encoded).unwrap();
		assert_eq!(key.column, 0xABCD);
		assert_eq!(key.property, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = ColumnPropertyKey {
			column: ColumnId(1),
			property: ColumnPropertyId(100),
		};
		let key2 = ColumnPropertyKey {
			column: ColumnId(1),
			property: ColumnPropertyId(200),
		};
		let key3 = ColumnPropertyKey {
			column: ColumnId(2),
			property: ColumnPropertyId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
