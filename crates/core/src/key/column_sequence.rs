// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey,
	interface::{ColumnId, PrimitiveId},
	key::{EncodableKey, KeyKind},
	util::encoding::keycode::{KeyDeserializer, KeySerializer},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnSequenceKey {
	pub primitive: PrimitiveId,
	pub column: ColumnId,
}

const VERSION: u8 = 1;

impl EncodableKey for ColumnSequenceKey {
	const KIND: KeyKind = KeyKind::ColumnSequence;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(19); // 1 + 1 + 9 + 8
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_primitive_id(self.primitive)
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

		let primitive = de.read_primitive_id().ok()?;
		let column = de.read_u64().ok()?;

		Some(Self {
			primitive,
			column: ColumnId(column),
		})
	}
}

impl ColumnSequenceKey {
	pub fn encoded(primitive: impl Into<PrimitiveId>, column: impl Into<ColumnId>) -> EncodedKey {
		Self {
			primitive: primitive.into(),
			column: column.into(),
		}
		.encode()
	}
}

#[cfg(test)]
mod tests {
	use super::{ColumnSequenceKey, EncodableKey};
	use crate::{
		EncodedKey,
		interface::{ColumnId, PrimitiveId},
	};

	#[test]
	fn test_encode_decode() {
		let key = ColumnSequenceKey {
			primitive: PrimitiveId::table(0x1234),
			column: ColumnId(0x5678),
		};
		let encoded = key.encode();

		assert_eq!(encoded[0], 0xFE); // version serialized
		assert_eq!(encoded[1], 0xF1); // KeyKind::StoreColumnSequence serialized

		// Test decode
		let decoded = ColumnSequenceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.primitive, PrimitiveId::table(0x1234));
		assert_eq!(decoded.column, ColumnId(0x5678));
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = vec![0xFF]; // wrong version
		encoded.push(0x0E); // correct kind
		encoded.extend(&[0; 16]); // payload

		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = vec![0x01]; // correct version
		encoded.push(0xFF); // wrong kind
		encoded.extend(&[0; 16]); // payload

		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_length() {
		let encoded = vec![0x01, 0x0E]; // version and kind only, missing payload
		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}
}
