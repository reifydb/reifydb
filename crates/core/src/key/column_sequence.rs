// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	encoded::key::EncodedKey,
	interface::catalog::{id::ColumnId, shape::ShapeId},
	key::{EncodableKey, KeyKind},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnSequenceKey {
	pub shape: ShapeId,
	pub column: ColumnId,
}

impl EncodableKey for ColumnSequenceKey {
	const KIND: KeyKind = KeyKind::ColumnSequence;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer.extend_u8(Self::KIND as u8).extend_shape_id(self.shape).extend_u64(self.column);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let shape = de.read_shape_id().ok()?;
		let column = de.read_u64().ok()?;

		Some(Self {
			shape,
			column: ColumnId(column),
		})
	}
}

impl ColumnSequenceKey {
	pub fn encoded(shape: impl Into<ShapeId>, column: impl Into<ColumnId>) -> EncodedKey {
		Self {
			shape: shape.into(),
			column: column.into(),
		}
		.encode()
	}
}

#[cfg(test)]
pub mod tests {
	use super::{ColumnSequenceKey, EncodableKey};
	use crate::{
		encoded::key::EncodedKey,
		interface::catalog::{id::ColumnId, shape::ShapeId},
	};

	#[test]
	fn test_encode_decode() {
		let key = ColumnSequenceKey {
			shape: ShapeId::table(0x1234),
			column: ColumnId(0x5678),
		};
		let encoded = key.encode();

		assert_eq!(encoded[0], 0xF1);

		let decoded = ColumnSequenceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.shape, ShapeId::table(0x1234));
		assert_eq!(decoded.column, ColumnId(0x5678));
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = vec![0xFF];
		encoded.push(0x0E);
		encoded.extend(&[0; 16]);

		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = vec![0x01];
		encoded.push(0xFF);
		encoded.extend(&[0; 16]);

		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_length() {
		let encoded = vec![0x01, 0x0E];
		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}
}
