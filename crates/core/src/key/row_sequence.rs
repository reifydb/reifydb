// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::shape::ShapeId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, PartialEq)]
pub struct RowSequenceKey {
	pub shape: ShapeId,
}

impl EncodableKey for RowSequenceKey {
	const KIND: KeyKind = KeyKind::RowSequence;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_shape_id(self.shape);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let shape = de.read_shape_id().ok()?;

		Some(Self {
			shape,
		})
	}
}

impl RowSequenceKey {
	pub fn encoded(shape: impl Into<ShapeId>) -> EncodedKey {
		Self {
			shape: shape.into(),
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
	use crate::interface::catalog::shape::ShapeId;

	#[test]
	fn test_encode_decode() {
		let key = RowSequenceKey {
			shape: ShapeId::table(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xF7, 0x01, 0x3F, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = RowSequenceKey::decode(&encoded).unwrap();
		assert_eq!(key.shape, ShapeId::table(0xABCD));
	}
}
