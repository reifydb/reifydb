// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		shape::fingerprint::RowShapeFingerprint,
	},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

/// Key for storing a shape definition by its fingerprint
#[derive(Debug, Clone, PartialEq)]
pub struct RowShapeKey {
	pub fingerprint: RowShapeFingerprint,
}

const VERSION: u8 = 1;

impl EncodableKey for RowShapeKey {
	const KIND: KeyKind = KeyKind::Shape;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.fingerprint.as_u64());
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

		let fingerprint = de.read_u64().ok()?;

		Some(Self {
			fingerprint: RowShapeFingerprint::new(fingerprint),
		})
	}
}

impl RowShapeKey {
	pub fn encoded(fingerprint: RowShapeFingerprint) -> EncodedKey {
		Self {
			fingerprint,
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::scan_start()), Some(Self::scan_end()))
	}

	fn scan_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn scan_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

/// Key for storing individual shape fields
/// Keyed by (shape_fingerprint, field_index) for ordered retrieval
#[derive(Debug, Clone, PartialEq)]
pub struct RowShapeFieldKey {
	pub shape_fingerprint: RowShapeFingerprint,
	pub field_index: u16,
}

impl EncodableKey for RowShapeFieldKey {
	const KIND: KeyKind = KeyKind::RowShapeField;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.shape_fingerprint.as_u64())
			.extend_u16(self.field_index);
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

		let shape_fingerprint = de.read_u64().ok()?;
		let field_index = de.read_u16().ok()?;

		Some(Self {
			shape_fingerprint: RowShapeFingerprint::new(shape_fingerprint),
			field_index,
		})
	}
}

impl RowShapeFieldKey {
	pub fn encoded(shape_fingerprint: RowShapeFingerprint, field_index: u16) -> EncodedKey {
		Self {
			shape_fingerprint,
			field_index,
		}
		.encode()
	}

	/// Scan all fields for a given shape
	pub fn scan_for_shape(fingerprint: RowShapeFingerprint) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::shape_start(fingerprint)), Some(Self::shape_end(fingerprint)))
	}

	fn shape_start(fingerprint: RowShapeFingerprint) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(fingerprint.as_u64());
		serializer.to_encoded_key()
	}

	fn shape_end(fingerprint: RowShapeFingerprint) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(fingerprint.as_u64())
			.extend_u8(0xFF);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_shape_key_encode_decode() {
		let key = RowShapeKey {
			fingerprint: RowShapeFingerprint::new(0xDEADBEEFCAFEBABE),
		};
		let encoded = key.encode();
		let decoded = RowShapeKey::decode(&encoded).unwrap();
		assert_eq!(decoded.fingerprint, RowShapeFingerprint::new(0xDEADBEEFCAFEBABE));
	}

	#[test]
	fn test_shape_field_key_encode_decode() {
		let key = RowShapeFieldKey {
			shape_fingerprint: RowShapeFingerprint::new(0x1234567890ABCDEF),
			field_index: 42,
		};
		let encoded = key.encode();
		let decoded = RowShapeFieldKey::decode(&encoded).unwrap();
		assert_eq!(decoded.shape_fingerprint, RowShapeFingerprint::new(0x1234567890ABCDEF));
		assert_eq!(decoded.field_index, 42);
	}
}
