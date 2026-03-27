// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		schema::fingerprint::RowSchemaFingerprint,
	},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

/// Key for storing a schema definition by its fingerprint
#[derive(Debug, Clone, PartialEq)]
pub struct RowSchemaKey {
	pub fingerprint: RowSchemaFingerprint,
}

const VERSION: u8 = 1;

impl EncodableKey for RowSchemaKey {
	const KIND: KeyKind = KeyKind::Schema;

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
			fingerprint: RowSchemaFingerprint::new(fingerprint),
		})
	}
}

impl RowSchemaKey {
	pub fn encoded(fingerprint: RowSchemaFingerprint) -> EncodedKey {
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

/// Key for storing individual schema fields
/// Keyed by (schema_fingerprint, field_index) for ordered retrieval
#[derive(Debug, Clone, PartialEq)]
pub struct RowSchemaFieldKey {
	pub schema_fingerprint: RowSchemaFingerprint,
	pub field_index: u16,
}

impl EncodableKey for RowSchemaFieldKey {
	const KIND: KeyKind = KeyKind::RowSchemaField;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.schema_fingerprint.as_u64())
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

		let schema_fingerprint = de.read_u64().ok()?;
		let field_index = de.read_u16().ok()?;

		Some(Self {
			schema_fingerprint: RowSchemaFingerprint::new(schema_fingerprint),
			field_index,
		})
	}
}

impl RowSchemaFieldKey {
	pub fn encoded(schema_fingerprint: RowSchemaFingerprint, field_index: u16) -> EncodedKey {
		Self {
			schema_fingerprint,
			field_index,
		}
		.encode()
	}

	/// Scan all fields for a given schema
	pub fn scan_for_schema(fingerprint: RowSchemaFingerprint) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::schema_start(fingerprint)), Some(Self::schema_end(fingerprint)))
	}

	fn schema_start(fingerprint: RowSchemaFingerprint) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(fingerprint.as_u64());
		serializer.to_encoded_key()
	}

	fn schema_end(fingerprint: RowSchemaFingerprint) -> EncodedKey {
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
	fn test_schema_key_encode_decode() {
		let key = RowSchemaKey {
			fingerprint: RowSchemaFingerprint::new(0xDEADBEEFCAFEBABE),
		};
		let encoded = key.encode();
		let decoded = RowSchemaKey::decode(&encoded).unwrap();
		assert_eq!(decoded.fingerprint, RowSchemaFingerprint::new(0xDEADBEEFCAFEBABE));
	}

	#[test]
	fn test_schema_field_key_encode_decode() {
		let key = RowSchemaFieldKey {
			schema_fingerprint: RowSchemaFingerprint::new(0x1234567890ABCDEF),
			field_index: 42,
		};
		let encoded = key.encode();
		let decoded = RowSchemaFieldKey::decode(&encoded).unwrap();
		assert_eq!(decoded.schema_fingerprint, RowSchemaFingerprint::new(0x1234567890ABCDEF));
		assert_eq!(decoded.field_index, 42);
	}
}
