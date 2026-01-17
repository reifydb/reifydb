// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_hash::Hash64;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

/// Key for storing a schema definition by its fingerprint
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaKey {
	pub fingerprint: Hash64,
}

const VERSION: u8 = 1;

impl EncodableKey for SchemaKey {
	const KIND: KeyKind = KeyKind::Schema;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.fingerprint.0);
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
			fingerprint: Hash64(fingerprint),
		})
	}
}

impl SchemaKey {
	pub fn encoded(fingerprint: Hash64) -> EncodedKey {
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
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 + 1);
		serializer.to_encoded_key()
	}
}

/// Key for storing individual schema fields
/// Keyed by (schema_fingerprint, field_index) for ordered retrieval
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaFieldKey {
	pub schema_fingerprint: Hash64,
	pub field_index: u8,
}

impl EncodableKey for SchemaFieldKey {
	const KIND: KeyKind = KeyKind::SchemaField;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.schema_fingerprint.0)
			.extend_u8(self.field_index);
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
		let field_index = de.read_u8().ok()?;

		Some(Self {
			schema_fingerprint: Hash64(schema_fingerprint),
			field_index,
		})
	}
}

impl SchemaFieldKey {
	pub fn encoded(schema_fingerprint: Hash64, field_index: u8) -> EncodedKey {
		Self {
			schema_fingerprint,
			field_index,
		}
		.encode()
	}

	/// Scan all fields for a given schema
	pub fn scan_for_schema(fingerprint: Hash64) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::schema_start(fingerprint)), Some(Self::schema_end(fingerprint)))
	}

	fn schema_start(fingerprint: Hash64) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(fingerprint.0);
		serializer.to_encoded_key()
	}

	fn schema_end(fingerprint: Hash64) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(fingerprint.0).extend_u8(0xFF);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_schema_key_encode_decode() {
		let key = SchemaKey {
			fingerprint: Hash64(0xDEADBEEFCAFEBABE),
		};
		let encoded = key.encode();
		let decoded = SchemaKey::decode(&encoded).unwrap();
		assert_eq!(decoded.fingerprint, Hash64(0xDEADBEEFCAFEBABE));
	}

	#[test]
	fn test_schema_field_key_encode_decode() {
		let key = SchemaFieldKey {
			schema_fingerprint: Hash64(0x1234567890ABCDEF),
			field_index: 42,
		};
		let encoded = key.encode();
		let decoded = SchemaFieldKey::decode(&encoded).unwrap();
		assert_eq!(decoded.schema_fingerprint, Hash64(0x1234567890ABCDEF));
		assert_eq!(decoded.field_index, 42);
	}
}
