// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::dictionary::DictionaryId;
use serde::{Deserialize, Serialize};

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::{
		flow::FlowNodeId,
		id::{RingBufferId, SeriesId, TableId, ViewId},
		schema::SchemaId,
		vtable::VTableId,
	},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

/// Key for storing retention policy for a data object (table, view, ringbuffer)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRetentionPolicyKey {
	pub object: SchemaId,
}

impl SchemaRetentionPolicyKey {
	pub fn encoded(object: impl Into<SchemaId>) -> EncodedKey {
		Self {
			object: object.into(),
		}
		.encode()
	}
}

impl EncodableKey for SchemaRetentionPolicyKey {
	const KIND: KeyKind = KeyKind::SchemaRetentionPolicy;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);

		// Encode object_id with discriminator
		match &self.object {
			SchemaId::Table(id) => {
				serializer.extend_u8(0x01).extend_u64(id.0);
			}
			SchemaId::View(id) => {
				serializer.extend_u8(0x02).extend_u64(id.0);
			}
			SchemaId::TableVirtual(id) => {
				serializer.extend_u8(0x03).extend_u64(id.0);
			}
			SchemaId::RingBuffer(id) => {
				serializer.extend_u8(0x04).extend_u64(id.0);
			}
			SchemaId::Dictionary(id) => {
				serializer.extend_u8(0x06).extend_u64(id.0);
			}
			SchemaId::Series(id) => {
				serializer.extend_u8(0x07).extend_u64(id.0);
			}
		}

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

		let discriminator = de.read_u8().ok()?;
		let id = de.read_u64().ok()?;

		let object_id = match discriminator {
			0x01 => SchemaId::Table(TableId(id)),
			0x02 => SchemaId::View(ViewId(id)),
			0x03 => SchemaId::TableVirtual(VTableId(id)),
			0x04 => SchemaId::RingBuffer(RingBufferId(id)),
			0x06 => SchemaId::Dictionary(DictionaryId(id)),
			0x07 => SchemaId::Series(SeriesId(id)),
			_ => return None,
		};

		Some(Self {
			object: object_id,
		})
	}
}

/// Key for storing retention policy for a flow operator
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorRetentionPolicyKey {
	pub operator: FlowNodeId,
}

impl OperatorRetentionPolicyKey {
	pub fn encoded(operator: impl Into<FlowNodeId>) -> EncodedKey {
		Self {
			operator: operator.into(),
		}
		.encode()
	}
}

impl EncodableKey for OperatorRetentionPolicyKey {
	const KIND: KeyKind = KeyKind::OperatorRetentionPolicy;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.operator);
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

		Some(Self {
			operator: FlowNodeId(de.read_u64().ok()?),
		})
	}
}

/// Range for scanning all object retention policies
pub struct SchemaRetentionPolicyKeyRange;

impl SchemaRetentionPolicyKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(SchemaRetentionPolicyKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(SchemaRetentionPolicyKey::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

/// Range for scanning all operator retention policies
pub struct OperatorRetentionPolicyKeyRange;

impl OperatorRetentionPolicyKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(OperatorRetentionPolicyKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(OperatorRetentionPolicyKey::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_schema_retention_policy_key_encoding() {
		let key = SchemaRetentionPolicyKey {
			object: SchemaId::Table(TableId(42)),
		};

		let encoded = key.encode();
		assert_eq!(encoded[0], 0xFE); // version (1 encoded as !1)
		assert_eq!(encoded[1], 0xE8); // kind (0x17 encoded as !0x17)
		assert_eq!(&encoded[3..11], &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD5]);

		let decoded = SchemaRetentionPolicyKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_operator_retention_policy_key_encoding() {
		let key = OperatorRetentionPolicyKey {
			operator: FlowNodeId(12345),
		};

		let encoded = key.encode();
		assert_eq!(encoded[0], 0xFE); // version (1 encoded as !1)
		assert_eq!(encoded[1], 0xE7); // kind (0x18 encoded as !0x18)
		assert_eq!(&encoded[2..10], &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xCF, 0xC6]);

		let decoded = OperatorRetentionPolicyKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}
}
