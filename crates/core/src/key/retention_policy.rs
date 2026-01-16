// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::{
		flow::{FlowId, FlowNodeId},
		id::{DictionaryId, RingBufferId, TableId, ViewId},
		primitive::PrimitiveId,
		vtable::VTableId,
	},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

/// Key for storing retention policy for a data primitive (table, view, ringbuffer)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimitiveRetentionPolicyKey {
	pub primitive: PrimitiveId,
}

impl PrimitiveRetentionPolicyKey {
	pub fn encoded(primitive: impl Into<PrimitiveId>) -> EncodedKey {
		Self {
			primitive: primitive.into(),
		}
		.encode()
	}
}

impl EncodableKey for PrimitiveRetentionPolicyKey {
	const KIND: KeyKind = KeyKind::PrimitiveRetentionPolicy;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);

		// Encode primitive_id with discriminator
		match &self.primitive {
			PrimitiveId::Table(id) => {
				serializer.extend_u8(0x01).extend_u64(id.0);
			}
			PrimitiveId::View(id) => {
				serializer.extend_u8(0x02).extend_u64(id.0);
			}
			PrimitiveId::Flow(id) => {
				serializer.extend_u8(0x05).extend_u64(id.0);
			}
			PrimitiveId::TableVirtual(id) => {
				serializer.extend_u8(0x03).extend_u64(id.0);
			}
			PrimitiveId::RingBuffer(id) => {
				serializer.extend_u8(0x04).extend_u64(id.0);
			}
			PrimitiveId::Dictionary(id) => {
				serializer.extend_u8(0x06).extend_u64(id.0);
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

		let primitive_id = match discriminator {
			0x01 => PrimitiveId::Table(TableId(id)),
			0x02 => PrimitiveId::View(ViewId(id)),
			0x03 => PrimitiveId::TableVirtual(VTableId(id)),
			0x04 => PrimitiveId::RingBuffer(RingBufferId(id)),
			0x05 => PrimitiveId::Flow(FlowId(id)),
			0x06 => PrimitiveId::Dictionary(DictionaryId(id)),
			_ => return None,
		};

		Some(Self {
			primitive: primitive_id,
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

/// Range for scanning all primitive retention policies
pub struct PrimitiveRetentionPolicyKeyRange;

impl PrimitiveRetentionPolicyKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(PrimitiveRetentionPolicyKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(PrimitiveRetentionPolicyKey::KIND as u8 - 1);
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
	fn test_primitive_retention_policy_key_encoding() {
		let key = PrimitiveRetentionPolicyKey {
			primitive: PrimitiveId::Table(TableId(42)),
		};

		let encoded = key.encode();
		assert_eq!(encoded[0], 0xFE); // version (1 encoded as !1)
		assert_eq!(encoded[1], 0xE8); // kind (0x17 encoded as !0x17)
		assert_eq!(&encoded[3..11], &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD5]);

		let decoded = PrimitiveRetentionPolicyKey::decode(&encoded).unwrap();
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
