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
		shape::ShapeId,
		vtable::VTableId,
	},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

/// Key for storing retention strategy for a data shape (table, view, ringbuffer)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShapeRetentionStrategyKey {
	pub shape: ShapeId,
}

impl ShapeRetentionStrategyKey {
	pub fn encoded(shape: impl Into<ShapeId>) -> EncodedKey {
		Self {
			shape: shape.into(),
		}
		.encode()
	}
}

impl EncodableKey for ShapeRetentionStrategyKey {
	const KIND: KeyKind = KeyKind::ShapeRetentionStrategy;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);

		// Encode object_id with discriminator
		match &self.shape {
			ShapeId::Table(id) => {
				serializer.extend_u8(0x01).extend_u64(id.0);
			}
			ShapeId::View(id) => {
				serializer.extend_u8(0x02).extend_u64(id.0);
			}
			ShapeId::TableVirtual(id) => {
				serializer.extend_u8(0x03).extend_u64(id.0);
			}
			ShapeId::RingBuffer(id) => {
				serializer.extend_u8(0x04).extend_u64(id.0);
			}
			ShapeId::Dictionary(id) => {
				serializer.extend_u8(0x06).extend_u64(id.0);
			}
			ShapeId::Series(id) => {
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
			0x01 => ShapeId::Table(TableId(id)),
			0x02 => ShapeId::View(ViewId(id)),
			0x03 => ShapeId::TableVirtual(VTableId(id)),
			0x04 => ShapeId::RingBuffer(RingBufferId(id)),
			0x06 => ShapeId::Dictionary(DictionaryId(id)),
			0x07 => ShapeId::Series(SeriesId(id)),
			_ => return None,
		};

		Some(Self {
			shape: object_id,
		})
	}
}

/// Key for storing retention strategy for a flow operator
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorRetentionStrategyKey {
	pub operator: FlowNodeId,
}

impl OperatorRetentionStrategyKey {
	pub fn encoded(operator: impl Into<FlowNodeId>) -> EncodedKey {
		Self {
			operator: operator.into(),
		}
		.encode()
	}
}

impl EncodableKey for OperatorRetentionStrategyKey {
	const KIND: KeyKind = KeyKind::OperatorRetentionStrategy;

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

/// Range for scanning all shape retention policies
pub struct ShapeRetentionStrategyKeyRange;

impl ShapeRetentionStrategyKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(ShapeRetentionStrategyKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(ShapeRetentionStrategyKey::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

/// Range for scanning all operator retention policies
pub struct OperatorRetentionStrategyKeyRange;

impl OperatorRetentionStrategyKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(OperatorRetentionStrategyKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(OperatorRetentionStrategyKey::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_shape_retention_strategy_key_encoding() {
		let key = ShapeRetentionStrategyKey {
			shape: ShapeId::Table(TableId(42)),
		};

		let encoded = key.encode();
		assert_eq!(encoded[0], 0xFE); // version (1 encoded as !1)
		assert_eq!(encoded[1], 0xE8); // kind (0x17 encoded as !0x17)
		assert_eq!(&encoded[3..11], &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD5]);

		let decoded = ShapeRetentionStrategyKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_operator_retention_strategy_key_encoding() {
		let key = OperatorRetentionStrategyKey {
			operator: FlowNodeId(12345),
		};

		let encoded = key.encode();
		assert_eq!(encoded[0], 0xFE); // version (1 encoded as !1)
		assert_eq!(encoded[1], 0xE7); // kind (0x18 encoded as !0x18)
		assert_eq!(&encoded[2..10], &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xCF, 0xC6]);

		let decoded = OperatorRetentionStrategyKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}
}
