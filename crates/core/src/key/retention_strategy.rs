// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::value::dictionary::DictionaryId;
use serde::{Deserialize, Serialize};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::{
	flow::FlowNodeId,
	id::{RingBufferId, SegmentTreeId, SeriesId, TableId, ViewId},
	shape::ShapeId,
	vtable::VTableId,
};

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
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8);

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
			ShapeId::SegmentTree(id) => {
				serializer.extend_u8(0x08).extend_u64(id.0);
			}
		}

		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

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
			0x08 => ShapeId::SegmentTree(SegmentTreeId(id)),
			_ => return None,
		};

		Some(Self {
			shape: object_id,
		})
	}
}

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
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.operator);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		Some(Self {
			operator: FlowNodeId(de.read_u64().ok()?),
		})
	}
}

pub struct ShapeRetentionStrategyKeyRange;

impl ShapeRetentionStrategyKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(ShapeRetentionStrategyKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(ShapeRetentionStrategyKey::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

pub struct OperatorRetentionStrategyKeyRange;

impl OperatorRetentionStrategyKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(OperatorRetentionStrategyKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(OperatorRetentionStrategyKey::KIND as u8 - 1);
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
		assert_eq!(encoded[0], 0xE8);

		assert_eq!(encoded.len(), 3);
		assert_eq!(encoded[1], 0xFE);
		assert_eq!(encoded[2], 0xD5);

		let decoded = ShapeRetentionStrategyKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_operator_retention_strategy_key_encoding() {
		let key = OperatorRetentionStrategyKey {
			operator: FlowNodeId(12345),
		};

		let encoded = key.encode();
		assert_eq!(encoded[0], 0xE7);

		assert_eq!(encoded.len(), 3);
		assert_eq!(encoded[1], 0x4F);
		assert_eq!(encoded[2], 0xC6);

		let decoded = OperatorRetentionStrategyKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}
}
