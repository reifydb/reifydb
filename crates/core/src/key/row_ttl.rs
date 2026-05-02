// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::dictionary::DictionaryId;
use serde::{Deserialize, Serialize};

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::{
		id::{RingBufferId, SeriesId, TableId, ViewId},
		shape::ShapeId,
		vtable::VTableId,
	},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

/// Key for storing TTL configuration for a data shape (table, ringbuffer, series)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowTtlKey {
	pub shape: ShapeId,
}

impl RowTtlKey {
	pub fn encoded(shape: impl Into<ShapeId>) -> EncodedKey {
		Self {
			shape: shape.into(),
		}
		.encode()
	}
}

impl EncodableKey for RowTtlKey {
	const KIND: KeyKind = KeyKind::RowTtl;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(11);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8);

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

		let shape = match discriminator {
			0x01 => ShapeId::Table(TableId(id)),
			0x02 => ShapeId::View(ViewId(id)),
			0x03 => ShapeId::TableVirtual(VTableId(id)),
			0x04 => ShapeId::RingBuffer(RingBufferId(id)),
			0x06 => ShapeId::Dictionary(DictionaryId(id)),
			0x07 => ShapeId::Series(SeriesId(id)),
			_ => return None,
		};

		Some(Self {
			shape,
		})
	}
}

/// Range for scanning all shape TTL configurations
pub struct RowTtlKeyRange;

impl RowTtlKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(RowTtlKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(RowTtlKey::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_row_ttl_key_encoding() {
		let key = RowTtlKey {
			shape: ShapeId::Table(TableId(42)),
		};

		let encoded = key.encode();
		let decoded = RowTtlKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_row_ttl_key_roundtrip_ringbuffer() {
		let key = RowTtlKey {
			shape: ShapeId::RingBuffer(RingBufferId(99)),
		};

		let encoded = key.encode();
		let decoded = RowTtlKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_row_ttl_key_roundtrip_series() {
		let key = RowTtlKey {
			shape: ShapeId::Series(SeriesId(7)),
		};

		let encoded = key.encode();
		let decoded = RowTtlKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}
}
