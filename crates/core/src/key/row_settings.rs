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
	id::{RingBufferId, SegmentTreeId, SeriesId, TableId, ViewId},
	shape::ShapeId,
	vtable::VTableId,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowSettingsKey {
	pub shape: ShapeId,
}

impl RowSettingsKey {
	pub fn encoded(shape: impl Into<ShapeId>) -> EncodedKey {
		Self {
			shape: shape.into(),
		}
		.encode()
	}
}

impl EncodableKey for RowSettingsKey {
	const KIND: KeyKind = KeyKind::RowSettings;

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

		let shape = match discriminator {
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
			shape,
		})
	}
}

pub struct RowSettingsKeyRange;

impl RowSettingsKeyRange {
	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(RowSettingsKey::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(RowSettingsKey::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_row_settings_key_encoding() {
		let key = RowSettingsKey {
			shape: ShapeId::Table(TableId(42)),
		};

		let encoded = key.encode();
		let decoded = RowSettingsKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_row_settings_key_roundtrip_ringbuffer() {
		let key = RowSettingsKey {
			shape: ShapeId::RingBuffer(RingBufferId(99)),
		};

		let encoded = key.encode();
		let decoded = RowSettingsKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_row_settings_key_roundtrip_series() {
		let key = RowSettingsKey {
			shape: ShapeId::Series(SeriesId(7)),
		};

		let encoded = key.encode();
		let decoded = RowSettingsKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}
}
