// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use serde::{Deserialize, Serialize};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::storage::StorageId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowSettingsKey {
	pub storage: StorageId,
}

impl RowSettingsKey {
	pub fn encoded(storage: StorageId) -> EncodedKey {
		Self {
			storage,
		}
		.encode()
	}
}

impl EncodableKey for RowSettingsKey {
	const KIND: KeyKind = KeyKind::RowSettings;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8);

		serializer.extend_u8(self.storage.type_tag()).extend_u64(self.storage.as_u64());

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

		Some(Self {
			storage: StorageId::from_type_tag(discriminator, id)?,
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
	use crate::interface::catalog::id::{RingBufferId, SeriesId, TableId, ViewId};

	#[test]
	fn test_row_settings_key_encoding() {
		let key = RowSettingsKey {
			storage: StorageId::Table(TableId(42)),
		};

		let encoded = key.encode();
		let decoded = RowSettingsKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_row_settings_key_roundtrip_view() {
		// A view owns its rows, so its settings key must survive the tag round trip like any other storage.
		let key = RowSettingsKey {
			storage: StorageId::View(ViewId(13)),
		};

		let encoded = key.encode();
		let decoded = RowSettingsKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_row_settings_key_roundtrip_ringbuffer() {
		let key = RowSettingsKey {
			storage: StorageId::RingBuffer(RingBufferId(99)),
		};

		let encoded = key.encode();
		let decoded = RowSettingsKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_row_settings_key_roundtrip_series() {
		let key = RowSettingsKey {
			storage: StorageId::Series(SeriesId(7)),
		};

		let encoded = key.encode();
		let decoded = RowSettingsKey::decode(&encoded).unwrap();
		assert_eq!(key, decoded);
	}
}
