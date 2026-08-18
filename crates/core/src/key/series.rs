// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::{id::SeriesId, storage::StorageId},
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesKey {
	pub series: SeriesId,
}

impl SeriesKey {
	pub fn new(series: SeriesId) -> Self {
		Self {
			series,
		}
	}

	pub fn encoded(series: impl Into<SeriesId>) -> EncodedKey {
		Self::new(series.into()).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::series_start()), Some(Self::series_end()))
	}

	fn series_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn series_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for SeriesKey {
	const KIND: KeyKind = KeyKind::Series;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.series);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let series = de.read_u64().ok()?;

		Some(Self {
			series: SeriesId(series),
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesMetadataKey {
	pub storage: StorageId,
}

impl SeriesMetadataKey {
	pub fn new(storage: impl Into<StorageId>) -> Self {
		Self {
			storage: storage.into(),
		}
	}

	pub fn encoded(storage: impl Into<StorageId>) -> EncodedKey {
		Self::new(storage).encode()
	}
}

impl EncodableKey for SeriesMetadataKey {
	const KIND: KeyKind = KeyKind::SeriesMetadata;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.storage);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let storage = StorageId::from_object(de.read_object_id().ok()?)?;

		Some(Self {
			storage,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::{EncodableKey, SeriesMetadataKey};
	use crate::interface::catalog::{
		id::{SeriesId, ViewId},
		storage::StorageId,
	};

	#[test]
	fn test_metadata_key_roundtrip_series() {
		// The tag byte is what keeps a series' metadata out of a view's; a bare id would collide.
		let key = SeriesMetadataKey {
			storage: StorageId::Series(SeriesId(7)),
		};
		assert_eq!(SeriesMetadataKey::decode(&key.encode()).unwrap(), key);
	}

	#[test]
	fn test_metadata_key_roundtrip_view() {
		// A series-backed view must keep its metadata under its own id, never a backing object's.
		let key = SeriesMetadataKey {
			storage: StorageId::View(ViewId(7)),
		};
		assert_eq!(SeriesMetadataKey::decode(&key.encode()).unwrap(), key);
	}

	#[test]
	fn test_metadata_key_separates_a_series_from_a_view_with_the_same_id() {
		// Both narrow from the same numeric id, so identical bytes would silently share one row.
		assert_ne!(SeriesMetadataKey::encoded(SeriesId(7)), SeriesMetadataKey::encoded(ViewId(7)));
	}
}
