// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::value::Value;

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::{id::RingBufferId, object::ObjectId, storage::StorageId},
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
pub struct RingBufferKey {
	pub ringbuffer: RingBufferId,
}

impl RingBufferKey {
	pub fn new(ringbuffer: RingBufferId) -> Self {
		Self {
			ringbuffer,
		}
	}

	pub fn encoded(ringbuffer: impl Into<RingBufferId>) -> EncodedKey {
		Self::new(ringbuffer.into()).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::ringbuffer_start()), Some(Self::ringbuffer_end()))
	}

	fn ringbuffer_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn ringbuffer_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for RingBufferKey {
	const KIND: KeyKind = KeyKind::RingBuffer;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.ringbuffer);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let ringbuffer = de.read_u64().ok()?;

		Some(Self {
			ringbuffer: RingBufferId(ringbuffer),
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct RingBufferMetadataKey {
	pub storage: StorageId,
	pub partition_values: Vec<Value>,
}

impl RingBufferMetadataKey {
	pub fn new(storage: impl Into<StorageId>) -> Self {
		Self {
			storage: storage.into(),
			partition_values: vec![],
		}
	}

	pub fn encoded(storage: impl Into<StorageId>) -> EncodedKey {
		Self::new(storage).encode()
	}

	pub fn encoded_partition(storage: impl Into<StorageId>, partition_values: Vec<Value>) -> EncodedKey {
		Self {
			storage: storage.into(),
			partition_values,
		}
		.encode()
	}

	pub fn full_scan_for_storage(storage: impl Into<StorageId>) -> EncodedKeyRange {
		let storage = ObjectId::from(storage.into());

		let mut start = KeySerializer::with_capacity(10);
		start.extend_u8(Self::KIND as u8).extend_object_id(storage);
		let start_key = start.to_encoded_key();

		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(Self::KIND as u8).extend_object_id(storage.prev());
		let end_key = end.to_encoded_key();

		EncodedKeyRange::start_end(Some(start_key), Some(end_key))
	}
}

impl EncodableKey for RingBufferMetadataKey {
	const KIND: KeyKind = KeyKind::RingBufferMetadata;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.storage);
		for value in &self.partition_values {
			serializer.extend_value(value);
		}
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let storage = StorageId::from_object(de.read_object_id().ok()?)?;

		let mut partition_values = Vec::new();
		while !de.is_empty() {
			partition_values.push(de.read_value().ok()?);
		}

		Some(Self {
			storage,
			partition_values,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use super::*;
	use crate::interface::catalog::id::ViewId;

	#[test]
	fn test_metadata_key_encode_decode_roundtrip() {
		let key = RingBufferMetadataKey::encoded_partition(
			RingBufferId(42),
			vec![Value::Utf8("east".to_string())],
		);
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let _ = (de.read_u8(), de.read_object_id());
		let value = de.read_value().unwrap();
		assert_eq!(value, Value::Utf8("east".to_string()));
	}

	#[test]
	fn test_metadata_key_encode_decode_multiple() {
		let key = RingBufferMetadataKey::encoded_partition(
			RingBufferId(7),
			vec![Value::Utf8("us".to_string()), Value::Uint8(42)],
		);
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let _ = (de.read_u8(), de.read_object_id());
		assert_eq!(de.read_value().unwrap(), Value::Utf8("us".to_string()));
		assert_eq!(de.read_value().unwrap(), Value::Uint8(42));
	}

	#[test]
	fn test_metadata_key_roundtrip_ringbuffer() {
		// The tag byte is what keeps a ring buffer's metadata out of a view's; a bare id would collide.
		let key = RingBufferMetadataKey {
			storage: StorageId::RingBuffer(RingBufferId(42)),
			partition_values: vec![Value::Utf8("east".to_string())],
		};
		assert_eq!(RingBufferMetadataKey::decode(&key.encode()).unwrap(), key);
	}

	#[test]
	fn test_metadata_key_roundtrip_view() {
		// A ring-buffer-backed view keeps its own metadata under its own id, not a backing object's.
		let key = RingBufferMetadataKey {
			storage: StorageId::View(ViewId(42)),
			partition_values: vec![Value::Utf8("east".to_string())],
		};
		assert_eq!(RingBufferMetadataKey::decode(&key.encode()).unwrap(), key);
	}

	#[test]
	fn test_full_scan_for_storage_excludes_a_view_with_the_same_id() {
		// Ring buffer 42 and view 42 share a numeric id, so only the tag byte separates their scans.
		let range = RingBufferMetadataKey::full_scan_for_storage(RingBufferId(42));
		let ringbuffer = RingBufferMetadataKey::encoded(RingBufferId(42));
		let view = RingBufferMetadataKey::encoded(ViewId(42));
		assert!(range.contains(&ringbuffer));
		assert!(!range.contains(&view));
	}
}
