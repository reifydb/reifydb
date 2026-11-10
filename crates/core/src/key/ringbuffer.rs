// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::KeyCodec;
use reifydb_value::value::partition::Partition;

use super::KeyTag;
use crate::{
	interface::catalog::{
		id::{RingBufferId, SeriesId, TableId, ViewId},
		object::ObjectId,
		storage::StorageId,
	},
	key::{
		any::{Field, KeyFields, RawEncoding, Width},
		bound::{TaggedKeyBoundRange, object_fields},
		catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
	},
};

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = RingBuffer)]
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

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = RingBufferMetadata)]
pub struct RingBufferMetadataKey {
	pub storage: StorageId,
	pub encoded_partition: Vec<u8>,
}

impl RingBufferMetadataKey {
	pub fn new(storage: impl Into<StorageId>) -> Self {
		Self {
			storage: storage.into(),
			encoded_partition: Vec::new(),
		}
	}

	pub fn encoded(storage: impl Into<StorageId>) -> EncodedKey {
		Self::new(storage).encode()
	}

	pub fn of(storage: impl Into<StorageId>, partition: Option<Partition>) -> Self {
		Self {
			storage: storage.into(),
			encoded_partition: partition
				.map(|partition| partition.0.to_be_bytes().to_vec())
				.unwrap_or_default(),
		}
	}

	pub fn encoded_of(storage: impl Into<StorageId>, partition: Option<Partition>) -> EncodedKey {
		Self::of(storage, partition).encode()
	}

	pub fn partition(&self) -> Option<Option<Partition>> {
		match self.encoded_partition.len() {
			0 => Some(None),
			16 => Some(Some(Partition(u128::from_be_bytes(
				self.encoded_partition.as_slice().try_into().ok()?,
			)))),
			_ => None,
		}
	}

	pub fn full_scan_for_storage(storage: impl Into<StorageId>) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, object_fields(ObjectId::from(storage.into())))
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = RingBufferEviction)]
pub struct RingBufferEvictionKey {
	pub ringbuffer: RingBufferId,
}

impl RingBufferEvictionKey {
	pub fn new(ringbuffer: RingBufferId) -> Self {
		Self {
			ringbuffer,
		}
	}

	pub fn encoded(ringbuffer: impl Into<RingBufferId>) -> EncodedKey {
		Self::new(ringbuffer.into()).encode()
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = ViewEviction)]
pub struct ViewEvictionKey {
	pub view: ViewId,
}

impl ViewEvictionKey {
	pub fn new(view: ViewId) -> Self {
		Self {
			view,
		}
	}

	pub fn encoded(view: impl Into<ViewId>) -> EncodedKey {
		Self::new(view.into()).encode()
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = TableEviction)]
pub struct TableEvictionKey {
	pub table: TableId,
}

impl TableEvictionKey {
	pub fn new(table: TableId) -> Self {
		Self {
			table,
		}
	}

	pub fn encoded(table: impl Into<TableId>) -> EncodedKey {
		Self::new(table.into()).encode()
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = SeriesEviction)]
pub struct SeriesEvictionKey {
	pub series: SeriesId,
}

impl SeriesEvictionKey {
	pub fn new(series: SeriesId) -> Self {
		Self {
			series,
		}
	}

	pub fn encoded(series: impl Into<SeriesId>) -> EncodedKey {
		Self::new(series.into()).encode()
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use reifydb_codec::key::serializer::KeySerializer;

	use super::*;
	use crate::interface::catalog::id::ViewId;

	#[test]
	fn test_metadata_key_carries_the_partition_hash_and_an_unpartitioned_key_none() {
		// Lookups read metadata by hash alone; a key that lost the hash would alias every partition.
		let key = RingBufferMetadataKey::of(RingBufferId(42), Some(Partition(0xdead_beef)));
		let decoded = RingBufferMetadataKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded.partition(), Some(Some(Partition(0xdead_beef))));
		assert_eq!(RingBufferMetadataKey::new(RingBufferId(42)).partition(), Some(None));
		assert_eq!(
			RingBufferMetadataKey::of(RingBufferId(42), None),
			RingBufferMetadataKey::new(RingBufferId(42))
		);
	}

	#[test]
	fn test_metadata_key_roundtrip_ringbuffer() {
		// The tag byte is what keeps a ring buffer's metadata out of a view's; a bare id would collide.
		let key = RingBufferMetadataKey::of(StorageId::RingBuffer(RingBufferId(42)), Some(Partition(7)));
		assert_eq!(RingBufferMetadataKey::decode(&key.encode()).unwrap(), key);
	}

	#[test]
	fn test_metadata_key_roundtrip_view() {
		// A ring-buffer-backed view keeps its own metadata under its own id, not a backing object's.
		let key = RingBufferMetadataKey::of(StorageId::View(ViewId(42)), Some(Partition(7)));
		assert_eq!(RingBufferMetadataKey::decode(&key.encode()).unwrap(), key);
	}

	#[test]
	fn test_full_scan_for_storage_excludes_a_view_with_the_same_id() {
		// Ring buffer 42 and view 42 share a numeric id, so only the tag byte separates their scans.
		let range = RingBufferMetadataKey::full_scan_for_storage(RingBufferId(42)).encode();
		let ringbuffer = RingBufferMetadataKey::encoded(RingBufferId(42));
		let view = RingBufferMetadataKey::encoded(ViewId(42));
		assert!(range.contains(&ringbuffer));
		assert!(!range.contains(&view));
	}

	#[test]
	fn test_ring_buffer_key_matches_legacy_byte_layout() {
		for id in [RingBufferId(0), RingBufferId(1), RingBufferId(u64::MAX)] {
			let mut legacy = KeySerializer::with_capacity(9);
			legacy.extend_u8(KeyTag::RingBuffer as u8).extend_u64(id);
			assert_eq!(legacy.to_encoded_key().as_slice(), RingBufferKey::encoded(id).as_slice());
		}
	}
}
