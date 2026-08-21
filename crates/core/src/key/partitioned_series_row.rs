// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::value::partition::Partition;

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::{object::ObjectId, storage::StorageId},
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedSeriesRowKey {
	pub storage: StorageId,
	pub partition: Partition,
	pub variant_tag: Option<u8>,
	pub key: u64,
	pub sequence: u64,
}

impl PartitionedSeriesRowKey {
	pub fn new(
		storage: impl Into<StorageId>,
		partition: Partition,
		variant_tag: Option<u8>,
		key: u64,
		sequence: u64,
	) -> Self {
		Self {
			storage: storage.into(),
			partition,
			variant_tag,
			key,
			sequence,
		}
	}

	pub fn encoded(
		storage: impl Into<StorageId>,
		partition: Partition,
		variant_tag: Option<u8>,
		key: u64,
		sequence: u64,
	) -> EncodedKey {
		Self::new(storage, partition, variant_tag, key, sequence).encode()
	}

	pub fn storage_of(key: &EncodedKey) -> Option<StorageId> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}
		StorageId::from_object(de.read_object_id().ok()?)
	}
}

impl EncodableKey for PartitionedSeriesRowKey {
	const KIND: KeyKind = KeyKind::PartitionedSeriesRow;

	fn encode(&self) -> EncodedKey {
		let capacity = if self.variant_tag.is_some() {
			44
		} else {
			43
		};
		let mut serializer = KeySerializer::with_capacity(capacity);
		serializer
			.extend_u8(Self::KIND as u8)
			.extend_object_id(ObjectId::from(self.storage))
			.extend_u128(self.partition.0);
		match self.variant_tag {
			Some(tag) => {
				serializer.extend_u8(1u8).extend_u8(tag);
			}
			None => {
				serializer.extend_u8(0u8);
			}
		}
		serializer.extend_u64(self.key).extend_u64(self.sequence);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let storage = StorageId::from_object(de.read_object_id().ok()?)?;
		let partition = Partition(de.read_u128().ok()?);

		let variant_tag = match de.read_u8().ok()? {
			1 => Some(de.read_u8().ok()?),
			0 => None,
			_ => return None,
		};

		let key = de.read_u64().ok()?;
		let sequence = de.read_u64().ok()?;

		Some(Self {
			storage,
			partition,
			variant_tag,
			key,
			sequence,
		})
	}
}

#[derive(Debug, Clone)]
pub struct PartitionedSeriesRowKeyRange {
	pub storage: StorageId,
	pub partition: Partition,
	pub variant_tag: Option<u8>,
	pub key_start: Option<u64>,
	pub key_end: Option<u64>,
}

impl PartitionedSeriesRowKeyRange {
	pub fn full_scan(storage: impl Into<StorageId>) -> EncodedKeyRange {
		let object = ObjectId::from(storage.into());
		let mut start = KeySerializer::with_capacity(10);
		start.extend_u8(PartitionedSeriesRowKey::KIND as u8).extend_object_id(object);
		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(PartitionedSeriesRowKey::KIND as u8).extend_object_id(object.prev());
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}

	pub fn partition_range(storage: impl Into<StorageId>, partition: Partition) -> EncodedKeyRange {
		EncodedKeyRange::prefix(Self::partition_prefix(storage.into(), partition).as_slice())
	}

	pub fn partition_scan_range(
		storage: impl Into<StorageId>,
		partition: Partition,
		last_key: Option<&EncodedKey>,
	) -> EncodedKeyRange {
		let base = Self::partition_range(storage, partition);
		match last_key {
			Some(last) => EncodedKeyRange::new(Bound::Excluded(last.clone()), base.end),
			None => base,
		}
	}

	pub fn scan_range(
		storage: impl Into<StorageId>,
		partition: Partition,
		variant_tag: Option<u8>,
		key_start: Option<u64>,
		key_end: Option<u64>,
		last_key: Option<&EncodedKey>,
	) -> EncodedKeyRange {
		if matches!(key_end, Some(0)) {
			let empty = EncodedKey::new(Vec::<u8>::new());
			return EncodedKeyRange::new(Bound::Excluded(empty.clone()), Bound::Excluded(empty));
		}

		let range = PartitionedSeriesRowKeyRange {
			storage: storage.into(),
			partition,
			variant_tag,
			key_start,
			key_end,
		};

		let start = if let Some(last_key) = last_key {
			Bound::Excluded(last_key.clone())
		} else {
			Bound::Included(range.start_key())
		};

		EncodedKeyRange::new(start, range.end_bound())
	}

	pub fn decode_storage(key: &EncodedKey) -> Option<StorageId> {
		PartitionedSeriesRowKey::storage_of(key)
	}

	pub fn decode(range: &EncodedKeyRange) -> (Option<StorageId>, Option<StorageId>) {
		let start = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_storage(key),
			Bound::Unbounded => None,
		};

		let end = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_storage(key),
			Bound::Unbounded => None,
		};

		(start, end)
	}

	fn partition_prefix(storage: StorageId, partition: Partition) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(26);
		serializer
			.extend_u8(PartitionedSeriesRowKey::KIND as u8)
			.extend_object_id(ObjectId::from(storage))
			.extend_u128(partition.0);
		serializer.to_encoded_key()
	}

	fn start_key(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(43);
		serializer
			.extend_u8(PartitionedSeriesRowKey::KIND as u8)
			.extend_object_id(ObjectId::from(self.storage))
			.extend_u128(self.partition.0);
		match self.variant_tag {
			Some(tag) => {
				serializer.extend_u8(1u8).extend_u8(tag);
			}
			None if self.key_start.is_some() || self.key_end.is_some() => {
				serializer.extend_u8(0u8);
			}
			None => {}
		}

		if let Some(key_val) = self.key_end {
			serializer.extend_u64(key_val - 1);
		}
		serializer.to_encoded_key()
	}

	fn end_bound(&self) -> Bound<EncodedKey> {
		match self.key_start {
			Some(key_val) => {
				let mut serializer = KeySerializer::with_capacity(43);
				serializer
					.extend_u8(PartitionedSeriesRowKey::KIND as u8)
					.extend_object_id(ObjectId::from(self.storage))
					.extend_u128(self.partition.0);
				match self.variant_tag {
					Some(tag) => {
						serializer.extend_u8(1u8).extend_u8(tag);
					}
					None => {
						serializer.extend_u8(0u8);
					}
				}

				serializer.extend_u64(key_val).extend_u64(0u64);
				Bound::Included(serializer.to_encoded_key())
			}
			None => EncodedKeyRange::prefix(
				Self::partition_prefix(self.storage, self.partition).as_slice(),
			)
			.end,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use reifydb_value::value::{Value, partition::Partition};

	use super::*;
	use crate::{
		interface::catalog::id::{SeriesId, TableId, ViewId},
		key::partitioned_row::{PartitionedRowKey, RowLocator},
	};

	fn part(v: &str) -> Partition {
		Partition::of(&[Value::Utf8(v.to_string())])
	}

	#[test]
	fn test_round_trip_without_tag() {
		// Without the flag byte an untagged key shifts every later field by one and reads back wrong.
		let key = PartitionedSeriesRowKey {
			storage: StorageId::Series(SeriesId(3)),
			partition: part("btc"),
			variant_tag: None,
			key: 1_700_000_000,
			sequence: 9,
		};
		let decoded = PartitionedSeriesRowKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_round_trip_with_tag() {
		// The partition sits between the object id and the tag, so a mis-sized partition eats the tag.
		let key = PartitionedSeriesRowKey {
			storage: StorageId::Series(SeriesId(3)),
			partition: part("eth"),
			variant_tag: Some(5),
			key: 42,
			sequence: 0,
		};
		let decoded = PartitionedSeriesRowKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_a_view_storage_round_trips() {
		// A partitioned series materialised into a view must keep its view tag, never narrow to a series.
		let key = PartitionedSeriesRowKey {
			storage: StorageId::View(ViewId(11)),
			partition: part("us"),
			variant_tag: Some(1),
			key: 77,
			sequence: 4,
		};
		let decoded = PartitionedSeriesRowKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_storage_of() {
		// Range classification reads the storage without decoding the whole key; a wrong offset misroutes it.
		let key = PartitionedSeriesRowKey::encoded(StorageId::Series(SeriesId(42)), part("us"), None, 1, 0);
		assert_eq!(PartitionedSeriesRowKey::storage_of(&key), Some(StorageId::Series(SeriesId(42))));
	}

	#[test]
	fn test_ordering_by_key_is_descending() {
		// Reads walk newest first; an ascending key encoding would hand back the oldest rows instead.
		let storage = StorageId::Series(SeriesId(1));
		let low = PartitionedSeriesRowKey::encoded(storage, part("us"), None, 100, 0);
		let high = PartitionedSeriesRowKey::encoded(storage, part("us"), None, 200, 0);

		assert!(low > high, "key descending ordering not preserved");
	}

	#[test]
	fn test_untagged_full_scan_covers_tagged_rows() {
		// A flag byte in the range bounds would pin the scan to flag=0 and silently drop every tagged row.
		let storage = StorageId::Series(SeriesId(9));
		let range = PartitionedSeriesRowKeyRange::full_scan(storage);
		let untagged = PartitionedSeriesRowKey::encoded(storage, part("us"), None, 500, 0);
		let tagged = PartitionedSeriesRowKey::encoded(storage, part("us"), Some(4), 500, 0);

		assert!(range.contains(&untagged));
		assert!(range.contains(&tagged), "an untagged full scan must still see tagged rows");
	}

	#[test]
	fn test_scan_range_brackets_the_rows_it_selects() {
		// The range must contain a key inside the window and exclude ones outside, or eviction skips live rows.
		let storage = StorageId::Series(SeriesId(1));
		let partition = part("us");
		let range = PartitionedSeriesRowKeyRange::scan_range(
			storage,
			partition,
			None,
			Some(100),
			Some(200),
			None,
		);
		let inside = PartitionedSeriesRowKey::encoded(storage, partition, None, 150, 1);
		let below = PartitionedSeriesRowKey::encoded(storage, partition, None, 99, 1);
		let above = PartitionedSeriesRowKey::encoded(storage, partition, None, 201, 1);

		assert!(range.contains(&inside));
		assert!(!range.contains(&below));
		assert!(!range.contains(&above));
	}

	#[test]
	fn test_scan_range_never_crosses_into_another_partition() {
		// Bounding only the key span would let a neighbouring partition's rows be evicted with this one.
		let storage = StorageId::Series(SeriesId(1));
		let range =
			PartitionedSeriesRowKeyRange::scan_range(storage, part("us"), None, Some(100), Some(200), None);
		let other = PartitionedSeriesRowKey::encoded(storage, part("eu"), None, 150, 1);

		assert!(!range.contains(&other), "an in-bounds key of another partition must stay outside");
	}

	#[test]
	fn test_partition_range_covers_every_key_of_its_partition() {
		// The prefix range is the eviction unit for a partition, so it must not depend on the tag.
		let storage = StorageId::Series(SeriesId(1));
		let range = PartitionedSeriesRowKeyRange::partition_range(storage, part("us"));
		let untagged = PartitionedSeriesRowKey::encoded(storage, part("us"), None, 1, 0);
		let tagged = PartitionedSeriesRowKey::encoded(storage, part("us"), Some(9), u64::MAX, u64::MAX);
		let other = PartitionedSeriesRowKey::encoded(storage, part("eu"), None, 1, 0);

		assert!(range.contains(&untagged));
		assert!(range.contains(&tagged));
		assert!(!range.contains(&other));
	}

	#[test]
	fn test_partition_scan_range_resumes_after_the_cursor() {
		// A resumed page must exclude the cursor itself, otherwise the last row of a page repeats forever.
		let storage = StorageId::Series(SeriesId(1));
		let partition = part("us");
		let cursor = PartitionedSeriesRowKey::encoded(storage, partition, None, 200, 0);
		let range = PartitionedSeriesRowKeyRange::partition_scan_range(storage, partition, Some(&cursor));
		let next = PartitionedSeriesRowKey::encoded(storage, partition, None, 100, 0);

		assert!(!range.contains(&cursor));
		assert!(range.contains(&next));
	}

	#[test]
	fn test_the_two_partitioned_kinds_do_not_share_a_keyspace() {
		// One kind byte for two layouts is what let a series key answer to a plain partitioned row read.
		let series = PartitionedSeriesRowKey::encoded(StorageId::Series(SeriesId(1)), part("us"), Some(2), 7, 9);
		let row = PartitionedRowKey::encoded(
			StorageId::Table(TableId(1)),
			part("us"),
			RowLocator::Series {
				variant_tag: Some(2),
				key: 7,
				sequence: 9,
			},
		);

		assert_ne!(series.as_slice()[0], row.as_slice()[0]);
		assert!(PartitionedRowKey::decode(&series).is_none());
		assert!(PartitionedSeriesRowKey::decode(&row).is_none());
	}

	#[test]
	fn test_half_bounded_untagged_range_excludes_tagged_rows() {
		// Same tag-class pinning as the unpartitioned range: the flag byte must appear in the start bound
		// whenever either key bound is set, or tagged rows (flag 0xFE) sort inside an untagged window (0xFF).
		let range = PartitionedSeriesRowKeyRange::scan_range(
			StorageId::Series(SeriesId(7)),
			part("us"),
			None,
			Some(100),
			None,
			None,
		);

		let untagged = PartitionedSeriesRowKey {
			storage: StorageId::Series(SeriesId(7)),
			partition: part("us"),
			variant_tag: None,
			key: 500,
			sequence: 1,
		}
		.encode();
		let tagged = PartitionedSeriesRowKey {
			storage: StorageId::Series(SeriesId(7)),
			partition: part("us"),
			variant_tag: Some(3),
			key: 500,
			sequence: 1,
		}
		.encode();

		assert!(range.contains(&untagged), "an untagged row above the lower bound must stay in range");
		assert!(!range.contains(&tagged), "a tagged row must never leak into an untagged key-bounded range");
	}
}
