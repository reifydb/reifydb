// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_macro::Key;
use reifydb_value::value::partition::Partition;

use super::KeyKind;
use crate::{
	interface::catalog::{id::SeriesId, object::ObjectId, storage::StorageId},
	key::{
		catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
		typed::{TypedKey, direction::Desc, key::Key},
	},
	metrics::heap::HeapSize,
};

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Series)]
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
		Key::encode(&Self::new(series.into()))
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::series_start()), Some(Self::series_end()))
	}

	fn series_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<Self as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn series_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<Self as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = SeriesMetadata)]
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
		Key::encode(&Self::new(storage))
	}
}

#[cfg(test)]
mod series_metadata_key_tests {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::{KeyKind, SeriesKey, SeriesMetadataKey};
	use crate::{
		interface::catalog::{
			id::{SeriesId, ViewId},
			storage::StorageId,
		},
		key::typed::key::Key,
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

	#[test]
	fn test_series_key_matches_legacy_byte_layout() {
		for id in [SeriesId(0), SeriesId(1), SeriesId(u64::MAX)] {
			let mut legacy = KeySerializer::with_capacity(9);
			legacy.extend_u8(KeyKind::Series as u8).extend_u64(id);
			assert_eq!(legacy.to_encoded_key().as_slice(), SeriesKey::encoded(id).as_slice());
		}
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = SeriesRow)]
pub struct SeriesRowKey {
	pub storage: StorageId,
	pub variant_tag: Option<u8>,
	pub key: u64,
	pub sequence: u64,
}

#[derive(Debug, Clone)]
pub struct SeriesRowKeyRange {
	pub storage: StorageId,
	pub variant_tag: Option<u8>,
	pub key_start: Option<u64>,
	pub key_end: Option<u64>,
}

impl SeriesRowKeyRange {
	pub fn full_scan(storage: StorageId, variant_tag: Option<u8>) -> EncodedKeyRange {
		let range = SeriesRowKeyRange {
			storage,
			variant_tag,
			key_start: None,
			key_end: None,
		};
		EncodedKeyRange::new(Bound::Included(range.start_key()), Bound::Included(range.end_key()))
	}

	pub fn scan_range(
		storage: StorageId,
		variant_tag: Option<u8>,
		key_start: Option<u64>,
		key_end: Option<u64>,
		last_key: Option<&EncodedKey>,
	) -> EncodedKeyRange {
		if matches!(key_end, Some(0)) {
			let empty = EncodedKey::new(Vec::<u8>::new());
			return EncodedKeyRange::new(Bound::Excluded(empty.clone()), Bound::Excluded(empty));
		}

		let range = SeriesRowKeyRange {
			storage,
			variant_tag,
			key_start,
			key_end,
		};

		let start = if let Some(last_key) = last_key {
			Bound::Excluded(last_key.clone())
		} else {
			Bound::Included(range.start_key())
		};

		EncodedKeyRange::new(start, Bound::Included(range.end_key()))
	}

	pub fn decode_storage(key: &EncodedKey) -> Option<StorageId> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != SeriesRowKey::KIND {
			return None;
		}

		StorageId::from_object(de.read_object_id().ok()?)
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

	fn start_key(&self) -> EncodedKey {
		let object = ObjectId::from(self.storage);
		let mut serializer = KeySerializer::with_capacity(27);
		serializer.extend_u8(SeriesRowKey::KIND as u8).extend_object_id(object);
		match self.variant_tag {
			Some(tag) => {
				serializer.extend_u8(1u8).extend_u8(tag);
			}
			None if self.key_start.is_some() || self.key_end.is_some() => {
				serializer.extend_u8(0u8).extend_u8(0u8);
			}
			None => {}
		}

		if let Some(key_val) = self.key_end {
			serializer.extend_u64(key_val - 1);
		}
		serializer.to_encoded_key()
	}

	fn end_key(&self) -> EncodedKey {
		if let Some(key_val) = self.key_start {
			let object = ObjectId::from(self.storage);
			let mut serializer = KeySerializer::with_capacity(27);
			serializer.extend_u8(SeriesRowKey::KIND as u8).extend_object_id(object);
			match self.variant_tag {
				Some(tag) => {
					serializer.extend_u8(1u8).extend_u8(tag);
				}
				None => {
					serializer.extend_u8(0u8).extend_u8(0u8);
				}
			}

			serializer.extend_u64(key_val).extend_u64(0u64);
			serializer.to_encoded_key()
		} else {
			let object = ObjectId::from(self.storage);
			let mut serializer = KeySerializer::with_capacity(10);
			serializer.extend_u8(SeriesRowKey::KIND as u8).extend_object_id(object.prev());
			serializer.to_encoded_key()
		}
	}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageSeriesKey {
	pub variant_tag: Desc<Option<u8>>,
	pub key: Desc<u64>,
	pub sequence: Desc<u64>,
}

impl StorageSeriesKey {
	pub fn new(variant_tag: Option<u8>, key: u64, sequence: u64) -> Self {
		Self {
			variant_tag: Desc(variant_tag),
			key: Desc(key),
			sequence: Desc(sequence),
		}
	}

	pub fn variant_tag(self) -> Option<u8> {
		self.variant_tag.0
	}

	pub fn key(self) -> u64 {
		self.key.0
	}

	pub fn sequence(self) -> u64 {
		self.sequence.0
	}

	pub fn with_storage(self, storage: StorageId) -> SeriesRowKey {
		SeriesRowKey {
			storage,
			variant_tag: self.variant_tag(),
			key: self.key(),
			sequence: self.sequence(),
		}
	}
}

impl From<SeriesRowKey> for StorageSeriesKey {
	fn from(key: SeriesRowKey) -> Self {
		StorageSeriesKey::new(key.variant_tag, key.key, key.sequence)
	}
}

impl HeapSize for StorageSeriesKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl TypedKey for StorageSeriesKey {
	fn low() -> Self {
		Self {
			variant_tag: lowest_variant_tag(),
			key: <Desc<u64> as TypedKey>::low(),
			sequence: <Desc<u64> as TypedKey>::low(),
		}
	}

	fn successor(&self) -> Option<Self> {
		if let Some(sequence) = self.sequence.successor() {
			return Some(Self {
				variant_tag: self.variant_tag,
				key: self.key,
				sequence,
			});
		}
		if let Some(key) = self.key.successor() {
			return Some(Self {
				variant_tag: self.variant_tag,
				key,
				sequence: <Desc<u64> as TypedKey>::low(),
			});
		}
		Some(Self {
			variant_tag: next_variant_tag(self.variant_tag)?,
			key: <Desc<u64> as TypedKey>::low(),
			sequence: <Desc<u64> as TypedKey>::low(),
		})
	}
}

fn lowest_variant_tag() -> Desc<Option<u8>> {
	Desc(Some(u8::MAX))
}

fn next_variant_tag(tag: Desc<Option<u8>>) -> Option<Desc<Option<u8>>> {
	match tag.0 {
		Some(0) => Some(Desc(None)),
		Some(value) => Some(Desc(Some(value - 1))),
		None => None,
	}
}

#[cfg(test)]
mod row_key_range_tests {
	use std::ops::RangeBounds;

	use super::*;
	use crate::{
		interface::catalog::id::{SeriesId, ViewId},
		key::{EncodableKeyRange, row::RowKeyRange},
	};

	#[test]
	fn test_encode_decode_without_tag() {
		// Without the flag byte a missing tag shifts every later field by one and the key reads back wrong.
		let key = SeriesRowKey {
			storage: StorageId::Series(SeriesId(42)),
			variant_tag: None,
			key: 1706745600000,
			sequence: 1,
		};
		let encoded = key.encode();
		let decoded = SeriesRowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.storage, StorageId::Series(SeriesId(42)));
		assert_eq!(decoded.variant_tag, None);
		assert_eq!(decoded.key, 1706745600000);
		assert_eq!(decoded.sequence, 1);
	}

	#[test]
	fn test_encode_decode_with_tag() {
		// A tagged key must report the exact tag it was written with, never a byte borrowed from the key.
		let key = SeriesRowKey {
			storage: StorageId::Series(SeriesId(42)),
			variant_tag: Some(3),
			key: 1706745600000,
			sequence: 5,
		};
		let encoded = key.encode();
		let decoded = SeriesRowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.storage, StorageId::Series(SeriesId(42)));
		assert_eq!(decoded.variant_tag, Some(3));
		assert_eq!(decoded.key, 1706745600000);
		assert_eq!(decoded.sequence, 5);
	}

	#[test]
	fn test_a_view_storage_round_trips() {
		// Widening off SeriesId is pointless unless a non-series storage keeps its own tag through decode.
		let key = SeriesRowKey {
			storage: StorageId::View(ViewId(42)),
			variant_tag: Some(7),
			key: 900,
			sequence: 2,
		};
		let decoded = SeriesRowKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_full_scan_range_still_names_its_storage() {
		// classify_range routes a series scan to its own physical entry; an undecodable range falls to Multi.
		let range = SeriesRowKeyRange::full_scan(StorageId::Series(SeriesId(42)), None);
		let (start, end) = SeriesRowKeyRange::decode(&range);
		assert_eq!(start, Some(StorageId::Series(SeriesId(42))));
		assert_eq!(
			end,
			Some(StorageId::Series(SeriesId(41))),
			"the exclusive end brackets the next series id down"
		);
	}

	#[test]
	fn test_untagged_full_scan_covers_tagged_rows() {
		// A flag byte in the range bounds would pin the scan to flag=0 and silently drop every tagged row.
		let storage = StorageId::Series(SeriesId(9));
		let range = SeriesRowKeyRange::full_scan(storage, None);
		let untagged = SeriesRowKey {
			storage,
			variant_tag: None,
			key: 500,
			sequence: 0,
		}
		.encode();
		let tagged = SeriesRowKey {
			storage,
			variant_tag: Some(4),
			key: 500,
			sequence: 0,
		}
		.encode();

		assert!(range.contains(&untagged));
		assert!(range.contains(&tagged), "an untagged full scan must still see tagged rows");
	}

	#[test]
	fn test_scan_range_brackets_the_rows_it_selects() {
		// The range must contain a key inside the window and exclude one outside, or eviction skips live rows.
		let storage = StorageId::Series(SeriesId(1));
		let range = SeriesRowKeyRange::scan_range(storage, None, Some(100), Some(200), None);
		let inside = SeriesRowKey {
			storage,
			variant_tag: None,
			key: 150,
			sequence: 1,
		}
		.encode();
		let below = SeriesRowKey {
			storage,
			variant_tag: None,
			key: 99,
			sequence: 1,
		}
		.encode();
		let above = SeriesRowKey {
			storage,
			variant_tag: None,
			key: 201,
			sequence: 1,
		}
		.encode();

		assert!(range.contains(&inside));
		assert!(!range.contains(&below));
		assert!(!range.contains(&above));
	}

	#[test]
	fn test_row_key_range_never_claims_a_series_range() {
		// A shared kind byte made a series scan classify as a plain row scan of the same object id.
		let range = SeriesRowKeyRange::full_scan(StorageId::Series(SeriesId(42)), None);
		assert_eq!(RowKeyRange::decode(&range), (None, None));
	}

	#[test]
	fn test_ordering_by_key() {
		// Reads walk newest first; an ascending key encoding would hand back the oldest rows instead.
		let key1 = SeriesRowKey {
			storage: StorageId::Series(SeriesId(1)),
			variant_tag: None,
			key: 100,
			sequence: 0,
		};
		let key2 = SeriesRowKey {
			storage: StorageId::Series(SeriesId(1)),
			variant_tag: None,
			key: 200,
			sequence: 0,
		};
		let e1 = key1.encode();
		let e2 = key2.encode();

		assert!(e1 > e2, "key descending ordering not preserved");
	}

	#[test]
	fn test_ordering_by_sequence() {
		// Two rows at the same key must still come back newest sequence first.
		let key1 = SeriesRowKey {
			storage: StorageId::Series(SeriesId(1)),
			variant_tag: None,
			key: 100,
			sequence: 1,
		};
		let key2 = SeriesRowKey {
			storage: StorageId::Series(SeriesId(1)),
			variant_tag: None,
			key: 100,
			sequence: 2,
		};
		let e1 = key1.encode();
		let e2 = key2.encode();

		assert!(e1 > e2, "sequence descending ordering not preserved");
	}

	#[test]
	fn test_half_bounded_untagged_range_excludes_tagged_rows() {
		// A range bounded on one side only must still pin its tag class: the untagged flag encodes 0xFF and
		// the tagged flag 0xFE, so omitting the flag from the start bound lets every tagged row of the series
		// sort into the window regardless of its key.
		let range = SeriesRowKeyRange::scan_range(StorageId::Series(SeriesId(7)), None, Some(100), None, None);

		let untagged = SeriesRowKey {
			storage: StorageId::Series(SeriesId(7)),
			variant_tag: None,
			key: 500,
			sequence: 1,
		}
		.encode();
		let tagged = SeriesRowKey {
			storage: StorageId::Series(SeriesId(7)),
			variant_tag: Some(3),
			key: 500,
			sequence: 1,
		}
		.encode();

		assert!(range.contains(&untagged), "an untagged row above the lower bound must stay in range");
		assert!(!range.contains(&tagged), "a tagged row must never leak into an untagged key-bounded range");
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = PartitionedSeriesRow)]
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

	pub fn full_scan_range(storage: impl Into<StorageId>, last_key: Option<&EncodedKey>) -> EncodedKeyRange {
		let base = Self::full_scan(storage);
		match last_key {
			Some(last) => EncodedKeyRange::new(Bound::Excluded(last.clone()), base.end),
			None => base,
		}
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
				serializer.extend_u8(0u8).extend_u8(0u8);
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
						serializer.extend_u8(0u8).extend_u8(0u8);
					}
				}

				serializer.extend_u64(key_val).extend_u64(0u64);
				Bound::Included(serializer.to_encoded_key())
			}
			None => {
				EncodedKeyRange::prefix(Self::partition_prefix(self.storage, self.partition).as_slice())
					.end
			}
		}
	}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoragePartitionedSeriesKey {
	pub partition: Desc<Partition>,
	pub variant_tag: Desc<Option<u8>>,
	pub key: Desc<u64>,
	pub sequence: Desc<u64>,
}

impl StoragePartitionedSeriesKey {
	pub fn new(partition: Partition, variant_tag: Option<u8>, key: u64, sequence: u64) -> Self {
		Self {
			partition: Desc(partition),
			variant_tag: Desc(variant_tag),
			key: Desc(key),
			sequence: Desc(sequence),
		}
	}

	pub fn partition(self) -> Partition {
		self.partition.0
	}

	pub fn variant_tag(self) -> Option<u8> {
		self.variant_tag.0
	}

	pub fn key(self) -> u64 {
		self.key.0
	}

	pub fn sequence(self) -> u64 {
		self.sequence.0
	}

	pub fn with_storage(self, storage: StorageId) -> PartitionedSeriesRowKey {
		PartitionedSeriesRowKey {
			storage,
			partition: self.partition(),
			variant_tag: self.variant_tag(),
			key: self.key(),
			sequence: self.sequence(),
		}
	}
}

impl From<PartitionedSeriesRowKey> for StoragePartitionedSeriesKey {
	fn from(key: PartitionedSeriesRowKey) -> Self {
		StoragePartitionedSeriesKey::new(key.partition, key.variant_tag, key.key, key.sequence)
	}
}

impl HeapSize for StoragePartitionedSeriesKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl TypedKey for StoragePartitionedSeriesKey {
	fn low() -> Self {
		Self {
			partition: <Desc<Partition> as TypedKey>::low(),
			variant_tag: lowest_variant_tag(),
			key: <Desc<u64> as TypedKey>::low(),
			sequence: <Desc<u64> as TypedKey>::low(),
		}
	}

	fn successor(&self) -> Option<Self> {
		if let Some(sequence) = self.sequence.successor() {
			return Some(Self {
				sequence,
				..*self
			});
		}
		if let Some(key) = self.key.successor() {
			return Some(Self {
				key,
				sequence: <Desc<u64> as TypedKey>::low(),
				..*self
			});
		}
		if let Some(variant_tag) = next_variant_tag(self.variant_tag) {
			return Some(Self {
				partition: self.partition,
				variant_tag,
				key: <Desc<u64> as TypedKey>::low(),
				sequence: <Desc<u64> as TypedKey>::low(),
			});
		}
		Some(Self {
			partition: self.partition.successor()?,
			variant_tag: lowest_variant_tag(),
			key: <Desc<u64> as TypedKey>::low(),
			sequence: <Desc<u64> as TypedKey>::low(),
		})
	}
}

#[cfg(test)]
mod partitioned_row_key_tests {
	use std::ops::RangeBounds;

	use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

	use super::*;
	use crate::{
		interface::catalog::id::{SeriesId, TableId, ViewId},
		key::row::PartitionedRowKey,
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
		let range =
			PartitionedSeriesRowKeyRange::scan_range(storage, partition, None, Some(100), Some(200), None);
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
	fn test_full_scan_range_resumes_across_partitions() {
		// An object-wide read pages through every partition at once, so the cursor must only cut the
		// rows already returned and never fence the scan into the cursor's own partition.
		let storage = StorageId::Series(SeriesId(1));
		let cursor = PartitionedSeriesRowKey::encoded(storage, part("us"), None, 200, 0);
		let range = PartitionedSeriesRowKeyRange::full_scan_range(storage, Some(&cursor));

		assert!(!range.contains(&cursor));
		assert!(range.contains(&PartitionedSeriesRowKey::encoded(storage, part("us"), None, 100, 0)));
		assert!(
			range.contains(&PartitionedSeriesRowKey::encoded(storage, part("eu"), None, 100, 0))
				|| range.contains(&PartitionedSeriesRowKey::encoded(storage, part("eu"), None, 300, 0)),
			"a resumed object-wide scan must still be able to reach another partition"
		);
	}

	#[test]
	fn test_full_scan_range_without_a_cursor_covers_every_partition() {
		// The unpaged form is the object-wide read the planner falls back to when no partition was
		// pruned; missing a partition there silently halves the answer.
		let storage = StorageId::Series(SeriesId(1));
		let range = PartitionedSeriesRowKeyRange::full_scan_range(storage, None);

		assert!(range.contains(&PartitionedSeriesRowKey::encoded(storage, part("us"), None, 1, 0)));
		assert!(range.contains(&PartitionedSeriesRowKey::encoded(storage, part("eu"), Some(3), 9, 9)));
		assert!(!range.contains(&PartitionedSeriesRowKey::encoded(
			StorageId::Series(SeriesId(2)),
			part("us"),
			None,
			1,
			0
		)));
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
		let series =
			PartitionedSeriesRowKey::encoded(StorageId::Series(SeriesId(1)), part("us"), Some(2), 7, 9);
		let row = PartitionedRowKey::encoded(StorageId::Table(TableId(1)), part("us"), RowNumber(9));

		assert_ne!(series.as_slice()[0], row.as_slice()[0]);
		assert!(<PartitionedRowKey as Key>::decode(&series).is_none());
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

#[cfg(test)]
mod storage_series_key_tests {
	use reifydb_value::value::partition::Partition;

	use super::{PartitionedSeriesRowKey, SeriesRowKey, StorageId, StoragePartitionedSeriesKey, StorageSeriesKey};
	use crate::key::typed::{TypedKey, key::Key};

	const STORAGE: StorageId = StorageId::Series(crate::interface::catalog::id::SeriesId(7));

	fn series(variant_tag: Option<u8>, key: u64, sequence: u64) -> StorageSeriesKey {
		StorageSeriesKey::new(variant_tag, key, sequence)
	}

	#[test]
	fn storage_key_order_matches_the_encoded_byte_order() {
		// the storage key is the cache key for the same rows the encoded key orders on disk, so a disagreement
		// here silently hands back a neighbouring row on any ordered lookup
		let mut keys = vec![
			series(None, 5, 1),
			series(Some(0), 5, 1),
			series(Some(9), 5, 1),
			series(Some(9), 5, 2),
			series(Some(9), 6, 1),
		];
		let mut encoded: Vec<_> =
			keys.iter().map(|it| Key::encode(&it.with_storage(STORAGE)).to_vec()).collect();
		keys.sort();
		encoded.sort();

		let reordered: Vec<_> = keys.iter().map(|it| Key::encode(&it.with_storage(STORAGE)).to_vec()).collect();
		assert_eq!(reordered, encoded);
	}

	#[test]
	fn a_tagged_row_sorts_before_an_untagged_one() {
		// none is written as a zero presence flag, which inverts to 0xff and therefore lands last; rust's
		// natural Option order puts none first, so Desc must be what reconciles the two
		assert!(series(Some(0), 1, 1) < series(None, 1, 1));
		assert!(series(Some(255), 1, 1) < series(Some(0), 1, 1));
	}

	#[test]
	fn low_is_the_first_key_the_encoder_can_produce() {
		let low = <StorageSeriesKey as TypedKey>::low();
		assert_eq!(low, series(Some(u8::MAX), u64::MAX, u64::MAX));
		for other in [series(None, 0, 0), series(Some(0), 0, 0), series(Some(200), 7, 3)] {
			assert!(low < other, "low must not sort above a real key");
		}
	}

	#[test]
	fn successor_walks_the_sequence_then_the_key_then_the_tag() {
		// the odometer must carry left, otherwise an exclusive upper end skips every row that sorts
		// between a key and the value successor hands back
		assert_eq!(series(Some(9), 5, 2).successor(), Some(series(Some(9), 5, 1)));
		assert_eq!(series(Some(9), 5, 0).successor(), Some(series(Some(9), 4, u64::MAX)));
		assert_eq!(series(Some(9), 0, 0).successor(), Some(series(Some(8), u64::MAX, u64::MAX)));
		assert_eq!(series(Some(0), 0, 0).successor(), Some(series(None, u64::MAX, u64::MAX)));
		assert_eq!(series(None, 0, 0).successor(), None);
	}

	#[test]
	fn every_successor_is_the_immediate_next_storage_key() {
		let mut walk = vec![series(Some(1), 1, 1)];
		for _ in 0..4 {
			walk.push(walk.last().unwrap().successor().unwrap());
		}
		assert!(walk.windows(2).all(|pair| pair[0] < pair[1]), "the walk must ascend: {walk:?}");
	}

	#[test]
	fn partitioned_storage_key_order_matches_the_encoded_byte_order() {
		let mut keys = vec![
			StoragePartitionedSeriesKey::new(Partition(2), None, 5, 1),
			StoragePartitionedSeriesKey::new(Partition(2), Some(3), 5, 1),
			StoragePartitionedSeriesKey::new(Partition(1), Some(3), 5, 1),
			StoragePartitionedSeriesKey::new(Partition(2), Some(3), 5, 2),
		];
		let mut encoded: Vec<_> =
			keys.iter().map(|it| Key::encode(&it.with_storage(STORAGE)).to_vec()).collect();
		keys.sort();
		encoded.sort();

		let reordered: Vec<_> = keys.iter().map(|it| Key::encode(&it.with_storage(STORAGE)).to_vec()).collect();
		assert_eq!(reordered, encoded);
	}

	#[test]
	fn partitioned_successor_carries_into_the_partition() {
		// the partition is the outermost column, so it may only step once every inner column is exhausted
		let last_of_partition = StoragePartitionedSeriesKey::new(Partition(5), None, 0, 0);
		assert_eq!(
			last_of_partition.successor(),
			Some(StoragePartitionedSeriesKey::new(Partition(4), Some(u8::MAX), u64::MAX, u64::MAX))
		);
		assert_eq!(StoragePartitionedSeriesKey::new(Partition(0), None, 0, 0).successor(), None);
	}

	#[test]
	fn a_storage_key_round_trips_through_its_row_key() {
		let key = SeriesRowKey {
			storage: STORAGE,
			variant_tag: Some(4),
			key: 900,
			sequence: 12,
		};
		assert_eq!(StorageSeriesKey::from(key.clone()).with_storage(STORAGE), key);

		let partitioned = PartitionedSeriesRowKey::new(STORAGE, Partition(3), None, 900, 12);
		assert_eq!(StoragePartitionedSeriesKey::from(partitioned.clone()).with_storage(STORAGE), partitioned);
	}
}
