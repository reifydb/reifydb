// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::{
	key::{
		deserializer::KeyDeserializer,
		encoded::{EncodedKey, EncodedKeyRange},
		serializer::KeySerializer,
	},
	row::shape::fingerprint::RowShapeFingerprint,
};
use reifydb_macro::Key;
use reifydb_value::value::{partition::Partition, row_number::RowNumber};
use serde::{Deserialize, Serialize};

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	interface::catalog::{object::ObjectId, storage::StorageId},
	key::{
		catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
		typed::key::Key,
	},
};

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Row)]
pub struct RowKey {
	pub storage: StorageId,
	pub row: RowNumber,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowKeyRange {
	pub storage: StorageId,
}

impl RowKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let storage = StorageId::from_object(de.read_object_id().ok()?)?;

		Some(RowKeyRange {
			storage,
		})
	}

	pub fn scan_range(storage: StorageId, last_key: Option<&EncodedKey>) -> EncodedKeyRange {
		let range = RowKeyRange {
			storage,
		};

		if let Some(last_key) = last_key {
			EncodedKeyRange::new(Bound::Excluded(last_key.clone()), Bound::Included(range.end().unwrap()))
		} else {
			EncodedKeyRange::new(
				Bound::Included(range.start().unwrap()),
				Bound::Included(range.end().unwrap()),
			)
		}
	}

	pub fn scan_range_rev(storage: StorageId, last_key: Option<&EncodedKey>) -> EncodedKeyRange {
		let range = RowKeyRange {
			storage,
		};

		if let Some(last_key) = last_key {
			EncodedKeyRange::new(Bound::Included(range.start().unwrap()), Bound::Excluded(last_key.clone()))
		} else {
			EncodedKeyRange::new(
				Bound::Included(range.start().unwrap()),
				Bound::Included(range.end().unwrap()),
			)
		}
	}
}

impl EncodableKeyRange for RowKeyRange {
	const KIND: KeyKind = KeyKind::Row;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.storage);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(ObjectId::from(self.storage).prev());
		Some(serializer.to_encoded_key())
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
		let start_key = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		let end_key = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		(start_key, end_key)
	}
}

impl RowKey {
	pub fn encoded(storage: impl Into<StorageId>, row: impl Into<RowNumber>) -> EncodedKey {
		Key::encode(&Self {
			storage: storage.into(),
			row: row.into(),
		})
	}

	pub fn full_scan(storage: impl Into<StorageId>) -> EncodedKeyRange {
		let storage = storage.into();
		EncodedKeyRange::start_end(Some(Self::storage_start(storage)), Some(Self::storage_end(storage)))
	}

	pub fn storage_start(storage: impl Into<StorageId>) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(<RowKey as Key>::KIND as u8).extend_object_id(storage.into());
		serializer.to_encoded_key()
	}

	pub fn storage_end(storage: impl Into<StorageId>) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer
			.extend_u8(<RowKey as Key>::KIND as u8)
			.extend_object_id(ObjectId::from(storage.into()).prev());
		serializer.to_encoded_key()
	}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowIdent(pub RowNumber);

impl From<RowKey> for RowIdent {
	fn from(key: RowKey) -> Self {
		RowIdent(key.row)
	}
}

impl RowIdent {
	pub fn with_storage(self, storage: StorageId) -> RowKey {
		RowKey {
			storage,
			row: self.0,
		}
	}
}

#[cfg(test)]
pub mod row_key_tests {
	use reifydb_value::value::row_number::RowNumber;

	use super::RowKey;
	use crate::{interface::catalog::storage::StorageId, key::typed::key::Key};

	#[test]
	fn test_encode_decode() {
		let key = RowKey {
			storage: StorageId::table(0xABCD),
			row: RowNumber(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFC, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43,
			0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = RowKey::decode(&encoded).unwrap();
		assert_eq!(key.storage, StorageId::table(0xABCD));
		assert_eq!(key.row, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_encode_decode_view() {
		// Without the view tag narrowing back through `from_object`, a view's row key decodes to `None`.
		let key = RowKey {
			storage: StorageId::view(0xABCD),
			row: RowNumber(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFC, 0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43,
			0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = RowKey::decode(&encoded).unwrap();
		assert_eq!(key.storage, StorageId::view(0xABCD));
		assert_eq!(key.row, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = RowKey {
			storage: StorageId::table(1),
			row: RowNumber(100),
		};
		let key2 = RowKey {
			storage: StorageId::table(1),
			row: RowNumber(200),
		};
		let key3 = RowKey {
			storage: StorageId::table(2),
			row: RowNumber(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}

	#[test]
	fn test_row_ident_roundtrip() {
		use super::RowIdent;

		let key = RowKey {
			storage: StorageId::table(7),
			row: RowNumber(42),
		};

		// dropping storage and re-supplying the same value must recover the original key
		let ident: RowIdent = key.clone().into();
		let restored = ident.with_storage(key.storage);
		assert_eq!(restored, key);
	}

	#[test]
	fn test_row_ident_ordering_matches_row_number() {
		use super::RowIdent;

		let low = RowIdent(RowNumber(1));
		let high = RowIdent(RowNumber(2));

		// narrow identity must sort ascending by row number, same as the full key does today
		assert!(low < high);
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = RowSequence)]
pub struct RowSequenceKey {
	pub storage: StorageId,
}

impl RowSequenceKey {
	pub fn encoded(storage: impl Into<StorageId>) -> EncodedKey {
		Key::encode(&Self {
			storage: storage.into(),
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::sequence_start()), Some(Self::sequence_end()))
	}

	fn sequence_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<RowSequenceKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn sequence_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<RowSequenceKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod row_sequence_key_tests {
	use super::{Key, RowSequenceKey};
	use crate::interface::catalog::storage::StorageId;

	#[test]
	fn test_encode_decode() {
		let key = RowSequenceKey {
			storage: StorageId::table(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xF7, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = RowSequenceKey::decode(&encoded).unwrap();
		assert_eq!(key.storage, StorageId::table(0xABCD));
	}

	#[test]
	fn test_encode_decode_view() {
		// A view owns its row numbering; the view tag must survive the narrowing back through decode.
		let key = RowSequenceKey {
			storage: StorageId::view(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xF7, 0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = RowSequenceKey::decode(&encoded).unwrap();
		assert_eq!(key.storage, StorageId::view(0xABCD));
	}
}

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
pub mod row_settings_key_tests {
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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = RowShape)]
pub struct RowShapeKey {
	pub fingerprint: RowShapeFingerprint,
}

impl RowShapeKey {
	pub fn encoded(fingerprint: RowShapeFingerprint) -> EncodedKey {
		Key::encode(&Self {
			fingerprint,
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::scan_start()), Some(Self::scan_end()))
	}

	fn scan_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<RowShapeKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn scan_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<RowShapeKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = RowShapeField)]
pub struct RowShapeFieldKey {
	pub shape_fingerprint: RowShapeFingerprint,
	pub field_index: u16,
}

impl RowShapeFieldKey {
	pub fn encoded(shape_fingerprint: RowShapeFingerprint, field_index: u16) -> EncodedKey {
		Key::encode(&Self {
			shape_fingerprint,
			field_index,
		})
	}

	pub fn scan_for_shape(fingerprint: RowShapeFingerprint) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::shape_start(fingerprint)), Some(Self::shape_end(fingerprint)))
	}

	fn shape_start(fingerprint: RowShapeFingerprint) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<RowShapeFieldKey as Key>::KIND as u8).extend_u64(fingerprint.as_u64());
		serializer.to_encoded_key()
	}

	fn shape_end(fingerprint: RowShapeFingerprint) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer
			.extend_u8(<RowShapeFieldKey as Key>::KIND as u8)
			.extend_u64(fingerprint.as_u64())
			.extend_u8(0xFF);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod row_shape_key_tests {
	use super::*;

	#[test]
	fn test_shape_key_encode_decode() {
		let key = RowShapeKey {
			fingerprint: RowShapeFingerprint::new(0xDEADBEEFCAFEBABE),
		};
		let encoded = Key::encode(&key);
		let decoded = <RowShapeKey as Key>::decode(&encoded).unwrap();
		assert_eq!(decoded.fingerprint, RowShapeFingerprint::new(0xDEADBEEFCAFEBABE));
	}

	#[test]
	fn test_shape_field_key_encode_decode() {
		let key = RowShapeFieldKey {
			shape_fingerprint: RowShapeFingerprint::new(0x1234567890ABCDEF),
			field_index: 42,
		};
		let encoded = Key::encode(&key);
		let decoded = <RowShapeFieldKey as Key>::decode(&encoded).unwrap();
		assert_eq!(decoded.shape_fingerprint, RowShapeFingerprint::new(0x1234567890ABCDEF));
		assert_eq!(decoded.field_index, 42);
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = PartitionedRow)]
pub struct PartitionedRowKey {
	pub storage: StorageId,
	pub partition: Partition,
	pub row: RowNumber,
}

impl PartitionedRowKey {
	pub fn new(storage: impl Into<StorageId>, partition: Partition, row: RowNumber) -> Self {
		Self {
			storage: storage.into(),
			partition,
			row,
		}
	}

	pub fn encoded(storage: impl Into<StorageId>, partition: Partition, row: RowNumber) -> EncodedKey {
		Key::encode(&Self::new(storage, partition, row))
	}

	pub fn storage_of(key: &EncodedKey) -> Option<StorageId> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != <Self as Key>::KIND {
			return None;
		}
		StorageId::from_object(de.read_object_id().ok()?)
	}

	pub fn full_scan(storage: impl Into<StorageId>) -> EncodedKeyRange {
		let storage = ObjectId::from(storage.into());
		let mut start = KeySerializer::with_capacity(10);
		start.extend_u8(<Self as Key>::KIND as u8).extend_object_id(storage);
		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(<Self as Key>::KIND as u8).extend_object_id(storage.prev());
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}

	pub fn scan_range(storage: impl Into<StorageId>, last_key: Option<&EncodedKey>) -> EncodedKeyRange {
		let storage = ObjectId::from(storage.into());
		let start = match last_key {
			Some(last) => Bound::Excluded(last.clone()),
			None => {
				let mut start = KeySerializer::with_capacity(10);
				start.extend_u8(<Self as Key>::KIND as u8).extend_object_id(storage);
				Bound::Included(start.to_encoded_key())
			}
		};
		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(<Self as Key>::KIND as u8).extend_object_id(storage.prev());
		EncodedKeyRange::new(start, Bound::Included(end.to_encoded_key()))
	}

	pub fn partition_range(storage: impl Into<StorageId>, partition: Partition) -> EncodedKeyRange {
		let storage = storage.into();
		let mut prefix = KeySerializer::with_capacity(26);
		prefix.extend_u8(<Self as Key>::KIND as u8).extend_object_id(storage).extend_u128(partition.0);
		EncodedKeyRange::prefix(prefix.to_encoded_key().as_slice())
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
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionedRowIdent {
	pub partition_hi: u64,
	pub partition_lo: u64,
	pub row: RowNumber,
}

impl PartitionedRowIdent {
	pub fn new(partition: Partition, row: RowNumber) -> Self {
		Self {
			partition_hi: (partition.0 >> 64) as u64,
			partition_lo: partition.0 as u64,
			row,
		}
	}

	pub fn partition(self) -> Partition {
		Partition(((self.partition_hi as u128) << 64) | self.partition_lo as u128)
	}

	pub fn with_storage(self, storage: StorageId) -> PartitionedRowKey {
		PartitionedRowKey {
			storage,
			partition: self.partition(),
			row: self.row,
		}
	}
}

impl From<PartitionedRowKey> for PartitionedRowIdent {
	fn from(key: PartitionedRowKey) -> Self {
		PartitionedRowIdent::new(key.partition, key.row)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedRowKeyRange {
	pub storage: StorageId,
}

impl PartitionedRowKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let storage = StorageId::from_object(de.read_object_id().ok()?)?;

		Some(PartitionedRowKeyRange {
			storage,
		})
	}
}

impl EncodableKeyRange for PartitionedRowKeyRange {
	const KIND: KeyKind = KeyKind::PartitionedRow;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.storage);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(ObjectId::from(self.storage).prev());
		Some(serializer.to_encoded_key())
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
		let start_key = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		let end_key = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		(start_key, end_key)
	}
}

#[cfg(test)]
mod partitioned_row_key_tests {
	use std::ops::RangeBounds;

	use reifydb_codec::key::serializer::KeySerializer;
	use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

	use super::PartitionedRowKey;
	use crate::{
		interface::catalog::{
			id::{TableId, ViewId},
			object::ObjectId,
			storage::StorageId,
		},
		key::{catalog::KeySerializerCatalogExt, typed::key::Key},
	};

	fn part(v: &str) -> Partition {
		Partition::of(&[Value::Utf8(v.to_string())])
	}

	#[test]
	fn test_table_roundtrip() {
		let key = PartitionedRowKey {
			storage: StorageId::Table(TableId(7)),
			partition: part("us"),
			row: RowNumber(42),
		};
		let decoded = PartitionedRowKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_view_roundtrip() {
		// A view owns its rows directly; without the view tag narrowing back the key decodes to `None`.
		let key = PartitionedRowKey {
			storage: StorageId::View(ViewId(11)),
			partition: part("us"),
			row: RowNumber(42),
		};
		let decoded = PartitionedRowKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_storage_of() {
		let key = PartitionedRowKey::encoded(StorageId::Table(TableId(42)), part("us"), RowNumber(1));
		assert_eq!(PartitionedRowKey::storage_of(&key), Some(StorageId::Table(TableId(42))));
	}

	#[test]
	fn test_storage_of_rejects_a_rowless_object() {
		// A vtable owns no rows, so its tag must not narrow into a table of the same numeric id.
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(PartitionedRowKey::KIND as u8).extend_object_id(ObjectId::vtable(42));
		assert_eq!(PartitionedRowKey::storage_of(&serializer.to_encoded_key()), None);
	}

	#[test]
	fn test_partition_rows_cluster_together() {
		let storage = StorageId::Table(TableId(1));
		let us_a = PartitionedRowKey::encoded(storage, part("us"), RowNumber(1));
		let us_b = PartitionedRowKey::encoded(storage, part("us"), RowNumber(2));
		let eu = PartitionedRowKey::encoded(storage, part("eu"), RowNumber(1));

		let mut keys = [us_a.clone(), us_b.clone(), eu.clone()];
		keys.sort();
		let us_positions: Vec<usize> =
			keys.iter().enumerate().filter(|(_, k)| **k == us_a || **k == us_b).map(|(i, _)| i).collect();
		assert_eq!(us_positions[1] - us_positions[0], 1, "us partition rows must be contiguous");
	}

	#[test]
	fn test_partition_range_contains_only_its_partition() {
		let storage = StorageId::Table(TableId(1));
		let range = PartitionedRowKey::partition_range(storage, part("us"));
		let us = PartitionedRowKey::encoded(storage, part("us"), RowNumber(500));
		let eu = PartitionedRowKey::encoded(storage, part("eu"), RowNumber(1));
		assert!(range.contains(&us), "us row must be inside the us partition range");
		assert!(!range.contains(&eu), "eu row must be outside the us partition range");
	}

	#[test]
	fn test_partitioned_row_ident_roundtrip() {
		use super::PartitionedRowIdent;

		let key = PartitionedRowKey {
			storage: StorageId::Table(TableId(7)),
			partition: part("us"),
			row: RowNumber(42),
		};

		// dropping storage and re-supplying it must recover the original key, halves included
		let ident: PartitionedRowIdent = key.clone().into();
		let restored = ident.with_storage(key.storage);
		assert_eq!(restored, key);
	}

	#[test]
	fn test_partitioned_row_ident_halves_split_correctly() {
		use super::PartitionedRowIdent;

		let partition = Partition(0x1122334455667788_99AABBCCDDEEFF00);
		let ident = PartitionedRowIdent::new(partition, RowNumber(1));

		// the two native halves must reassemble into the exact original 128-bit value
		assert_eq!(ident.partition_hi, 0x1122334455667788);
		assert_eq!(ident.partition_lo, 0x99AABBCCDDEEFF00);
		assert_eq!(ident.partition(), partition);
	}

	#[test]
	fn test_partitioned_row_ident_ordering_matches_field_order() {
		use super::PartitionedRowIdent;

		let lower_partition = PartitionedRowIdent::new(Partition(1), RowNumber(999));
		let higher_partition = PartitionedRowIdent::new(Partition(2), RowNumber(1));
		let same_partition_lower_row = PartitionedRowIdent::new(Partition(2), RowNumber(1));
		let same_partition_higher_row = PartitionedRowIdent::new(Partition(2), RowNumber(2));

		// partition must dominate row in ordering, matching PartitionedRowKey's field order
		assert!(lower_partition < higher_partition);
		assert!(same_partition_lower_row < same_partition_higher_row);
	}
}
