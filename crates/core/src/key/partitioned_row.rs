// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::value::{partition::Partition, row_number::RowNumber};

use super::{EncodableKey, EncodableKeyRange, KeyKind};
use crate::{
	interface::catalog::{object::ObjectId, storage::StorageId},
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
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
		Self::new(storage, partition, row).encode()
	}

	pub fn storage_of(key: &EncodedKey) -> Option<StorageId> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}
		StorageId::from_object(de.read_object_id().ok()?)
	}

	pub fn full_scan(storage: impl Into<StorageId>) -> EncodedKeyRange {
		let storage = ObjectId::from(storage.into());
		let mut start = KeySerializer::with_capacity(10);
		start.extend_u8(Self::KIND as u8).extend_object_id(storage);
		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(Self::KIND as u8).extend_object_id(storage.prev());
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}

	pub fn scan_range(storage: impl Into<StorageId>, last_key: Option<&EncodedKey>) -> EncodedKeyRange {
		let storage = ObjectId::from(storage.into());
		let start = match last_key {
			Some(last) => Bound::Excluded(last.clone()),
			None => {
				let mut start = KeySerializer::with_capacity(10);
				start.extend_u8(Self::KIND as u8).extend_object_id(storage);
				Bound::Included(start.to_encoded_key())
			}
		};
		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(Self::KIND as u8).extend_object_id(storage.prev());
		EncodedKeyRange::new(start, Bound::Included(end.to_encoded_key()))
	}

	pub fn partition_range(storage: impl Into<StorageId>, partition: Partition) -> EncodedKeyRange {
		let storage = storage.into();
		let mut prefix = KeySerializer::with_capacity(26);
		prefix.extend_u8(Self::KIND as u8).extend_object_id(storage).extend_u128(partition.0);
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

impl EncodableKey for PartitionedRowKey {
	const KIND: KeyKind = KeyKind::PartitionedRow;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.storage).extend_u128(self.partition.0);
		serializer.extend_u64(self.row.0);
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
		let row = RowNumber(de.read_u64().ok()?);

		Some(Self {
			storage,
			partition,
			row,
		})
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
mod tests {
	use std::ops::RangeBounds;

	use reifydb_codec::key::serializer::KeySerializer;
	use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

	use super::{EncodableKey, PartitionedRowKey};
	use crate::{
		interface::catalog::{
			id::{TableId, ViewId},
			object::ObjectId,
			storage::StorageId,
		},
		key::catalog::KeySerializerCatalogExt,
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
}
