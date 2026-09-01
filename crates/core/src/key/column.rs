// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_macro::Key;

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::{
		id::{ColumnId, ColumnSnapshotId, SeriesId, TableId},
		object::ObjectId,
	},
	key::{
		catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
		typed::key::Key,
	},
};

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Column)]
pub struct ColumnKey {
	pub object: ObjectId,
	pub column: ColumnId,
}

impl ColumnKey {
	pub fn encoded(object: impl Into<ObjectId>, column: impl Into<ColumnId>) -> EncodedKey {
		Key::encode(&Self {
			object: object.into(),
			column: column.into(),
		})
	}

	pub fn full_scan(object: impl Into<ObjectId>) -> EncodedKeyRange {
		let object = object.into();
		EncodedKeyRange::start_end(Some(Self::start(object)), Some(Self::end(object)))
	}

	fn start(object: ObjectId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(<ColumnKey as Key>::KIND as u8).extend_object_id(object);
		serializer.to_encoded_key()
	}

	fn end(object: ObjectId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(<ColumnKey as Key>::KIND as u8).extend_object_id(object.prev());
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod column_key_tests {
	use crate::{
		interface::catalog::{id::ColumnId, object::ObjectId},
		key::{column::ColumnKey, typed::key::Key},
	};

	#[test]
	fn test_encode_decode() {
		let key = ColumnKey {
			object: ObjectId::table(0xABCD),
			column: ColumnId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xF8, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43,
			0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = ColumnKey::decode(&encoded).unwrap();
		assert_eq!(key.object, 0xABCD);
		assert_eq!(key.column, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = ColumnKey {
			object: ObjectId::table(1),
			column: ColumnId(100),
		};
		let key2 = ColumnKey {
			object: ObjectId::table(1),
			column: ColumnId(200),
		};
		let key3 = ColumnKey {
			object: ObjectId::table(2),
			column: ColumnId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Key)]
#[key(kind = ColumnSequence)]
pub struct ColumnSequenceKey {
	pub object: ObjectId,
	pub column: ColumnId,
}

impl ColumnSequenceKey {
	pub fn encoded(object: impl Into<ObjectId>, column: impl Into<ColumnId>) -> EncodedKey {
		Key::encode(&Self {
			object: object.into(),
			column: column.into(),
		})
	}
}

#[cfg(test)]
pub mod column_sequence_key_tests {
	use reifydb_codec::key::encoded::EncodedKey;

	use super::ColumnSequenceKey;
	use crate::{
		interface::catalog::{id::ColumnId, object::ObjectId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = ColumnSequenceKey {
			object: ObjectId::table(0x1234),
			column: ColumnId(0x5678),
		};
		let encoded = key.encode();

		assert_eq!(encoded[0], 0xF1);

		let decoded = ColumnSequenceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.object, ObjectId::table(0x1234));
		assert_eq!(decoded.column, ColumnId(0x5678));
	}

	#[test]
	fn test_decode_invalid_version() {
		let mut encoded = vec![0xFF];
		encoded.push(0x0E);
		encoded.extend(&[0; 16]);

		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_kind() {
		let mut encoded = vec![0x01];
		encoded.push(0xFF);
		encoded.extend(&[0; 16]);

		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}

	#[test]
	fn test_decode_invalid_length() {
		let encoded = vec![0x01, 0x0E];
		let decoded = ColumnSequenceKey::decode(&EncodedKey::new(encoded));
		assert!(decoded.is_none());
	}
}

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSnapshotKey {
	pub snapshot: ColumnSnapshotId,
}

impl ColumnSnapshotKey {
	pub fn new(snapshot: ColumnSnapshotId) -> Self {
		Self {
			snapshot,
		}
	}

	pub fn encoded(snapshot: impl Into<ColumnSnapshotId>) -> EncodedKey {
		Self::new(snapshot.into()).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::scan_start()), Some(Self::scan_end()))
	}

	fn scan_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn scan_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for ColumnSnapshotKey {
	const KIND: KeyKind = KeyKind::ColumnSnapshot;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.snapshot);
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

		let snapshot = de.read_u64().ok()?;

		Some(Self {
			snapshot: ColumnSnapshotId(snapshot),
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesColumnSnapshotKey {
	pub series: SeriesId,
	pub snapshot: ColumnSnapshotId,
}

impl SeriesColumnSnapshotKey {
	pub fn new(series: SeriesId, snapshot: ColumnSnapshotId) -> Self {
		Self {
			series,
			snapshot,
		}
	}

	pub fn encoded(series: impl Into<SeriesId>, snapshot: impl Into<ColumnSnapshotId>) -> EncodedKey {
		Self::new(series.into(), snapshot.into()).encode()
	}

	pub fn full_scan(series: SeriesId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(series)), Some(Self::link_end(series)))
	}

	fn link_start(series: SeriesId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(series);
		serializer.to_encoded_key()
	}

	fn link_end(series: SeriesId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*series - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for SeriesColumnSnapshotKey {
	const KIND: KeyKind = KeyKind::SeriesColumnSnapshot;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.series)
			.extend_u64(self.snapshot);
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

		let series = de.read_u64().ok()?;
		let snapshot = de.read_u64().ok()?;

		Some(Self {
			series: SeriesId(series),
			snapshot: ColumnSnapshotId(snapshot),
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableColumnSnapshotKey {
	pub table: TableId,
	pub snapshot: ColumnSnapshotId,
}

impl TableColumnSnapshotKey {
	pub fn new(table: TableId, snapshot: ColumnSnapshotId) -> Self {
		Self {
			table,
			snapshot,
		}
	}

	pub fn encoded(table: impl Into<TableId>, snapshot: impl Into<ColumnSnapshotId>) -> EncodedKey {
		Self::new(table.into(), snapshot.into()).encode()
	}

	pub fn full_scan(table: TableId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(table)), Some(Self::link_end(table)))
	}

	fn link_start(table: TableId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(table);
		serializer.to_encoded_key()
	}

	fn link_end(table: TableId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(*table - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for TableColumnSnapshotKey {
	const KIND: KeyKind = KeyKind::TableColumnSnapshot;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_u64(self.table)
			.extend_u64(self.snapshot);
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

		let table = de.read_u64().ok()?;
		let snapshot = de.read_u64().ok()?;

		Some(Self {
			table: TableId(table),
			snapshot: ColumnSnapshotId(snapshot),
		})
	}
}

#[cfg(test)]
pub mod column_snapshot_key_tests {
	use std::ops::Bound;

	use super::*;

	#[test]
	fn test_column_snapshot_key_encode_decode() {
		let key = ColumnSnapshotKey {
			snapshot: ColumnSnapshotId(0x1234),
		};
		let encoded = key.encode();
		let decoded = ColumnSnapshotKey::decode(&encoded).unwrap();
		assert_eq!(decoded.snapshot, key.snapshot);
	}

	#[test]
	fn test_column_snapshot_key_full_scan() {
		let range = ColumnSnapshotKey::full_scan();
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}

	#[test]
	fn test_series_column_snapshot_key_encode_decode() {
		let key = SeriesColumnSnapshotKey {
			series: SeriesId(42),
			snapshot: ColumnSnapshotId(99),
		};
		let encoded = key.encode();
		let decoded = SeriesColumnSnapshotKey::decode(&encoded).unwrap();
		assert_eq!(decoded.series, key.series);
		assert_eq!(decoded.snapshot, key.snapshot);
	}

	#[test]
	fn test_table_column_snapshot_key_encode_decode() {
		let key = TableColumnSnapshotKey {
			table: TableId(42),
			snapshot: ColumnSnapshotId(99),
		};
		let encoded = key.encode();
		let decoded = TableColumnSnapshotKey::decode(&encoded).unwrap();
		assert_eq!(decoded.table, key.table);
		assert_eq!(decoded.snapshot, key.snapshot);
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Columns)]
pub struct ColumnsKey {
	pub column: ColumnId,
}

impl ColumnsKey {
	pub fn encoded(column: impl Into<ColumnId>) -> EncodedKey {
		Key::encode(&Self {
			column: column.into(),
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::column_start()), Some(Self::column_end()))
	}

	fn column_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<ColumnsKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn column_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<ColumnsKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod columns_key_tests {
	use super::ColumnsKey;
	use crate::{interface::catalog::id::ColumnId, key::typed::key::Key};

	#[test]
	fn test_encode_decode() {
		let key = ColumnsKey {
			column: ColumnId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xF9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = ColumnsKey::decode(&encoded).unwrap();
		assert_eq!(key.column, 0xABCD);
	}
}
