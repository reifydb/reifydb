// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::KeyCodec;

use super::KeyTag;
use crate::{
	interface::catalog::{
		id::{ColumnId, ColumnSnapshotId, SeriesId, TableId},
		object::ObjectId,
	},
	key::{
		any::{Field, KeyFields, Width},
		bound::{TaggedKeyBoundRange, object_fields},
		catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
	},
};

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Column)]
pub struct ColumnKey {
	pub object: ObjectId,
	pub column: ColumnId,
}

impl ColumnKey {
	pub fn new(object: impl Into<ObjectId>, column: impl Into<ColumnId>) -> Self {
		Self {
			object: object.into(),
			column: column.into(),
		}
	}

	pub fn encoded(object: impl Into<ObjectId>, column: impl Into<ColumnId>) -> EncodedKey {
		Self::new(object, column).encode()
	}

	pub fn full_scan(object: impl Into<ObjectId>) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, object_fields(object.into()))
	}
}

#[cfg(test)]
pub mod column_key_tests {
	use crate::{
		interface::catalog::{id::ColumnId, object::ObjectId},
		key::column::ColumnKey,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, KeyCodec, Hash)]
#[key(tag = ColumnSequence)]
pub struct ColumnSequenceKey {
	pub object: ObjectId,
	pub column: ColumnId,
}

impl ColumnSequenceKey {
	pub fn new(object: impl Into<ObjectId>, column: impl Into<ColumnId>) -> Self {
		Self {
			object: object.into(),
			column: column.into(),
		}
	}

	pub fn encoded(object: impl Into<ObjectId>, column: impl Into<ColumnId>) -> EncodedKey {
		Self::new(object, column).encode()
	}
}

#[cfg(test)]
pub mod column_sequence_key_tests {
	use reifydb_codec::key::encoded::EncodedKey;

	use super::ColumnSequenceKey;
	use crate::interface::catalog::{id::ColumnId, object::ObjectId};

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

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = ColumnSnapshot)]
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

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = SeriesColumnSnapshot)]
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

	pub fn full_scan(series: SeriesId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, [Field::UDesc(Width::U64, series.0 as u128)])
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = TableColumnSnapshot)]
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

	pub fn full_scan(table: TableId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, [Field::UDesc(Width::U64, table.0 as u128)])
	}
}

#[cfg(test)]
pub mod column_snapshot_key_tests {
	use std::ops::Bound;

	use super::*;

	#[test]
	fn a_snapshot_key_reports_its_own_kind_from_its_first_byte() {
		// the leading version byte made KeyTag::of read every snapshot key as a namespace key, so the
		// metrics parser and the entry classifier both routed them to an owner they never belonged to
		assert_eq!(KeyTag::of(&ColumnSnapshotKey::encoded(ColumnSnapshotId(1))), Some(KeyTag::ColumnSnapshot));
		assert_eq!(
			KeyTag::of(&SeriesColumnSnapshotKey::encoded(SeriesId(1), ColumnSnapshotId(2))),
			Some(KeyTag::SeriesColumnSnapshot)
		);
		assert_eq!(
			KeyTag::of(&TableColumnSnapshotKey::encoded(TableId(1), ColumnSnapshotId(2))),
			Some(KeyTag::TableColumnSnapshot)
		);
	}

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

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Columns)]
pub struct ColumnsKey {
	pub column: ColumnId,
}

impl ColumnsKey {
	pub fn new(column: impl Into<ColumnId>) -> Self {
		Self {
			column: column.into(),
		}
	}

	pub fn encoded(column: impl Into<ColumnId>) -> EncodedKey {
		Self::new(column).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod columns_key_tests {
	use super::ColumnsKey;
	use crate::interface::catalog::id::ColumnId;

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
