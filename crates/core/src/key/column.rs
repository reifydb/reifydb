// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::KeyCodec;
use reifydb_value::value::partition::Partition;

use super::KeyTag;
use crate::{
	interface::catalog::{
		id::{ColumnBlockId, ColumnId, SeriesId, TableId},
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
#[key(tag = ColumnBlock)]
pub struct ColumnBlockKey {
	pub block: ColumnBlockId,
}

impl ColumnBlockKey {
	pub fn new(block: ColumnBlockId) -> Self {
		Self {
			block,
		}
	}

	pub fn encoded(block: impl Into<ColumnBlockId>) -> EncodedKey {
		Self::new(block.into()).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = SeriesColumnBlock)]
pub struct SeriesColumnBlockKey {
	pub series: SeriesId,
	pub partition: Partition,
	pub block: ColumnBlockId,
}

impl SeriesColumnBlockKey {
	pub fn new(series: SeriesId, partition: Partition, block: ColumnBlockId) -> Self {
		Self {
			series,
			partition,
			block,
		}
	}

	pub fn encoded(
		series: impl Into<SeriesId>,
		partition: Partition,
		block: impl Into<ColumnBlockId>,
	) -> EncodedKey {
		Self::new(series.into(), partition, block.into()).encode()
	}

	pub fn full_scan(series: SeriesId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, [Field::UDesc(Width::U64, series.0 as u128)])
	}

	pub fn partition_scan(series: SeriesId, partition: Partition) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(
			Self::TAG,
			[Field::UDesc(Width::U64, series.0 as u128), Field::UDesc(Width::U128, partition.0)],
		)
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = TableColumnBlock)]
pub struct TableColumnBlockKey {
	pub table: TableId,
	pub block: ColumnBlockId,
}

impl TableColumnBlockKey {
	pub fn new(table: TableId, block: ColumnBlockId) -> Self {
		Self {
			table,
			block,
		}
	}

	pub fn encoded(table: impl Into<TableId>, block: impl Into<ColumnBlockId>) -> EncodedKey {
		Self::new(table.into(), block.into()).encode()
	}

	pub fn full_scan(table: TableId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, [Field::UDesc(Width::U64, table.0 as u128)])
	}
}

#[cfg(test)]
pub mod column_block_key_tests {
	use std::ops::Bound;

	use reifydb_codec::key::encoded::EncodedKey;

	use super::*;

	fn contains(range: &TaggedKeyBoundRange, key: &EncodedKey) -> bool {
		let start = match &range.start {
			Bound::Included(b) => *key >= b.encode(),
			Bound::Excluded(b) => *key > b.encode(),
			Bound::Unbounded => true,
		};
		let end = match &range.end {
			Bound::Included(b) => *key <= b.encode(),
			Bound::Excluded(b) => *key < b.encode(),
			Bound::Unbounded => true,
		};
		start && end
	}

	#[test]
	fn a_block_key_reports_its_own_kind_from_its_first_byte() {
		// the leading version byte made KeyTag::of read every block key as a namespace key, so the
		// metrics parser and the entry classifier both routed them to an owner they never belonged to
		assert_eq!(KeyTag::of(ColumnBlockKey::encoded(ColumnBlockId(1))), Some(KeyTag::ColumnBlock));
		assert_eq!(
			KeyTag::of(SeriesColumnBlockKey::encoded(SeriesId(1), Partition(3), ColumnBlockId(2))),
			Some(KeyTag::SeriesColumnBlock)
		);
		assert_eq!(
			KeyTag::of(TableColumnBlockKey::encoded(TableId(1), ColumnBlockId(2))),
			Some(KeyTag::TableColumnBlock)
		);
	}

	#[test]
	fn test_column_block_key_encode_decode() {
		let key = ColumnBlockKey {
			block: ColumnBlockId(0x1234),
		};
		let encoded = key.encode();
		let decoded = ColumnBlockKey::decode(&encoded).unwrap();
		assert_eq!(decoded.block, key.block);
	}

	#[test]
	fn test_column_block_key_full_scan() {
		let range = ColumnBlockKey::full_scan();
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}

	#[test]
	fn test_series_column_block_key_encode_decode() {
		let key = SeriesColumnBlockKey {
			series: SeriesId(42),
			partition: Partition(0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210),
			block: ColumnBlockId(99),
		};
		let encoded = key.encode();
		let decoded = SeriesColumnBlockKey::decode(&encoded).unwrap();
		assert_eq!(decoded.series, key.series);
		assert_eq!(decoded.partition, key.partition);
		assert_eq!(decoded.block, key.block);
	}

	#[test]
	fn test_series_column_block_partition_scan_excludes_other_partitions() {
		// the scoped listing reads this range directly, so a prefix that still covers a
		// neighbouring partition would hand the scan another partition's blocks
		let range = SeriesColumnBlockKey::partition_scan(SeriesId(42), Partition(7));
		let inside = SeriesColumnBlockKey::encoded(SeriesId(42), Partition(7), ColumnBlockId(1));
		let other_partition = SeriesColumnBlockKey::encoded(SeriesId(42), Partition(8), ColumnBlockId(1));
		let other_series = SeriesColumnBlockKey::encoded(SeriesId(43), Partition(7), ColumnBlockId(1));

		assert!(contains(&range, &inside), "the scanned partition must be inside its own range");
		assert!(!contains(&range, &other_partition), "a different partition must fall outside");
		assert!(!contains(&range, &other_series), "a different series must fall outside");
	}

	#[test]
	fn test_series_column_block_full_scan_spans_every_partition() {
		// a query with no partition filter still needs every block of the series
		let range = SeriesColumnBlockKey::full_scan(SeriesId(42));
		for partition in [Partition(0), Partition(7), Partition(u128::MAX)] {
			let key = SeriesColumnBlockKey::encoded(SeriesId(42), partition, ColumnBlockId(1));
			assert!(contains(&range, &key), "partition {:?} must be inside the full scan", partition);
		}
		let other_series = SeriesColumnBlockKey::encoded(SeriesId(43), Partition(7), ColumnBlockId(1));
		assert!(!contains(&range, &other_series), "a different series must fall outside");
	}

	#[test]
	fn test_table_column_block_key_encode_decode() {
		let key = TableColumnBlockKey {
			table: TableId(42),
			block: ColumnBlockId(99),
		};
		let encoded = key.encode();
		let decoded = TableColumnBlockKey::decode(&encoded).unwrap();
		assert_eq!(decoded.table, key.table);
		assert_eq!(decoded.block, key.block);
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
