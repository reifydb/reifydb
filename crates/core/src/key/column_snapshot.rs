// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::id::{ColumnSnapshotId, SeriesId, TableId},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

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
pub mod tests {
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
