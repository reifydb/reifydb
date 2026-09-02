// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::{deserializer::KeyDeserializer, encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::storage::StorageId,
	key::{
		catalog::KeyDeserializerCatalogExt,
		kind::KeyKind,
		row::{PartitionedRowKey, RowKey, StoragePartitionedRowKey, StorageRowKey},
		typed::key::Key,
	},
};
use reifydb_value::value::{partition::Partition, row_number::RowNumber};

pub(super) fn row_ident_of(key: &[u8]) -> Option<StorageRowKey> {
	RowKey::decode(&EncodedKey::new(key)).map(StorageRowKey::from)
}

pub(super) fn row_key_for(storage: StorageId, row: i64) -> EncodedKey {
	RowKey::encoded(storage, RowNumber(row_from_sql(row)))
}

pub(super) fn partitioned_ident_of(key: &[u8]) -> Option<StoragePartitionedRowKey> {
	PartitionedRowKey::decode(&EncodedKey::new(key)).map(StoragePartitionedRowKey::from)
}

pub(super) fn partitioned_key_for(storage: StorageId, partition_hi: i64, partition_lo: i64, row: i64) -> EncodedKey {
	let ident = StoragePartitionedRowKey::from_halves(
		partition_half_from_sql(partition_hi),
		partition_half_from_sql(partition_lo),
		RowNumber(row_from_sql(row)),
	);
	PartitionedRowKey::encoded(storage, ident.partition(), ident.row())
}

fn partition_only_of(key: &[u8]) -> Option<Partition> {
	let mut de = KeyDeserializer::from_bytes(key);
	let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
	if kind != <PartitionedRowKey as Key>::KIND {
		return None;
	}
	de.read_object_id().ok()?;
	let partition = de.read_u128().ok()?;
	if !de.is_empty() {
		return None;
	}
	Some(Partition(partition))
}

pub(super) fn row_to_sql(row: u64) -> i64 {
	((!row) ^ (1u64 << 63)) as i64
}

pub(super) fn row_from_sql(value: i64) -> u64 {
	!((value as u64) ^ (1u64 << 63))
}

pub(super) fn partition_half_to_sql(half: u64) -> i64 {
	row_to_sql(half)
}

pub(super) fn partition_half_from_sql(value: i64) -> u64 {
	row_from_sql(value)
}

fn partition_halves(partition: Partition) -> (i64, i64) {
	(partition_half_to_sql((partition.0 >> 64) as u64), partition_half_to_sql(partition.0 as u64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowBoundary {
	Row(i64),
	Unbounded,
}

fn row_boundary(key: &[u8]) -> RowBoundary {
	match row_ident_of(key) {
		Some(ident) => RowBoundary::Row(row_to_sql(ident.row().0)),
		None => RowBoundary::Unbounded,
	}
}

pub(super) struct RowRangeBounds {
	pub lower: Bound<i64>,
	pub upper: Bound<i64>,
}

fn row_bound(bound: Bound<&[u8]>) -> Bound<i64> {
	match bound {
		Bound::Included(k) => match row_boundary(k) {
			RowBoundary::Row(r) => Bound::Included(r),
			RowBoundary::Unbounded => Bound::Unbounded,
		},
		Bound::Excluded(k) => match row_boundary(k) {
			RowBoundary::Row(r) => Bound::Excluded(r),
			RowBoundary::Unbounded => Bound::Unbounded,
		},
		Bound::Unbounded => Bound::Unbounded,
	}
}

pub(super) fn row_range_bounds(start: Bound<&[u8]>, end: Bound<&[u8]>) -> RowRangeBounds {
	RowRangeBounds {
		lower: row_bound(start),
		upper: row_bound(end),
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartitionedBoundary {
	Full(i64, i64, i64),
	Unbounded,
}

fn partitioned_boundary(key: &[u8]) -> PartitionedBoundary {
	match partitioned_ident_of(key) {
		Some(ident) => PartitionedBoundary::Full(
			partition_half_to_sql(ident.partition_hi()),
			partition_half_to_sql(ident.partition_lo()),
			row_to_sql(ident.row().0),
		),
		None => PartitionedBoundary::Unbounded,
	}
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
	let i = prefix.iter().rposition(|&b| b != 0xff)?;
	let mut out = prefix[..i].to_vec();
	out.push(prefix[i] + 1);
	Some(out)
}

fn partition_prefix_and_row(bytes: &[u8]) -> Option<(&[u8], Option<i64>)> {
	if let Some(ident) = partitioned_ident_of(bytes) {
		let split = bytes.len() - 8;
		return Some((&bytes[..split], Some(row_to_sql(ident.row().0))));
	}
	if partition_only_of(bytes).is_some() {
		return Some((bytes, None));
	}
	None
}

pub(super) enum PartitionedRangeBounds {
	ExactPartition {
		partition_hi: i64,
		partition_lo: i64,
		lower_row: Bound<i64>,
		upper_row: Bound<i64>,
	},
	Open {
		lower: Bound<(i64, i64, i64)>,
		upper: Bound<(i64, i64, i64)>,
	},
}

pub(super) fn partitioned_range_bounds(start: Bound<&[u8]>, end: Bound<&[u8]>) -> PartitionedRangeBounds {
	let start_excluded = matches!(start, Bound::Excluded(_));
	let start_bytes = match start {
		Bound::Included(k) | Bound::Excluded(k) => Some(k),
		Bound::Unbounded => None,
	};
	let end_bytes = match end {
		Bound::Included(k) | Bound::Excluded(k) => Some(k),
		Bound::Unbounded => None,
	};

	if let Some(sb) = start_bytes
		&& let Some((prefix, row)) = partition_prefix_and_row(sb)
		&& let Some(eb) = end_bytes
		&& prefix_successor(prefix).as_deref() == Some(eb)
		&& let Some(partition) = partition_only_of(prefix)
	{
		let (partition_hi, partition_lo) = partition_halves(partition);
		let lower_row = match row {
			Some(r) if start_excluded => Bound::Excluded(r),
			Some(r) => Bound::Included(r),
			None => Bound::Unbounded,
		};
		return PartitionedRangeBounds::ExactPartition {
			partition_hi,
			partition_lo,
			lower_row,
			upper_row: Bound::Unbounded,
		};
	}

	let lower = match start_bytes.map(partitioned_boundary) {
		Some(PartitionedBoundary::Full(hi, lo, row)) => {
			if start_excluded {
				Bound::Excluded((hi, lo, row))
			} else {
				Bound::Included((hi, lo, row))
			}
		}
		_ => Bound::Unbounded,
	};
	let end_excluded = matches!(end, Bound::Excluded(_));
	let upper = match end_bytes.map(partitioned_boundary) {
		Some(PartitionedBoundary::Full(hi, lo, row)) => {
			if end_excluded {
				Bound::Excluded((hi, lo, row))
			} else {
				Bound::Included((hi, lo, row))
			}
		}
		_ => Bound::Unbounded,
	};
	PartitionedRangeBounds::Open {
		lower,
		upper,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::store::EntryKind;
	use reifydb_value::value::{partition::Partition, row_number::RowNumber};

	use super::*;
	use crate::tier::persistent::sqlite::entry::{SqliteSchema, sqlite_schema};

	#[test]
	fn a_higher_row_stores_a_lower_integer() {
		// SQLite orders the narrow column ascending, so the stored integer must fall as the row rises,
		// otherwise every scan of a narrow table runs opposite to the encoded key space it mirrors.
		assert!(row_to_sql(1) > row_to_sql(2));
		assert!(row_to_sql(2) > row_to_sql(u64::MAX));
		assert!(row_to_sql(0) > row_to_sql(1));
	}

	#[test]
	fn the_stored_integer_round_trips_across_the_whole_range() {
		for row in [0u64, 1, 2, 42, i64::MAX as u64, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
			assert_eq!(row_from_sql(row_to_sql(row)), row, "row {row} did not survive the mapping");
		}
	}

	#[test]
	fn the_stored_order_matches_the_encoded_key_order() {
		// The narrow column and the encoded key must agree, or a table that switches schema silently
		// reverses every cursor built against it.
		let storage = StorageId::table(1);
		let mut rows: Vec<u64> = vec![7, 1, 900, 0, 42, u64::MAX];

		let mut by_key = rows.clone();
		by_key.sort_by_key(|r| RowKey::encoded(storage, RowNumber(*r)).as_slice().to_vec());

		rows.sort_by_key(|r| row_to_sql(*r));

		assert_eq!(rows, by_key, "the stored integer must sort exactly as the encoded key does");
	}

	#[test]
	fn both_partition_halves_invert_together() {
		// A partition is one 128 bit value split across two columns. If only one half inverts, a pair
		// that differs in the high half orders against a pair that differs in the low half.
		let low = Partition(1);
		let high = Partition((1u128 << 64) | 1);

		let low_halves = (partition_half_to_sql((low.0 >> 64) as u64), partition_half_to_sql(low.0 as u64));
		let high_halves = (partition_half_to_sql((high.0 >> 64) as u64), partition_half_to_sql(high.0 as u64));

		assert!(low_halves > high_halves, "the larger partition must store the lower pair, as rows do");
		assert_eq!(partition_half_from_sql(low_halves.1), 1);
		assert_eq!(partition_half_from_sql(high_halves.0), 1);
	}

	#[test]
	fn a_row_range_keeps_the_start_below_the_end() {
		// The encoded start is the smaller key, so it must stay the smaller stored integer. Swapping
		// the two here is how the narrowed schema first disagreed with the blob one.
		let storage = StorageId::table(1);
		let start = RowKey::encoded(storage, RowNumber(9));
		let end = RowKey::encoded(storage, RowNumber(2));
		assert!(start.as_slice() < end.as_slice(), "row 9 must encode below row 2");

		let bounds = row_range_bounds(Bound::Included(start.as_slice()), Bound::Included(end.as_slice()));
		match (bounds.lower, bounds.upper) {
			(Bound::Included(lower), Bound::Included(upper)) => {
				assert!(lower < upper, "the start must bound below and the end above");
				assert_eq!(row_from_sql(lower), 9);
				assert_eq!(row_from_sql(upper), 2);
			}
			other => panic!("expected two included bounds, got {other:?}"),
		}
	}

	#[test]
	fn only_the_row_shaped_storages_take_a_narrow_schema() {
		// A view entry can hold either a row key or a series key under one entry kind, so it has no
		// single narrow shape and must stay on the blob schema.
		assert_eq!(sqlite_schema(EntryKind::Source(StorageId::table(1))), SqliteSchema::Row);
		assert_eq!(sqlite_schema(EntryKind::Source(StorageId::view(1))), SqliteSchema::Blob);
		assert_eq!(sqlite_schema(EntryKind::Source(StorageId::series(1))), SqliteSchema::Blob);
		assert_eq!(sqlite_schema(EntryKind::PartitionedSource(StorageId::table(1))), SqliteSchema::Partitioned);
	}
}
