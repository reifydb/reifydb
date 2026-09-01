// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::{deserializer::KeyDeserializer, encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::storage::StorageId,
	key::{
		Key,
		catalog::KeyDeserializerCatalogExt,
		kind::KeyKind,
		partitioned_row::{PartitionedRowIdent, PartitionedRowKey},
		row::{RowIdent, RowKey},
	},
};
use reifydb_value::value::{partition::Partition, row_number::RowNumber};

pub(super) fn row_ident_of(key: &[u8]) -> Option<RowIdent> {
	RowKey::decode(&EncodedKey::new(key)).map(RowIdent::from)
}

pub(super) fn row_key_for(storage: StorageId, row: i64) -> EncodedKey {
	RowKey::encoded(storage, RowNumber(row as u64))
}

pub(super) fn partitioned_ident_of(key: &[u8]) -> Option<PartitionedRowIdent> {
	PartitionedRowKey::decode(&EncodedKey::new(key)).map(PartitionedRowIdent::from)
}

pub(super) fn partitioned_key_for(storage: StorageId, partition_hi: i64, partition_lo: i64, row: i64) -> EncodedKey {
	let ident = PartitionedRowIdent {
		partition_hi: partition_hi as u64,
		partition_lo: partition_lo as u64,
		row: RowNumber(row as u64),
	};
	PartitionedRowKey::encoded(storage, ident.partition(), ident.row)
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

fn partition_halves(partition: Partition) -> (i64, i64) {
	((partition.0 >> 64) as u64 as i64, partition.0 as u64 as i64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowBoundary {
	Row(i64),
	Unbounded,
}

fn row_boundary(key: &[u8]) -> RowBoundary {
	match row_ident_of(key) {
		Some(ident) => RowBoundary::Row(ident.0.0 as i64),
		None => RowBoundary::Unbounded,
	}
}

pub(super) struct RowRangeBounds {
	pub lower: Bound<i64>,
	pub upper: Bound<i64>,
}

pub(super) fn row_range_bounds(start: Bound<&[u8]>, end: Bound<&[u8]>) -> RowRangeBounds {
	let upper = match start {
		Bound::Included(k) => match row_boundary(k) {
			RowBoundary::Row(r) => Bound::Included(r),
			RowBoundary::Unbounded => Bound::Unbounded,
		},
		Bound::Excluded(k) => match row_boundary(k) {
			RowBoundary::Row(r) => Bound::Excluded(r),
			RowBoundary::Unbounded => Bound::Unbounded,
		},
		Bound::Unbounded => Bound::Unbounded,
	};
	let lower = match end {
		Bound::Included(k) => match row_boundary(k) {
			RowBoundary::Row(r) => Bound::Included(r),
			RowBoundary::Unbounded => Bound::Unbounded,
		},
		Bound::Excluded(k) => match row_boundary(k) {
			RowBoundary::Row(r) => Bound::Excluded(r),
			RowBoundary::Unbounded => Bound::Unbounded,
		},
		Bound::Unbounded => Bound::Unbounded,
	};
	RowRangeBounds {
		lower,
		upper,
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
			ident.partition_hi as i64,
			ident.partition_lo as i64,
			ident.row.0 as i64,
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
		return Some((&bytes[..split], Some(ident.row.0 as i64)));
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
		let upper_row = match row {
			Some(r) if start_excluded => Bound::Excluded(r),
			Some(r) => Bound::Included(r),
			None => Bound::Unbounded,
		};
		return PartitionedRangeBounds::ExactPartition {
			partition_hi,
			partition_lo,
			lower_row: Bound::Unbounded,
			upper_row,
		};
	}

	let upper = match start_bytes.map(partitioned_boundary) {
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
	let lower = match end_bytes.map(partitioned_boundary) {
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
