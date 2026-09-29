// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_codec::key::{encoded::EncodedKey, serializer::KeySerializer};
use reifydb_core::{
	interface::catalog::{storage::StorageId, view::ViewSortKey},
	key::{
		row::{PartitionedRowKey, PartitionedSortedViewRowKey, RowKey, SortedViewRowKey},
		sort_run::SortRun,
	},
};
use reifydb_value::{
	Result,
	value::{partition::Partition, row_number::RowNumber},
};

use crate::operator::sink::value_at;

#[inline]
pub fn row_key(storage: StorageId, row: RowNumber) -> EncodedKey {
	RowKey::encoded(storage, row)
}

#[inline]
pub fn sort_run(sort: &[ViewSortKey], cols: &RecordBatch, row_idx: usize) -> Result<SortRun> {
	let mut serializer = KeySerializer::new();
	for key in sort {
		let value = value_at(cols, key.column.0 as usize, row_idx)?;
		serializer.extend_value_with_direction(&value, key.direction.clone().into())?;
	}
	Ok(SortRun::from_encoded(serializer.to_encoded_key()))
}

#[inline]
pub fn sorted_view_key(
	storage: StorageId,
	sort: &[ViewSortKey],
	cols: &RecordBatch,
	row_idx: usize,
	row: RowNumber,
) -> Result<EncodedKey> {
	if sort.is_empty() {
		return Ok(row_key(storage, row));
	}
	Ok(SortedViewRowKey::encoded(storage, sort_run(sort, cols, row_idx)?, row))
}

#[inline]
pub fn partitioned_key(
	storage: StorageId,
	sort: &[ViewSortKey],
	cols: &RecordBatch,
	row_idx: usize,
	partition: Partition,
	row: RowNumber,
) -> Result<EncodedKey> {
	if sort.is_empty() {
		return Ok(PartitionedRowKey::encoded(storage, partition, row));
	}
	Ok(PartitionedSortedViewRowKey::encoded(storage, partition, sort_run(sort, cols, row_idx)?, row))
}
