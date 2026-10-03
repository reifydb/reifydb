// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_codec::key::{column::extend_column_with_direction, encoded::EncodedKey, serializer::KeySerializer};
use reifydb_core::{
	interface::catalog::{storage::StorageId, view::ViewSortKey},
	key::{
		row::{PartitionedRowKey, PartitionedSortedViewRowKey, RowKey, SortedViewRowKey},
		sort_run::SortRun,
	},
};
use reifydb_value::{
	Result,
	value::{column_view::ColumnView, partition::Partition, row_number::RowNumber},
};

#[inline]
pub fn row_key(storage: StorageId, row: RowNumber) -> EncodedKey {
	RowKey::encoded(storage, row)
}

pub fn sort_runs(sort: &[ViewSortKey], cols: &RecordBatch) -> Result<Vec<SortRun>> {
	if sort.is_empty() {
		return Ok(Vec::new());
	}
	let mut rows: Vec<KeySerializer> = (0..cols.num_rows()).map(|_| KeySerializer::new()).collect();
	for key in sort {
		let index = key.column.0 as usize;
		let view = ColumnView::try_from((cols.column(index), cols.schema_ref().field(index)))?;
		extend_column_with_direction(&view, key.direction.clone().into(), &mut rows)?;
	}
	Ok(rows.into_iter().map(|row| SortRun::from_encoded(row.to_encoded_key())).collect())
}

#[inline]
pub fn sorted_view_key(storage: StorageId, run: Option<SortRun>, row: RowNumber) -> EncodedKey {
	match run {
		Some(run) => SortedViewRowKey::encoded(storage, run, row),
		None => row_key(storage, row),
	}
}

#[inline]
pub fn partitioned_key(storage: StorageId, run: Option<SortRun>, partition: Partition, row: RowNumber) -> EncodedKey {
	match run {
		Some(run) => PartitionedSortedViewRowKey::encoded(storage, partition, run, row),
		None => PartitionedRowKey::encoded(storage, partition, row),
	}
}
