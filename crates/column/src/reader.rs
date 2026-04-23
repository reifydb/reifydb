// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::{
	Result,
	fragment::Fragment,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{error::ColumnError, snapshot::Snapshot};

pub struct SnapshotReader {
	snapshot: Arc<Snapshot>,
	batch_size: usize,
	offset: usize,
	row_count: usize,
}

impl SnapshotReader {
	pub fn new(snapshot: Arc<Snapshot>, batch_size: usize) -> Self {
		let row_count = snapshot
			.block
			.columns
			.first()
			.map(|c| c.chunks.iter().map(|a| a.len()).sum::<usize>())
			.unwrap_or(0);
		Self {
			snapshot,
			batch_size,
			offset: 0,
			row_count,
		}
	}

	pub fn row_count(&self) -> usize {
		self.row_count
	}

	pub fn next(&mut self) -> Result<Option<Columns>> {
		if self.offset >= self.row_count {
			return Ok(None);
		}
		let end = (self.offset + self.batch_size).min(self.row_count);
		let len = end - self.offset;

		let mut columns: Vec<Column> = Vec::with_capacity(self.snapshot.block.schema.len());
		for (i, (name, _ty, _nullable)) in self.snapshot.block.schema.iter().enumerate() {
			let chunked = &self.snapshot.block.columns[i];
			if chunked.chunks.len() != 1 {
				return Err(ColumnError::MultiChunkUnsupported {
					operation: "SnapshotReader",
					chunk_count: chunked.chunks.len(),
				}
				.into());
			}
			let canonical = chunked.chunks[0].to_canonical()?;
			let sliced = canonical.slice(self.offset, end)?;
			let data: ColumnData = sliced.to_column_data()?;
			columns.push(Column::new(Fragment::internal(name.clone()), data));
		}

		let row_numbers: Vec<RowNumber> = (self.offset..end).map(|i| RowNumber(i as u64)).collect();
		let ts = DateTime::default();
		let created_at = vec![ts; len];
		let updated_at = vec![ts; len];

		self.offset = end;

		Ok(Some(Columns::with_system_columns(columns, row_numbers, created_at, updated_at)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{common::CommitVersion, interface::catalog::id::TableId};
	use reifydb_runtime::context::clock::Clock;
	use reifydb_type::value::r#type::Type;

	use super::*;
	use crate::{
		array::{Array, canonical::CanonicalArray},
		chunked::ChunkedArray,
		column_block::ColumnBlock,
		snapshot::{SnapshotId, SnapshotSource},
	};

	fn array_from_column_data(cd: &ColumnData) -> Array {
		let ca = CanonicalArray::from_column_data(cd).unwrap();
		Array::from_canonical(ca)
	}

	fn mk_snapshot(rows: usize) -> Arc<Snapshot> {
		let a_col = ColumnData::int4((0..rows as i32).collect::<Vec<_>>());
		let b_col = ColumnData::utf8((0..rows).map(|i| format!("row-{i}")).collect::<Vec<_>>());

		let chunked_a = ChunkedArray::single(Type::Int4, false, array_from_column_data(&a_col));
		let chunked_b = ChunkedArray::single(Type::Utf8, false, array_from_column_data(&b_col));

		let schema = Arc::new(vec![("a".to_string(), Type::Int4, false), ("b".to_string(), Type::Utf8, false)]);
		let block = ColumnBlock::new(schema, vec![chunked_a, chunked_b]);

		let now = Clock::Real.instant();
		Arc::new(Snapshot {
			id: SnapshotId::Table {
				table_id: TableId(1),
				commit_version: CommitVersion(1),
			},
			source: SnapshotSource::Table {
				table_id: TableId(1),
				commit_version: CommitVersion(1),
			},
			namespace: "test".to_string(),
			name: "t".to_string(),
			created_at: now,
			block,
		})
	}

	#[test]
	fn reader_returns_none_for_empty_snapshot() {
		let snap = mk_snapshot(0);
		let mut reader = SnapshotReader::new(snap, 4);
		assert!(reader.next().unwrap().is_none());
	}

	#[test]
	fn reader_emits_batches_matching_batch_size() {
		let snap = mk_snapshot(5);
		let mut reader = SnapshotReader::new(snap, 2);

		let batch = reader.next().unwrap().expect("first batch");
		assert_eq!(batch.row_count(), 2);
		assert_eq!(batch.row_numbers[0], RowNumber(0));
		assert_eq!(batch.row_numbers[1], RowNumber(1));

		let a = batch.column("a").unwrap();
		assert_eq!(a.data().get_value(0).to_string(), "0");
		assert_eq!(a.data().get_value(1).to_string(), "1");

		let b = batch.column("b").unwrap();
		assert_eq!(b.data().get_value(0).to_string(), "row-0");

		let batch = reader.next().unwrap().expect("second batch");
		assert_eq!(batch.row_count(), 2);
		assert_eq!(batch.row_numbers[0], RowNumber(2));

		let batch = reader.next().unwrap().expect("final partial batch");
		assert_eq!(batch.row_count(), 1);
		assert_eq!(batch.row_numbers[0], RowNumber(4));
		assert_eq!(batch.column("a").unwrap().data().get_value(0).to_string(), "4");

		assert!(reader.next().unwrap().is_none());
	}
}
