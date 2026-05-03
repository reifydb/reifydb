// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{
	ColumnWithName, array::Column, buffer::ColumnBuffer, columns::Columns, mask::RowMask,
};
use reifydb_type::{
	Result,
	fragment::Fragment,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{
	compute,
	predicate::{self, Predicate},
	selection::Selection,
	snapshot::{ColumnBlock, ColumnChunks, Schema, Snapshot},
};

pub struct SnapshotReader {
	snapshot: Arc<Snapshot>,
	batch_size: usize,
	offset: usize,
	row_count: usize,
	predicate: Option<Predicate>,
}

impl SnapshotReader {
	pub fn new(snapshot: Arc<Snapshot>, batch_size: usize) -> Self {
		let row_count = snapshot.block.columns.first().map(|c| c.len()).unwrap_or(0);
		Self {
			snapshot,
			batch_size,
			offset: 0,
			row_count,
			predicate: None,
		}
	}

	pub fn with_predicate(mut self, predicate: Predicate) -> Self {
		self.predicate = Some(predicate);
		self
	}

	pub fn row_count(&self) -> usize {
		self.row_count
	}

	fn read_next_batch(&mut self) -> Result<Option<Columns>> {
		let start = self.offset;
		let end = (start + self.batch_size).min(self.row_count);
		self.offset = end;

		let block = &self.snapshot.block;
		let schema = &block.schema;

		let Some(predicate) = self.predicate.as_ref() else {
			return Ok(Some(materialize_full(block, start, end)?));
		};

		let view = block.view_range(start, end)?;
		let selection = predicate::evaluate(&view, predicate)?;
		match selection {
			Selection::None_ => Ok(None),
			Selection::All => Ok(Some(materialize_view_full(schema, &view, start, end)?)),
			Selection::Mask(mask) => Ok(Some(materialize_filtered(schema, &view, start, &mask)?)),
		}
	}
}

fn materialize_full(block: &ColumnBlock, start: usize, end: usize) -> Result<Columns> {
	let len = end - start;
	let mut columns: Vec<ColumnWithName> = Vec::with_capacity(block.schema.len());
	for (i, (name, _ty, _nullable)) in block.schema.iter().enumerate() {
		let data = read_range(&block.columns[i], start, end)?;
		columns.push(ColumnWithName::new(Fragment::internal(name.clone()), data));
	}
	let row_numbers: Vec<RowNumber> = (start..end).map(|i| RowNumber(i as u64)).collect();
	let ts = DateTime::default();
	Ok(Columns::with_system_columns(columns, row_numbers, vec![ts; len], vec![ts; len]))
}

fn materialize_view_full(schema: &Schema, view: &ColumnBlock, start: usize, end: usize) -> Result<Columns> {
	let len = end - start;
	let mut columns: Vec<ColumnWithName> = Vec::with_capacity(schema.len());
	for (i, (name, _ty, _nullable)) in schema.iter().enumerate() {
		let data = concat_view_chunks(&view.columns[i])?;
		columns.push(ColumnWithName::new(Fragment::internal(name.clone()), data));
	}
	let row_numbers: Vec<RowNumber> = (start..end).map(|i| RowNumber(i as u64)).collect();
	let ts = DateTime::default();
	Ok(Columns::with_system_columns(columns, row_numbers, vec![ts; len], vec![ts; len]))
}

fn materialize_filtered(schema: &Schema, view: &ColumnBlock, batch_start: usize, mask: &RowMask) -> Result<Columns> {
	let mut columns: Vec<ColumnWithName> = Vec::with_capacity(schema.len());
	for (i, (name, _ty, _nullable)) in schema.iter().enumerate() {
		let data = filter_view_column(&view.columns[i], mask)?;
		columns.push(ColumnWithName::new(Fragment::internal(name.clone()), data));
	}

	let kept = mask.popcount();
	let mut row_numbers: Vec<RowNumber> = Vec::with_capacity(kept);
	for i in 0..mask.len() {
		if mask.get(i) {
			row_numbers.push(RowNumber((batch_start + i) as u64));
		}
	}
	let ts = DateTime::default();
	Ok(Columns::with_system_columns(columns, row_numbers, vec![ts; kept], vec![ts; kept]))
}

fn filter_view_column(view_chunks: &ColumnChunks, mask: &RowMask) -> Result<ColumnBuffer> {
	let mut chunk_offset = 0usize;
	let mut out: Option<ColumnBuffer> = None;
	for chunk in &view_chunks.chunks {
		let chunk_len = chunk.len();
		let chunk_mask = mask.slice(chunk_offset, chunk_offset + chunk_len);
		chunk_offset += chunk_len;
		if chunk_mask.popcount() == 0 {
			continue;
		}
		let filtered: Column = compute::filter(chunk, &chunk_mask)?;
		let buf = filtered.to_canonical()?.to_column_buffer()?;
		match &mut out {
			None => out = Some(buf),
			Some(o) => o.extend(buf)?,
		}
	}
	Ok(out.expect("Selection::Mask guarantees at least one row survives"))
}

fn concat_view_chunks(view_chunks: &ColumnChunks) -> Result<ColumnBuffer> {
	let mut iter = view_chunks.chunks.iter();
	let first =
		iter.next().expect("concat_view_chunks called with empty chunks").to_canonical()?.to_column_buffer()?;
	let mut out = first;
	for chunk in iter {
		out.extend(chunk.to_canonical()?.to_column_buffer()?)?;
	}
	Ok(out)
}

fn read_range(column_chunks: &ColumnChunks, start: usize, end: usize) -> Result<ColumnBuffer> {
	let ranges = column_chunks.iter_range_chunks(start, end);
	let mut iter = ranges.into_iter();
	let (first_idx, first_s, first_e) = iter.next().expect("read_range called with empty range");
	let first = column_chunks.chunks[first_idx].slice(first_s, first_e)?.to_canonical()?.to_column_buffer()?;
	let mut out = first;
	for (idx, s, e) in iter {
		let buf = column_chunks.chunks[idx].slice(s, e)?.to_canonical()?.to_column_buffer()?;
		out.extend(buf)?;
	}
	Ok(out)
}

impl Iterator for SnapshotReader {
	type Item = Result<Columns>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if self.offset >= self.row_count {
				return None;
			}
			match self.read_next_batch() {
				Ok(Some(c)) => return Some(Ok(c)),
				Ok(None) => continue,
				Err(e) => return Some(Err(e)),
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::id::TableId,
		value::column::array::{Column, canonical::Canonical},
	};
	use reifydb_runtime::context::clock::Clock;
	use reifydb_type::value::r#type::Type;

	use super::*;
	use crate::snapshot::{ColumnBlock, ColumnChunks, SnapshotId, SnapshotSource};

	fn array_from_column_data(cd: &ColumnBuffer) -> Column {
		let ca = Canonical::from_column_buffer(cd).unwrap();
		Column::from_canonical(ca)
	}

	fn mk_snapshot(rows: usize) -> Arc<Snapshot> {
		let a_col = ColumnBuffer::int4((0..rows as i32).collect::<Vec<_>>());
		let b_col = ColumnBuffer::utf8((0..rows).map(|i| format!("row-{i}")).collect::<Vec<_>>());

		let chunked_a = ColumnChunks::single(Type::Int4, false, array_from_column_data(&a_col));
		let chunked_b = ColumnChunks::single(Type::Utf8, false, array_from_column_data(&b_col));

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
		assert!(reader.next().is_none());
	}

	#[test]
	fn reader_emits_batches_matching_batch_size() {
		let snap = mk_snapshot(5);
		let mut reader = SnapshotReader::new(snap, 2);

		let batch = reader.next().expect("first batch").unwrap();
		assert_eq!(batch.row_count(), 2);
		assert_eq!(batch.row_numbers[0], RowNumber(0));
		assert_eq!(batch.row_numbers[1], RowNumber(1));

		let a = batch.column("a").unwrap();
		assert_eq!(a.data().get_value(0).to_string(), "0");
		assert_eq!(a.data().get_value(1).to_string(), "1");

		let b = batch.column("b").unwrap();
		assert_eq!(b.data().get_value(0).to_string(), "row-0");

		let batch = reader.next().expect("second batch").unwrap();
		assert_eq!(batch.row_count(), 2);
		assert_eq!(batch.row_numbers[0], RowNumber(2));

		let batch = reader.next().expect("final partial batch").unwrap();
		assert_eq!(batch.row_count(), 1);
		assert_eq!(batch.row_numbers[0], RowNumber(4));
		assert_eq!(batch.column("a").unwrap().data().get_value(0).to_string(), "4");

		assert!(reader.next().is_none());
	}

	fn mk_chunked_snapshot(parts: &[&[i32]]) -> Arc<Snapshot> {
		let chunks: Vec<Column> =
			parts.iter().map(|p| array_from_column_data(&ColumnBuffer::int4(p.to_vec()))).collect();
		let chunked_a = ColumnChunks::new(Type::Int4, false, chunks);
		let schema = Arc::new(vec![("a".to_string(), Type::Int4, false)]);
		let block = ColumnBlock::new(schema, vec![chunked_a]);
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
	fn reader_handles_multi_chunk_column() {
		let snap = mk_chunked_snapshot(&[&[10, 20, 30], &[40, 50], &[60, 70, 80, 90]]);
		let mut reader = SnapshotReader::new(snap, 100);
		assert_eq!(reader.row_count(), 9);

		let batch = reader.next().unwrap().unwrap();
		assert_eq!(batch.row_count(), 9);
		let a = batch.column("a").unwrap();
		let actual: Vec<String> = (0..9).map(|i| a.data().get_value(i).to_string()).collect();
		assert_eq!(actual, vec!["10", "20", "30", "40", "50", "60", "70", "80", "90"]);
		assert!(reader.next().is_none());
	}

	#[test]
	fn reader_batch_spans_chunk_boundary() {
		// Chunks [0..3), [3..5), [5..9). Batch size 4 emits batches:
		//   [0..4) crosses chunk0->chunk1, [4..8) crosses chunk1->chunk2, [8..9) tail.
		let snap = mk_chunked_snapshot(&[&[10, 20, 30], &[40, 50], &[60, 70, 80, 90]]);
		let mut reader = SnapshotReader::new(snap, 4);

		let b0 = reader.next().unwrap().unwrap();
		assert_eq!(b0.row_count(), 4);
		let a = b0.column("a").unwrap();
		let v0: Vec<String> = (0..4).map(|i| a.data().get_value(i).to_string()).collect();
		assert_eq!(v0, vec!["10", "20", "30", "40"]);

		let b1 = reader.next().unwrap().unwrap();
		assert_eq!(b1.row_count(), 4);
		let a = b1.column("a").unwrap();
		let v1: Vec<String> = (0..4).map(|i| a.data().get_value(i).to_string()).collect();
		assert_eq!(v1, vec!["50", "60", "70", "80"]);

		let b2 = reader.next().unwrap().unwrap();
		assert_eq!(b2.row_count(), 1);
		assert_eq!(b2.column("a").unwrap().data().get_value(0).to_string(), "90");
		assert!(reader.next().is_none());
	}

	#[test]
	fn reader_batch_starts_mid_chunk() {
		// One chunk of length 10, batch size 3 means batches start mid-chunk.
		let snap = mk_chunked_snapshot(&[&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);
		let mut reader = SnapshotReader::new(snap, 3);

		let b0 = reader.next().unwrap().unwrap();
		assert_eq!(b0.row_count(), 3);
		let b1 = reader.next().unwrap().unwrap();
		assert_eq!(b1.row_count(), 3);
		let a = b1.column("a").unwrap();
		assert_eq!(a.data().get_value(0).to_string(), "4");
		assert_eq!(a.data().get_value(2).to_string(), "6");
	}

	use reifydb_type::value::Value;

	use crate::predicate::{ColRef, Predicate};

	#[test]
	fn pushdown_eq_predicate_keeps_only_matching_rows() {
		// Single chunk: id values 0..5; predicate id == 3 keeps row 3 only.
		let snap = mk_snapshot(5);
		let p = Predicate::Eq(ColRef::from("a"), Value::Int4(3));
		let mut reader = SnapshotReader::new(snap, 100).with_predicate(p);

		let batch = reader.next().expect("batch").unwrap();
		assert_eq!(batch.row_count(), 1);
		assert_eq!(batch.row_numbers[0], RowNumber(3));
		assert_eq!(batch.column("a").unwrap().data().get_value(0).to_string(), "3");
		assert_eq!(batch.column("b").unwrap().data().get_value(0).to_string(), "row-3");
		assert!(reader.next().is_none());
	}

	#[test]
	fn pushdown_filters_across_chunk_boundary() {
		// 3 chunks: [10,20,30] | [40,50] | [60,70,80,90]. Predicate keeps anything
		// equal to 30 (chunk 0) or 80 (chunk 2). Reader processes the whole snapshot
		// in one batch (batch_size=100) so the filter spans every chunk.
		let snap = mk_chunked_snapshot(&[&[10, 20, 30], &[40, 50], &[60, 70, 80, 90]]);
		let p = Predicate::In(ColRef::from("a"), vec![Value::Int4(30), Value::Int4(80)]);
		let mut reader = SnapshotReader::new(snap, 100).with_predicate(p);

		let batch = reader.next().expect("batch").unwrap();
		assert_eq!(batch.row_count(), 2);
		let a = batch.column("a").unwrap();
		assert_eq!(a.data().get_value(0).to_string(), "30");
		assert_eq!(a.data().get_value(1).to_string(), "80");
		assert_eq!(batch.row_numbers[0], RowNumber(2));
		assert_eq!(batch.row_numbers[1], RowNumber(7));
		assert!(reader.next().is_none());
	}

	#[test]
	fn pushdown_skips_empty_batches() {
		// 6 rows, batch size 2 → batches [0..2), [2..4), [4..6). Predicate id==4 only
		// matches in batch [4..6); the first two batches return Selection::None_ and
		// must be skipped by the iterator (consumer never sees them).
		let snap = mk_snapshot(6);
		let p = Predicate::Eq(ColRef::from("a"), Value::Int4(4));
		let mut reader = SnapshotReader::new(snap, 2).with_predicate(p);

		let batch = reader.next().expect("only matching batch").unwrap();
		assert_eq!(batch.row_count(), 1);
		assert_eq!(batch.row_numbers[0], RowNumber(4));
		assert_eq!(batch.column("a").unwrap().data().get_value(0).to_string(), "4");
		assert!(reader.next().is_none());
	}

	#[test]
	fn pushdown_selection_all_passes_batch_through() {
		// Predicate matches every row in the batch → Selection::All path. Output
		// must equal the no-predicate batch.
		let snap = mk_snapshot(5);
		let p = Predicate::GtEq(ColRef::from("a"), Value::Int4(0));
		let mut reader = SnapshotReader::new(snap, 100).with_predicate(p);

		let batch = reader.next().expect("batch").unwrap();
		assert_eq!(batch.row_count(), 5);
		let a = batch.column("a").unwrap();
		let vals: Vec<String> = (0..5).map(|i| a.data().get_value(i).to_string()).collect();
		assert_eq!(vals, vec!["0", "1", "2", "3", "4"]);
		assert_eq!(batch.row_numbers[0], RowNumber(0));
		assert_eq!(batch.row_numbers[4], RowNumber(4));
	}

	#[test]
	fn pushdown_is_none_over_multi_chunk_nullable() {
		// Two nullable chunks; nones at position 1 of each chunk → block rows 1, 4.
		let mut a = ColumnBuffer::int4_with_capacity(3);
		a.push::<i32>(10);
		a.push_none();
		a.push::<i32>(30);
		let mut b = ColumnBuffer::int4_with_capacity(3);
		b.push::<i32>(40);
		b.push_none();
		b.push::<i32>(60);
		let chunks = vec![array_from_column_data(&a), array_from_column_data(&b)];
		let id_col = ColumnChunks::new(Type::Int4, true, chunks);
		let schema = Arc::new(vec![("a".to_string(), Type::Int4, true)]);
		let block = ColumnBlock::new(schema, vec![id_col]);
		let now = Clock::Real.instant();
		let snap = Arc::new(Snapshot {
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
		});

		let p = Predicate::IsNone(ColRef::from("a"));
		let mut reader = SnapshotReader::new(snap, 100).with_predicate(p);

		let batch = reader.next().expect("batch").unwrap();
		assert_eq!(batch.row_count(), 2);
		assert_eq!(batch.row_numbers[0], RowNumber(1));
		assert_eq!(batch.row_numbers[1], RowNumber(4));
	}
}
