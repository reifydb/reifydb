// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::FieldRef;
use reifydb_core::value::{
	batch::{batch, concat_columns},
	column::data::Column,
};
use reifydb_value::{
	Result, reifydb_assertions,
	util::bitmap,
	value::system_columns::{require_created_at, require_row_numbers, require_updated_at},
};

use crate::{
	compute,
	predicate::{self, Predicate},
	selection::Selection,
	snapshot::{ColumnBlock, ColumnChunks, Schema},
};

pub struct SnapshotReader {
	block: Arc<ColumnBlock>,
	batch_size: usize,
	offset: usize,
	row_count: usize,
	predicate: Option<Predicate>,
}

impl SnapshotReader {
	pub fn new(block: Arc<ColumnBlock>, batch_size: usize) -> Self {
		let row_count = block.columns.first().map(|c| c.len()).unwrap_or(0);
		Self {
			block,
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

	fn read_next_batch(&mut self) -> Result<Option<RecordBatch>> {
		let (start, end) = self.advance_batch_window();
		let block = self.block.as_ref();

		let Some(predicate) = self.predicate.as_ref() else {
			return Ok(Some(materialize_full(block, start, end)?));
		};

		evaluate_and_materialize(block, predicate, start, end)
	}

	#[inline]
	fn advance_batch_window(&mut self) -> (usize, usize) {
		let start = self.offset;
		let end = (start + self.batch_size).min(self.row_count);
		self.offset = end;
		reifydb_assertions! {
			let row_count = self.row_count;
			assert!(
				start < end && end <= row_count,
				"read_next_batch produced an empty or out-of-bounds window, so a batch would \
				 materialize zero rows while the iterator's offset>=row_count guard still treats \
				 the reader as live, looping without progress (start={start}, end={end}, row_count={row_count})"
			);
		}
		(start, end)
	}
}

#[inline]
fn evaluate_and_materialize(
	block: &ColumnBlock,
	predicate: &Predicate,
	start: usize,
	end: usize,
) -> Result<Option<RecordBatch>> {
	let schema = &block.schema;
	let view = block.view_range(start, end)?;
	let selection = predicate::evaluate(&view, predicate)?;
	match selection {
		Selection::None_ => Ok(None),
		Selection::All => Ok(Some(materialize_view_full(schema, &view, start, end)?)),
		Selection::Mask(mask) => Ok(Some(materialize_filtered(schema, &view, start, &mask)?)),
	}
}

fn materialize(
	schema: &Schema,
	mut fetch: impl FnMut(&str, usize) -> Result<(FieldRef, ArrayRef)>,
) -> Result<RecordBatch> {
	let mut columns: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(schema.len());
	for (i, (name, _ty, _nullable)) in schema.iter().enumerate() {
		columns.push(fetch(name, i)?);
	}
	let materialized = batch(columns)?;
	require_row_numbers(&materialized)?;
	require_created_at(&materialized)?;
	require_updated_at(&materialized)?;
	Ok(materialized)
}

fn materialize_full(block: &ColumnBlock, start: usize, end: usize) -> Result<RecordBatch> {
	materialize(&block.schema, |name, i| read_range(name, &block.columns[i], start, end))
}

fn materialize_view_full(schema: &Schema, view: &ColumnBlock, _start: usize, _end: usize) -> Result<RecordBatch> {
	materialize(schema, |name, i| concat_view_chunks(name, &view.columns[i]))
}

fn materialize_filtered(
	schema: &Schema,
	view: &ColumnBlock,
	_batch_start: usize,
	mask: &BooleanBuffer,
) -> Result<RecordBatch> {
	materialize(schema, |name, i| filter_view_column(name, &view.columns[i], mask))
}

fn filter_view_column(name: &str, view_chunks: &ColumnChunks, mask: &BooleanBuffer) -> Result<(FieldRef, ArrayRef)> {
	let mut chunk_offset = 0usize;
	let mut out: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(view_chunks.chunks.len());
	for chunk in &view_chunks.chunks {
		let chunk_len = chunk.len();
		let chunk_end = chunk_offset + chunk_len;
		assert!(chunk_end <= mask.len(), "filter_view_column: mask end {chunk_end} > len {}", mask.len());
		let chunk_mask = bitmap::slice(mask, chunk_offset, chunk_end);
		chunk_offset += chunk_len;
		if chunk_mask.count_set_bits() == 0 {
			continue;
		}
		let filtered: Column = compute::filter(chunk, &chunk_mask)?;
		out.push(filtered.to_canonical()?.to_column(name));
	}
	assert!(!out.is_empty(), "Selection::Mask guarantees at least one row survives");
	concat_columns(&out)
}

fn concat_view_chunks(name: &str, view_chunks: &ColumnChunks) -> Result<(FieldRef, ArrayRef)> {
	assert!(!view_chunks.chunks.is_empty(), "concat_view_chunks called with empty chunks");
	let mut out: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(view_chunks.chunks.len());
	for chunk in &view_chunks.chunks {
		out.push(chunk.to_canonical()?.to_column(name));
	}
	concat_columns(&out)
}

fn read_range(name: &str, column_chunks: &ColumnChunks, start: usize, end: usize) -> Result<(FieldRef, ArrayRef)> {
	let ranges = column_chunks.iter_range_chunks(start, end);
	assert!(!ranges.is_empty(), "read_range called with empty range");
	let mut out: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(ranges.len());
	for (idx, s, e) in ranges {
		out.push(column_chunks.chunks[idx].slice(s, e)?.to_canonical()?.to_column(name));
	}
	concat_columns(&out)
}

impl Iterator for SnapshotReader {
	type Item = Result<RecordBatch>;

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
	use reifydb_core::value::column::{
		builder::ColumnBuilder,
		data::{Column, canonical::Canonical},
		factory,
	};
	use reifydb_value::value::{
		datetime::DateTime,
		row_number::RowNumber,
		system_columns::{SystemColumn, column_view, row_numbers},
		value_type::ValueType,
	};

	use super::*;
	use crate::snapshot::{ColumnBlock, ColumnChunks};

	fn array_from_column_data(cd: &(FieldRef, ArrayRef)) -> Column {
		let ca = Canonical::from_column(cd).unwrap();
		Column::from_canonical(ca)
	}

	fn system_chunked(rows: usize) -> Vec<((String, ValueType, bool), ColumnChunks)> {
		let row_numbers = factory::uint8(SystemColumn::RowNumbers.name(), (0..rows as u64).collect::<Vec<_>>());
		let ts = factory::datetime("ts", vec![DateTime::default(); rows]);
		let row_number_chunk =
			ColumnChunks::single(ValueType::Uint8, false, array_from_column_data(&row_numbers));
		let created_chunk = ColumnChunks::single(ValueType::DateTime, false, array_from_column_data(&ts));
		let updated_chunk = ColumnChunks::single(ValueType::DateTime, false, array_from_column_data(&ts));
		let time_chunk = ColumnChunks::single(ValueType::DateTime, false, array_from_column_data(&ts));
		vec![
			((SystemColumn::RowNumbers.name().to_string(), ValueType::Uint8, false), row_number_chunk),
			((SystemColumn::CreatedAt.name().to_string(), ValueType::DateTime, false), created_chunk),
			((SystemColumn::UpdatedAt.name().to_string(), ValueType::DateTime, false), updated_chunk),
			((SystemColumn::Time.name().to_string(), ValueType::DateTime, false), time_chunk),
		]
	}

	fn mk_block(rows: usize) -> Arc<ColumnBlock> {
		let a_col = factory::int4("a", (0..rows as i32).collect::<Vec<_>>());
		let b_col = factory::utf8("b", (0..rows).map(|i| format!("row-{i}")).collect::<Vec<_>>());

		let chunked_a = ColumnChunks::single(ValueType::Int4, false, array_from_column_data(&a_col));
		let chunked_b = ColumnChunks::single(ValueType::Utf8, false, array_from_column_data(&b_col));

		let mut schema_entries: Vec<(String, ValueType, bool)> =
			vec![("a".to_string(), ValueType::Int4, false), ("b".to_string(), ValueType::Utf8, false)];
		let mut chunks: Vec<ColumnChunks> = vec![chunked_a, chunked_b];
		for (entry, chunk) in system_chunked(rows) {
			schema_entries.push(entry);
			chunks.push(chunk);
		}
		Arc::new(ColumnBlock::new(Arc::new(schema_entries), chunks))
	}

	#[test]
	fn reader_returns_none_for_empty_snapshot() {
		let snap = mk_block(0);
		let mut reader = SnapshotReader::new(snap, 4);
		assert!(reader.next().is_none());
	}

	#[test]
	fn reader_emits_batches_matching_batch_size() {
		let snap = mk_block(5);
		let mut reader = SnapshotReader::new(snap, 2);

		let batch = reader.next().expect("first batch").unwrap();
		assert_eq!(batch.num_rows(), 2);
		assert_eq!(row_numbers(&batch).unwrap()[0], RowNumber(0));
		assert_eq!(row_numbers(&batch).unwrap()[1], RowNumber(1));

		let a = column_view(&batch, "a").unwrap().unwrap();
		assert_eq!(a.get_value(0).to_string(), "0");
		assert_eq!(a.get_value(1).to_string(), "1");

		let b = column_view(&batch, "b").unwrap().unwrap();
		assert_eq!(b.get_value(0).to_string(), "row-0");

		let batch = reader.next().expect("second batch").unwrap();
		assert_eq!(batch.num_rows(), 2);
		assert_eq!(row_numbers(&batch).unwrap()[0], RowNumber(2));

		let batch = reader.next().expect("final partial batch").unwrap();
		assert_eq!(batch.num_rows(), 1);
		assert_eq!(row_numbers(&batch).unwrap()[0], RowNumber(4));
		assert_eq!(column_view(&batch, "a").unwrap().unwrap().get_value(0).to_string(), "4");

		assert!(reader.next().is_none());
	}

	fn mk_chunked_block(parts: &[&[i32]]) -> Arc<ColumnBlock> {
		let total_rows: usize = parts.iter().map(|p| p.len()).sum();
		let chunks: Vec<Column> =
			parts.iter().map(|p| array_from_column_data(&factory::int4("a", p.to_vec()))).collect();
		let chunked_a = ColumnChunks::new(ValueType::Int4, false, chunks);
		let mut schema_entries: Vec<(String, ValueType, bool)> =
			vec![("a".to_string(), ValueType::Int4, false)];
		let mut all_chunks: Vec<ColumnChunks> = vec![chunked_a];
		for (entry, chunk) in system_chunked(total_rows) {
			schema_entries.push(entry);
			all_chunks.push(chunk);
		}
		Arc::new(ColumnBlock::new(Arc::new(schema_entries), all_chunks))
	}

	#[test]
	fn reader_handles_multi_chunk_column() {
		let snap = mk_chunked_block(&[&[10, 20, 30], &[40, 50], &[60, 70, 80, 90]]);
		assert_eq!(snap.len(), 9);
		let mut reader = SnapshotReader::new(snap, 100);

		let batch = reader.next().unwrap().unwrap();
		assert_eq!(batch.num_rows(), 9);
		let a = column_view(&batch, "a").unwrap().unwrap();
		let actual: Vec<String> = (0..9).map(|i| a.get_value(i).to_string()).collect();
		assert_eq!(actual, vec!["10", "20", "30", "40", "50", "60", "70", "80", "90"]);
		assert!(reader.next().is_none());
	}

	#[test]
	fn reader_batch_spans_chunk_boundary() {
		// Batch size 4 lands on no chunk boundary, so the first two batches each have to stitch two chunks
		// together and the third is a short tail.
		let snap = mk_chunked_block(&[&[10, 20, 30], &[40, 50], &[60, 70, 80, 90]]);
		let mut reader = SnapshotReader::new(snap, 4);

		let b0 = reader.next().unwrap().unwrap();
		assert_eq!(b0.num_rows(), 4);
		let a = column_view(&b0, "a").unwrap().unwrap();
		let v0: Vec<String> = (0..4).map(|i| a.get_value(i).to_string()).collect();
		assert_eq!(v0, vec!["10", "20", "30", "40"]);

		let b1 = reader.next().unwrap().unwrap();
		assert_eq!(b1.num_rows(), 4);
		let a = column_view(&b1, "a").unwrap().unwrap();
		let v1: Vec<String> = (0..4).map(|i| a.get_value(i).to_string()).collect();
		assert_eq!(v1, vec!["50", "60", "70", "80"]);

		let b2 = reader.next().unwrap().unwrap();
		assert_eq!(b2.num_rows(), 1);
		assert_eq!(column_view(&b2, "a").unwrap().unwrap().get_value(0).to_string(), "90");
		assert!(reader.next().is_none());
	}

	#[test]
	fn reader_batch_starts_mid_chunk() {
		// Batch size 3 over a single 10-row chunk makes every batch after the first start mid-chunk.
		let snap = mk_chunked_block(&[&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);
		let mut reader = SnapshotReader::new(snap, 3);

		let b0 = reader.next().unwrap().unwrap();
		assert_eq!(b0.num_rows(), 3);
		let b1 = reader.next().unwrap().unwrap();
		assert_eq!(b1.num_rows(), 3);
		let a = column_view(&b1, "a").unwrap().unwrap();
		assert_eq!(a.get_value(0).to_string(), "4");
		assert_eq!(a.get_value(2).to_string(), "6");
	}

	use reifydb_value::value::Value;

	use crate::predicate::{ColRef, Predicate};

	#[test]
	fn pushdown_eq_predicate_keeps_only_matching_rows() {
		let snap = mk_block(5);
		let p = Predicate::Eq(ColRef::from("a"), Value::Int4(3));
		let mut reader = SnapshotReader::new(snap, 100).with_predicate(p);

		let batch = reader.next().expect("batch").unwrap();
		assert_eq!(batch.num_rows(), 1);
		assert_eq!(row_numbers(&batch).unwrap()[0], RowNumber(3));
		assert_eq!(column_view(&batch, "a").unwrap().unwrap().get_value(0).to_string(), "3");
		assert_eq!(column_view(&batch, "b").unwrap().unwrap().get_value(0).to_string(), "row-3");
		assert!(reader.next().is_none());
	}

	#[test]
	fn pushdown_filters_across_chunk_boundary() {
		// The whole snapshot arrives as one batch, so the filter has to select across chunk boundaries.
		let snap = mk_chunked_block(&[&[10, 20, 30], &[40, 50], &[60, 70, 80, 90]]);
		let p = Predicate::In(ColRef::from("a"), vec![Value::Int4(30), Value::Int4(80)]);
		let mut reader = SnapshotReader::new(snap, 100).with_predicate(p);

		let batch = reader.next().expect("batch").unwrap();
		assert_eq!(batch.num_rows(), 2);
		let a = column_view(&batch, "a").unwrap().unwrap();
		assert_eq!(a.get_value(0).to_string(), "30");
		assert_eq!(a.get_value(1).to_string(), "80");
		assert_eq!(row_numbers(&batch).unwrap()[0], RowNumber(2));
		assert_eq!(row_numbers(&batch).unwrap()[1], RowNumber(7));
		assert!(reader.next().is_none());
	}

	#[test]
	fn pushdown_skips_empty_batches() {
		// The first two batches select nothing; the iterator must skip them rather than hand back empty
		// batches.
		let snap = mk_block(6);
		let p = Predicate::Eq(ColRef::from("a"), Value::Int4(4));
		let mut reader = SnapshotReader::new(snap, 2).with_predicate(p);

		let batch = reader.next().expect("only matching batch").unwrap();
		assert_eq!(batch.num_rows(), 1);
		assert_eq!(row_numbers(&batch).unwrap()[0], RowNumber(4));
		assert_eq!(column_view(&batch, "a").unwrap().unwrap().get_value(0).to_string(), "4");
		assert!(reader.next().is_none());
	}

	#[test]
	fn pushdown_selection_all_passes_batch_through() {
		// A predicate matching every row takes the Selection::All path, which must pass the batch through
		// intact.
		let snap = mk_block(5);
		let p = Predicate::GtEq(ColRef::from("a"), Value::Int4(0));
		let mut reader = SnapshotReader::new(snap, 100).with_predicate(p);

		let batch = reader.next().expect("batch").unwrap();
		assert_eq!(batch.num_rows(), 5);
		let a = column_view(&batch, "a").unwrap().unwrap();
		let vals: Vec<String> = (0..5).map(|i| a.get_value(i).to_string()).collect();
		assert_eq!(vals, vec!["0", "1", "2", "3", "4"]);
		assert_eq!(row_numbers(&batch).unwrap()[0], RowNumber(0));
		assert_eq!(row_numbers(&batch).unwrap()[4], RowNumber(4));
	}

	#[test]
	fn pushdown_is_none_over_multi_chunk_nullable() {
		// A none at position 1 of each chunk has to resolve to block rows 1 and 4, not chunk-local 1 twice.
		let mut a = ColumnBuilder::with_capacity(ValueType::Int4, 3);
		a.push::<i32>(10);
		a.push_none();
		a.push::<i32>(30);
		let a = a.finish("a");
		let mut b = ColumnBuilder::with_capacity(ValueType::Int4, 3);
		b.push::<i32>(40);
		b.push_none();
		b.push::<i32>(60);
		let b = b.finish("a");
		let chunks = vec![array_from_column_data(&a), array_from_column_data(&b)];
		let id_col = ColumnChunks::new(ValueType::Int4, true, chunks);
		let mut schema_entries: Vec<(String, ValueType, bool)> = vec![("a".to_string(), ValueType::Int4, true)];
		let mut block_chunks: Vec<ColumnChunks> = vec![id_col];
		for (entry, chunk) in system_chunked(6) {
			schema_entries.push(entry);
			block_chunks.push(chunk);
		}
		let block = Arc::new(ColumnBlock::new(Arc::new(schema_entries), block_chunks));

		let p = Predicate::IsNone(ColRef::from("a"));
		let mut reader = SnapshotReader::new(block, 100).with_predicate(p);

		let batch = reader.next().expect("batch").unwrap();
		assert_eq!(batch.num_rows(), 2);
		assert_eq!(row_numbers(&batch).unwrap()[0], RowNumber(1));
		assert_eq!(row_numbers(&batch).unwrap()[1], RowNumber(4));
	}

	#[test]
	fn pushdown_mask_over_a_mid_chunk_window_keeps_rows_and_nones_aligned() {
		// Batch 2 starts mid-chunk: an (offset, len) chunk mask, not (start, end), panics or keeps wrong rows.
		let first = factory::int4_optional(
			"a",
			[
				Some(0),
				None,
				Some(20),
				Some(30),
				None,
				Some(50),
				Some(60),
				Some(70),
				None,
				Some(90),
				Some(100),
			],
		);
		let second = factory::int4_optional("a", [Some(110), None, Some(130), Some(140), None, Some(160)]);
		let chunks = vec![array_from_column_data(&first), array_from_column_data(&second)];
		let a_col = ColumnChunks::new(ValueType::Int4, true, chunks);
		let mut schema_entries: Vec<(String, ValueType, bool)> = vec![("a".to_string(), ValueType::Int4, true)];
		let mut block_chunks: Vec<ColumnChunks> = vec![a_col];
		for (entry, chunk) in system_chunked(17) {
			schema_entries.push(entry);
			block_chunks.push(chunk);
		}
		let block = Arc::new(ColumnBlock::new(Arc::new(schema_entries), block_chunks));

		let p = Predicate::Or(vec![
			Predicate::IsNone(ColRef::from("a")),
			Predicate::Gt(ColRef::from("a"), Value::Int4(125)),
		]);
		let reader = SnapshotReader::new(block, 8).with_predicate(p);

		let mut rows = Vec::new();
		for batch in reader {
			let batch = batch.unwrap();
			let a = column_view(&batch, "a").unwrap().unwrap();
			for i in 0..batch.num_rows() {
				rows.push((row_numbers(&batch).unwrap()[i], a.get_value(i)));
			}
		}
		let none = || Value::none_of(ValueType::Int4);
		assert_eq!(
			rows,
			vec![
				(RowNumber(1), none()),
				(RowNumber(4), none()),
				(RowNumber(8), none()),
				(RowNumber(12), none()),
				(RowNumber(13), Value::Int4(130)),
				(RowNumber(14), Value::Int4(140)),
				(RowNumber(15), none()),
				(RowNumber(16), Value::Int4(160)),
			]
		);
	}
}
