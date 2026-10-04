// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_core::value::column::factory;
use reifydb_store_column::{
	compress::Compressor,
	reader::SnapshotReader,
	session::new_session,
	snapshot::{ColumnBlock, ColumnChunks},
};
use reifydb_value::value::{
	Value,
	column_view::{ColumnView, ViewData},
	datetime::DateTime,
	row_number::RowNumber,
	system_columns::{self, SystemColumn, column_view, user_columns},
	value_type::ValueType,
};

const ROWS: usize = 10_007;
const BATCH: usize = 1_024;

fn row_number(i: usize) -> u64 {
	i as u64 * 3 + 1
}

fn created_at(i: usize) -> DateTime {
	DateTime::from_nanos(1_700_000_000_000_000_000 + i as i64 * 1_000)
}

fn updated_at(i: usize) -> DateTime {
	DateTime::from_nanos(1_700_000_000_000_000_000 + i as i64 * 1_000 + 500)
}

fn time(i: usize) -> DateTime {
	DateTime::from_nanos(1_600_000_000_000_000_000 + i as i64 * 7)
}

fn commit_version(i: usize) -> u64 {
	10_000 + i as u64 / 3
}

fn value(i: usize) -> i32 {
	i as i32 * 17 - 50_000
}

fn label(i: usize) -> String {
	format!("row-{i}-{}", "q".repeat(i % 7))
}

fn defined(i: usize) -> bool {
	i % 4 != 3
}

fn maybe(i: usize) -> Value {
	if defined(i) {
		Value::Int4(-value(i))
	} else {
		Value::None {
			inner: ValueType::Int4,
		}
	}
}

fn int4_slice<'a>(view: &ColumnView<'a>) -> &'a [i32] {
	match view.data {
		ViewData::Int4(container) => &container.values()[..],
		_ => panic!("expected an int4 buffer, got {:?}", view.get_type()),
	}
}

fn schema() -> Vec<(String, ValueType, bool)> {
	vec![
		(SystemColumn::RowNumbers.name().to_string(), ValueType::Uint8, false),
		("value".to_string(), ValueType::Int4, false),
		(SystemColumn::CreatedAt.name().to_string(), ValueType::DateTime, false),
		("label".to_string(), ValueType::Utf8, false),
		(SystemColumn::UpdatedAt.name().to_string(), ValueType::DateTime, false),
		("maybe".to_string(), ValueType::Int4, true),
		(SystemColumn::Time.name().to_string(), ValueType::DateTime, false),
		(SystemColumn::CommitVersion.name().to_string(), ValueType::Uint8, false),
	]
}

fn fixture(bounds: &[usize]) -> Arc<ColumnBlock> {
	let schema = schema();
	let compressor = Compressor::new(new_session());
	let mut per_column: Vec<Option<ColumnChunks>> = vec![None; schema.len()];
	for window in bounds.windows(2) {
		let rows = window[0]..window[1];
		let buffers = [
			factory::uint8(SystemColumn::RowNumbers.name(), rows.clone().map(row_number)),
			factory::int4("value", rows.clone().map(value)),
			factory::datetime(SystemColumn::CreatedAt.name(), rows.clone().map(created_at)),
			factory::utf8("label", rows.clone().map(label)),
			factory::datetime(SystemColumn::UpdatedAt.name(), rows.clone().map(updated_at)),
			factory::int4_with_bitvec(
				"maybe",
				rows.clone().map(|i| -value(i)),
				rows.clone().map(defined).collect::<Vec<_>>(),
			),
			factory::datetime(SystemColumn::Time.name(), rows.clone().map(time)),
			factory::uint8(SystemColumn::CommitVersion.name(), rows.clone().map(commit_version)),
		];
		for ((slot, buffer), (_, ty, _)) in per_column.iter_mut().zip(buffers).zip(&schema) {
			let compressed = compressor.compress(ty.clone(), &buffer).unwrap();
			match slot {
				None => *slot = Some(compressed),
				Some(column) => column.chunks.extend(compressed.chunks),
			}
		}
	}
	let columns = per_column.into_iter().map(|c| c.expect("fixture has rows")).collect();
	Arc::new(ColumnBlock::new(Arc::new(schema), columns))
}

#[track_caller]
fn assert_batch_rows(batch: &RecordBatch, start: usize, end: usize) {
	let rows = start..end;
	assert_eq!(batch.num_rows(), end - start, "batch {start}..{end}");
	assert_eq!(user_columns(batch).count(), 3, "system columns must never surface as user columns");
	let expected: Vec<RowNumber> = rows.clone().map(|i| RowNumber(row_number(i))).collect();
	assert_eq!(system_columns::row_numbers(batch).unwrap(), &expected[..], "row numbers {start}..{end}");
	assert_eq!(system_columns::created_at(batch).unwrap(), &rows.clone().map(created_at).collect::<Vec<_>>()[..]);
	assert_eq!(system_columns::updated_at(batch).unwrap(), &rows.clone().map(updated_at).collect::<Vec<_>>()[..]);
	assert_eq!(system_columns::time(batch).unwrap(), &rows.clone().map(time).collect::<Vec<_>>()[..]);
	assert_eq!(
		system_columns::commit_versions(batch).unwrap(),
		&rows.clone().map(commit_version).collect::<Vec<_>>()[..]
	);

	let values = column_view(batch, "value").unwrap().expect("value column");
	assert_eq!(int4_slice(&values), &rows.clone().map(value).collect::<Vec<_>>()[..]);
	let labels = column_view(batch, "label").unwrap().expect("label column");
	let maybes = column_view(batch, "maybe").unwrap().expect("maybe column");
	assert_eq!(labels.len(), end - start);
	assert_eq!(maybes.len(), end - start);
	for (offset, row) in rows.enumerate() {
		assert_eq!(labels.get_value(offset), Value::Utf8(label(row)), "label row {row}");
		assert_eq!(maybes.get_value(offset), maybe(row), "maybe row {row}");
	}
}

fn scan(block: &Arc<ColumnBlock>, mut check: impl FnMut(&RecordBatch, usize, usize)) {
	let mut start = 0usize;
	let mut batches = 0usize;
	for batch in SnapshotReader::new(Arc::clone(block), BATCH, new_session()) {
		let batch = batch.expect("scan batch");
		let end = (start + BATCH).min(ROWS);
		assert_batch_rows(&batch, start, end);
		check(&batch, start, end);
		start = end;
		batches += 1;
	}
	assert_eq!(start, ROWS, "every row must be scanned exactly once");
	assert_eq!(batches, ROWS.div_ceil(BATCH), "a short final batch must still be emitted");
}

#[test]
fn a_second_scan_over_the_same_block_reads_identical_rows() {
	// A scan that mutated the shared block would make the rescan read different rows.
	let block = fixture(&[0, 5_000, ROWS]);
	let mut first = Vec::new();
	scan(&block, |batch, _, _| first.push(int4_slice(&column_view(batch, "value").unwrap().unwrap()).to_vec()));
	let mut second = Vec::new();
	scan(&block, |batch, _, _| second.push(int4_slice(&column_view(batch, "value").unwrap().unwrap()).to_vec()));
	assert_eq!(first, second, "a rescan must read exactly the rows of the first scan");
}

#[test]
fn a_batch_keeps_its_rows_after_the_block_is_dropped() {
	// A batch borrowing block memory would read freed rows once the block is dropped.
	let block = fixture(&[0, ROWS]);
	let mut reader = SnapshotReader::new(Arc::clone(&block), BATCH, new_session());
	let batch = reader.next().unwrap().unwrap();
	let second = reader.next().unwrap().unwrap();
	drop(reader);
	drop(block);
	assert_batch_rows(&batch, 0, BATCH);
	assert_batch_rows(&second, BATCH, 2 * BATCH);
}
