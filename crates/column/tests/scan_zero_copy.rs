// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_column::{
	reader::SnapshotReader,
	snapshot::{ColumnBlock, ColumnChunks},
};
use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	columns::Columns,
	data::{Column, canonical::Canonical},
};
use reifydb_value::value::{
	Value, container::varlen_array::compact_parts, datetime::DateTime, row_number::RowNumber,
	system_columns::SystemColumn, value_type::ValueType,
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

fn utf8_bytes(buffer: &ColumnBuffer) -> &[u8] {
	match buffer {
		ColumnBuffer::Utf8 {
			container,
			..
		} => compact_parts(container).0,
		other => panic!("expected a utf8 buffer, got {:?}", other.get_type()),
	}
}

fn label_byte_start(row: usize) -> usize {
	(0..row).map(|i| label(i).len()).sum()
}

struct ChunkBase {
	start: usize,
	end: usize,
	value: *const i32,
	label: *const u8,
	maybe: *const i32,
}

struct Fixture {
	block: Arc<ColumnBlock>,
	chunks: Vec<ChunkBase>,
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

fn fixture(bounds: &[usize]) -> Fixture {
	let schema = schema();
	let mut per_column: Vec<Vec<Column>> = vec![Vec::new(); schema.len()];
	let mut chunks = Vec::new();
	for window in bounds.windows(2) {
		let (start, end) = (window[0], window[1]);
		let rows = start..end;
		let value_buffer = ColumnBuffer::int4(rows.clone().map(value));
		let label_buffer = ColumnBuffer::utf8(rows.clone().map(label));
		let maybe_buffer = ColumnBuffer::int4_with_bitvec(
			rows.clone().map(|i| -value(i)),
			rows.clone().map(defined).collect::<Vec<_>>(),
		);
		chunks.push(ChunkBase {
			start,
			end,
			value: value_buffer.as_slice::<i32>().as_ptr(),
			label: utf8_bytes(&label_buffer).as_ptr(),
			maybe: maybe_buffer.as_slice::<i32>().as_ptr(),
		});
		let buffers = [
			ColumnBuffer::uint8(rows.clone().map(row_number)),
			value_buffer,
			ColumnBuffer::datetime(rows.clone().map(created_at)),
			label_buffer,
			ColumnBuffer::datetime(rows.clone().map(updated_at)),
			maybe_buffer,
			ColumnBuffer::datetime(rows.clone().map(time)),
			ColumnBuffer::uint8(rows.clone().map(commit_version)),
		];
		for (column, buffer) in per_column.iter_mut().zip(buffers) {
			column.push(Column::from_canonical(Canonical::from_buffer(buffer)));
		}
	}
	let columns = schema
		.iter()
		.zip(per_column)
		.map(|((_, ty, nullable), parts)| ColumnChunks::new(ty.clone(), *nullable, parts))
		.collect();
	Fixture {
		block: Arc::new(ColumnBlock::new(Arc::new(schema), columns)),
		chunks,
	}
}

fn containing(chunks: &[ChunkBase], start: usize, end: usize) -> Option<&ChunkBase> {
	chunks.iter().find(|c| c.start <= start && end <= c.end)
}

#[track_caller]
fn assert_batch_rows(batch: &Columns, start: usize, end: usize) {
	let rows = start..end;
	assert_eq!(batch.row_count(), end - start, "batch {start}..{end}");
	assert_eq!(batch.columns.len(), 3, "system columns must never surface as user columns");
	let expected: Vec<RowNumber> = rows.clone().map(|i| RowNumber(row_number(i))).collect();
	assert_eq!(batch.row_numbers(), &expected[..], "row numbers {start}..{end}");
	assert_eq!(batch.created_at(), &rows.clone().map(created_at).collect::<Vec<_>>()[..]);
	assert_eq!(batch.updated_at(), &rows.clone().map(updated_at).collect::<Vec<_>>()[..]);
	assert_eq!(batch.time(), &rows.clone().map(time).collect::<Vec<_>>()[..]);
	assert_eq!(batch.system.commit_versions(), &rows.clone().map(commit_version).collect::<Vec<_>>()[..]);

	let values = batch.column("value").expect("value column").data();
	assert_eq!(values.as_slice::<i32>(), &rows.clone().map(value).collect::<Vec<_>>()[..]);
	let labels = batch.column("label").expect("label column").data();
	let maybes = batch.column("maybe").expect("maybe column").data();
	assert_eq!(labels.len(), end - start);
	assert_eq!(maybes.len(), end - start);
	for (offset, row) in rows.enumerate() {
		assert_eq!(labels.get_value(offset), Value::Utf8(label(row)), "label row {row}");
		assert_eq!(maybes.get_value(offset), maybe(row), "maybe row {row}");
	}
}

fn scan(fixture: &Fixture, mut check: impl FnMut(&Columns, usize, usize)) {
	let mut start = 0usize;
	let mut batches = 0usize;
	for batch in SnapshotReader::new(Arc::clone(&fixture.block), BATCH) {
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
fn single_chunk_scan_aliases_the_block_for_every_batch() {
	// Each batch must be a view into the block's allocation at exactly its first row, otherwise the scan copies
	// every row.
	let fixture = fixture(&[0, ROWS]);
	let chunk = &fixture.chunks[0];
	scan(&fixture, |batch, start, _end| {
		let values = batch.column("value").unwrap().data();
		assert_eq!(
			values.as_slice::<i32>().as_ptr(),
			chunk.value.wrapping_add(start),
			"value batch at {start}"
		);
		let labels = batch.column("label").unwrap().data();
		assert_eq!(
			utf8_bytes(labels).as_ptr(),
			chunk.label.wrapping_add(label_byte_start(start)),
			"label batch at {start}"
		);
		let maybes = batch.column("maybe").unwrap().data();
		assert_eq!(
			maybes.as_slice::<i32>().as_ptr(),
			chunk.maybe.wrapping_add(start),
			"maybe batch at {start}"
		);
	});
}

#[test]
fn multi_chunk_scan_aliases_inside_a_chunk_and_copies_across_a_boundary() {
	// A batch inside one chunk must alias that chunk, and a batch spanning chunks must be a fresh copy that never
	// writes into either chunk.
	let bounds = [0, 3_000, 3_001, 7_000, ROWS];
	let fixture = fixture(&bounds);
	let mut aliased = 0usize;
	let mut copied = 0usize;
	scan(&fixture, |batch, start, end| {
		let values = batch.column("value").unwrap().data().as_slice::<i32>().as_ptr();
		let labels = utf8_bytes(batch.column("label").unwrap().data()).as_ptr();
		match containing(&fixture.chunks, start, end) {
			Some(chunk) => {
				assert_eq!(
					values,
					chunk.value.wrapping_add(start - chunk.start),
					"value batch at {start}"
				);
				let byte = label_byte_start(start) - label_byte_start(chunk.start);
				assert_eq!(labels, chunk.label.wrapping_add(byte), "label batch at {start}");
				aliased += 1;
			}
			None => {
				for chunk in &fixture.chunks {
					let inside = values >= chunk.value
						&& values < chunk.value.wrapping_add(chunk.end - chunk.start);
					assert!(
						!inside,
						"a spanning batch at {start} must never reuse a chunk allocation"
					);
				}
				copied += 1;
			}
		}
	});
	assert!(aliased > 0 && copied > 0, "the layout must exercise both paths, aliased {aliased} copied {copied}");

	let chunk_rows = fixture.block.column_by_name("value").unwrap().1;
	for (column, chunk) in chunk_rows.chunks.iter().zip(&fixture.chunks) {
		let canonical = column.to_canonical().unwrap();
		assert_eq!(
			canonical.buffer.as_slice::<i32>().as_ptr(),
			chunk.value,
			"the chunk must keep its allocation"
		);
		assert_eq!(
			canonical.buffer.as_slice::<i32>(),
			&(chunk.start..chunk.end).map(value).collect::<Vec<_>>()[..],
			"a scan must never change the rows of chunk {}..{}",
			chunk.start,
			chunk.end
		);
	}
}

#[test]
fn a_second_scan_over_the_same_block_reads_identical_rows() {
	// Scanning must never mutate the shared block, so a rescan must see exactly the same rows at the same
	// addresses.
	let fixture = fixture(&[0, 5_000, ROWS]);
	let mut first = Vec::new();
	scan(&fixture, |batch, _, _| first.push(batch.column("value").unwrap().data().as_slice::<i32>().as_ptr()));
	let mut second = Vec::new();
	scan(&fixture, |batch, _, _| second.push(batch.column("value").unwrap().data().as_slice::<i32>().as_ptr()));
	assert_eq!(first.len(), second.len());
	for (index, (a, b)) in first.iter().zip(&second).enumerate() {
		let start = index * BATCH;
		let end = (start + BATCH).min(ROWS);
		if containing(&fixture.chunks, start, end).is_some() {
			assert_eq!(a, b, "an aliased batch at {start} must land on the same address every scan");
		}
	}
}

#[test]
fn a_batch_keeps_its_rows_after_the_block_is_dropped() {
	// A batch must hold its own handle on the block memory, otherwise dropping the block frees rows the batch still
	// reads.
	let fixture = fixture(&[0, ROWS]);
	let base = fixture.chunks[0].value;
	let mut reader = SnapshotReader::new(Arc::clone(&fixture.block), BATCH);
	let batch = reader.next().unwrap().unwrap();
	let second = reader.next().unwrap().unwrap();
	drop(reader);
	drop(fixture);
	assert_batch_rows(&batch, 0, BATCH);
	assert_batch_rows(&second, BATCH, 2 * BATCH);
	assert_eq!(second.column("value").unwrap().data().as_slice::<i32>().as_ptr(), base.wrapping_add(BATCH));
}
