// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Multi-frame RBCF round trips where only some frames carry populated system columns. A frame
//! whose metadata arrays are present shifts the byte offsets of everything after it, so a decoder
//! that mistakes their length reads the next column descriptor misaligned.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, LargeStringArray, UInt64Array};
use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions};
use reifydb_value::value::{
	column_view::ColumnView,
	container::temporal_array::datetime_array,
	datetime::DateTime,
	frame::frame::Frame,
	system_columns::{SystemColumn, created_at, row_numbers, time, updated_at},
	value_type::ValueType,
};

use crate::common::{ColumnData, data, frame_of, view_at, with_system};

fn assert_col_data_eq(a: &ColumnView<'_>, b: &ColumnView<'_>) {
	assert_eq!(a.len(), b.len(), "column length mismatch");
	for i in 0..a.len() {
		let va = a.get_value(i);
		let vb = b.get_value(i);
		assert_eq!(va, vb, "mismatch at index {}: {:?} != {:?}", i, va, vb);
	}
}

fn names(frame: &Frame) -> Vec<&str> {
	frame.batch.schema_ref().fields().iter().map(|field| field.name().as_str()).collect()
}

fn assert_frame_eq(a: &Frame, b: &Frame) {
	let (rows_a, rows_b) = (row_numbers(&a.batch).unwrap(), row_numbers(&b.batch).unwrap());
	assert_eq!(rows_a.len(), rows_b.len(), "row_numbers length mismatch");
	for (i, (ra, rb)) in rows_a.iter().zip(rows_b).enumerate() {
		assert_eq!(ra.value(), rb.value(), "row_number mismatch at {}", i);
	}
	let (created_a, created_b) = (created_at(&a.batch).unwrap(), created_at(&b.batch).unwrap());
	assert_eq!(created_a.len(), created_b.len(), "created_at length mismatch");
	for (i, (ca, cb)) in created_a.iter().zip(created_b).enumerate() {
		assert_eq!(ca.to_nanos(), cb.to_nanos(), "created_at mismatch at {}", i);
	}
	let (updated_a, updated_b) = (updated_at(&a.batch).unwrap(), updated_at(&b.batch).unwrap());
	assert_eq!(updated_a.len(), updated_b.len(), "updated_at length mismatch");
	for (i, (ua, ub)) in updated_a.iter().zip(updated_b).enumerate() {
		assert_eq!(ua.to_nanos(), ub.to_nanos(), "updated_at mismatch at {}", i);
	}
	let (time_a, time_b) = (time(&a.batch).unwrap(), time(&b.batch).unwrap());
	assert_eq!(time_a.len(), time_b.len(), "time length mismatch");
	for (i, (ta, tb)) in time_a.iter().zip(time_b).enumerate() {
		assert_eq!(ta.to_nanos(), tb.to_nanos(), "time mismatch at {}", i);
	}
	assert_eq!(a.batch.num_columns(), b.batch.num_columns(), "column count mismatch");
	for (index, (na, nb)) in names(a).into_iter().zip(names(b)).enumerate() {
		assert_eq!(na, nb);
		assert_col_data_eq(&view_at(a, index), &view_at(b, index));
	}
}

fn round_trip_multi(frames: Vec<Frame>) {
	let encoded = encode_frames(&frames, &EncodeOptions::default()).expect("encode failed");
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), frames.len(), "frame count mismatch");
	for (i, (orig, dec)) in frames.iter().zip(decoded.iter()).enumerate() {
		assert_frame_eq_with_idx(i, orig, dec);
	}
}

fn assert_frame_eq_with_idx(idx: usize, a: &Frame, b: &Frame) {
	assert_eq!(a.batch.num_columns(), b.batch.num_columns(), "frame[{idx}] column count mismatch");
	for (na, nb) in names(a).into_iter().zip(names(b)) {
		assert_eq!(na, nb, "frame[{idx}] column name mismatch");
	}
	assert_frame_eq(a, b);
}

fn int4(values: Vec<i32>) -> ColumnData {
	data(ValueType::Int4, Int32Array::from(values))
}

fn utf8(values: Vec<&str>) -> ColumnData {
	data(ValueType::Utf8, LargeStringArray::from(values.into_iter().map(str::to_string).collect::<Vec<_>>()))
}

fn row_number_array(values: Vec<u64>) -> ArrayRef {
	Arc::new(UInt64Array::from(values))
}

fn datetimes(nanos: impl IntoIterator<Item = i64>) -> ArrayRef {
	Arc::new(datetime_array(nanos.into_iter().map(DateTime::from_nanos)))
}

fn frame_int4(name: &str, values: Vec<i32>) -> Frame {
	frame_of(vec![(name, int4(values))])
}

fn frame_with_metadata(name: &str, values: Vec<i32>) -> Frame {
	let n = values.len();
	let frame = frame_int4(name, values);
	let frame = with_system(
		frame,
		SystemColumn::RowNumbers,
		row_number_array((0..n).map(|i| (i as u64) + 1).collect()),
	);
	let frame = with_system(frame, SystemColumn::CreatedAt, datetimes((0..n).map(|i| (i as i64) * 1_000_000)));
	let frame = with_system(frame, SystemColumn::UpdatedAt, datetimes((0..n).map(|i| (i as i64) * 2_000_000)));
	with_system(frame, SystemColumn::Time, datetimes((0..n).map(|i| (i as i64) * 3_000_000)))
}

#[test]
fn two_frames_no_metadata() {
	round_trip_multi(vec![frame_int4("a", vec![1, 2]), frame_int4("b", vec![10, 20])]);
}

#[test]
fn two_frames_both_with_metadata() {
	round_trip_multi(vec![frame_with_metadata("a", vec![1, 2, 3]), frame_with_metadata("b", vec![10, 20, 30])]);
}

#[test]
fn metadata_then_no_metadata() {
	round_trip_multi(vec![frame_with_metadata("a", vec![1, 2, 3]), frame_int4("b", vec![10, 20, 30])]);
}

#[test]
fn no_metadata_then_metadata() {
	round_trip_multi(vec![frame_int4("a", vec![1, 2, 3]), frame_with_metadata("b", vec![10, 20, 30])]);
}

#[test]
fn three_frames_alternating_metadata() {
	round_trip_multi(vec![
		frame_with_metadata("a", vec![1]),
		frame_int4("b", vec![100, 200]),
		frame_with_metadata("c", vec![3, 4, 5]),
	]);
}

#[test]
fn two_frames_only_row_numbers() {
	let frame1 = with_system(frame_int4("v", vec![10, 20]), SystemColumn::RowNumbers, row_number_array(vec![1, 2]));
	let frame2 = with_system(frame_int4("w", vec![30]), SystemColumn::RowNumbers, row_number_array(vec![3]));
	round_trip_multi(vec![frame1, frame2]);
}

#[test]
fn two_frames_only_created_at() {
	let frame1 = with_system(frame_int4("v", vec![1, 2]), SystemColumn::CreatedAt, datetimes([100, 200]));
	let frame2 = with_system(frame_int4("w", vec![3]), SystemColumn::CreatedAt, datetimes([300]));
	round_trip_multi(vec![frame1, frame2]);
}

#[test]
fn frame_with_only_metadata_take_one_then_aggregate() {
	// The narrowest case: a single-row frame whose metadata arrays are length 1, followed by a
	// multi-row frame with none, so a decoder that reuses the first frame's lengths misaligns.
	let sort_take_frame = frame_of(vec![
		("base_mint", utf8(vec!["So11111111111111111111111111111111111111112"])),
		("close_usd", int4(vec![86])),
	]);
	let sort_take_frame = with_system(sort_take_frame, SystemColumn::RowNumbers, row_number_array(vec![42]));
	let sort_take_frame =
		with_system(sort_take_frame, SystemColumn::CreatedAt, datetimes([1_777_056_096_000_000_000i64]));
	let sort_take_frame =
		with_system(sort_take_frame, SystemColumn::UpdatedAt, datetimes([1_777_056_096_000_000_000i64]));
	let sort_take_frame =
		with_system(sort_take_frame, SystemColumn::Time, datetimes([1_777_056_096_000_000_000i64]));
	let aggregate_frame = frame_of(vec![
		(
			"quote_mint",
			utf8(vec![
				"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
				"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
			]),
		),
		("c", int4(vec![19, 21])),
	]);
	round_trip_multi(vec![sort_take_frame, aggregate_frame]);
}
