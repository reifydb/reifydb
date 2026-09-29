// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{slice::from_ref, sync::Arc};

use arrow_array::{ArrayRef, BooleanArray, Int32Array, Int64Array, LargeStringArray, UInt64Array};
use reifydb_codec::{
	error::DecodeError,
	frame::{decode::decode_frames, encode::encode_frames, format::Encoding, options::EncodeOptions},
};
use reifydb_value::value::{
	column_view::ColumnView,
	container::temporal_array::{date_array, datetime_array},
	date::Date,
	datetime::DateTime,
	frame::frame::Frame,
	system_columns::{SystemColumn, created_at, keep_system_columns, row_numbers, system_column, time, updated_at},
	value_type::ValueType,
};

use crate::common::{data, frame_of, frame_without_columns, optional, view_at, with_system};

fn assert_col_data_eq(a: &ColumnView<'_>, b: &ColumnView<'_>) {
	assert_eq!(a.len(), b.len(), "column length mismatch");
	for i in 0..a.len() {
		let va = a.get_value(i);
		let vb = b.get_value(i);
		assert_eq!(va, vb, "mismatch at index {}: {:?} != {:?}", i, va, vb);
	}
}

fn assert_frame_eq(a: &Frame, b: &Frame) {
	assert_eq!(a.op, b.op, "the op rides the frame header and must survive a round trip");
	let (rows_a, rows_b) = (row_numbers(&a.batch).unwrap(), row_numbers(&b.batch).unwrap());
	assert_eq!(rows_a.len(), rows_b.len());
	for (i, (ra, rb)) in rows_a.iter().zip(rows_b).enumerate() {
		assert_eq!(ra.value(), rb.value(), "row_number mismatch at {}", i);
	}
	assert_eq!(created_at(&a.batch).unwrap().len(), created_at(&b.batch).unwrap().len());
	assert_eq!(updated_at(&a.batch).unwrap().len(), updated_at(&b.batch).unwrap().len());
	assert_eq!(time(&a.batch).unwrap().len(), time(&b.batch).unwrap().len());
	assert_eq!(a.batch.num_columns(), b.batch.num_columns());
	for (index, (fa, fb)) in a.batch.schema_ref().fields().iter().zip(b.batch.schema_ref().fields()).enumerate() {
		assert_eq!(fa.name(), fb.name());
		assert_col_data_eq(&view_at(a, index), &view_at(b, index));
	}
}

fn round_trip(frame: Frame) {
	let encoded = encode_frames(from_ref(&frame), &EncodeOptions::default()).expect("encode failed");
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), 1);
	assert_frame_eq(&frame, &decoded[0]);
}

fn datetimes(nanos: &[i64]) -> ArrayRef {
	Arc::new(datetime_array(nanos.iter().map(|&n| DateTime::from_nanos(n))))
}

#[test]
fn empty_frame() {
	let frame = frame_without_columns();
	round_trip(frame);
}

#[test]
fn frame_with_metadata() {
	let frame = frame_of(vec![("x", data(ValueType::Int4, Int32Array::from(vec![10, 20, 30])))]);
	let frame = with_system(frame, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1, 2, 3])));
	let frame =
		with_system(frame, SystemColumn::CreatedAt, datetimes(&[1_000_000_000, 2_000_000_000, 3_000_000_000]));
	let frame =
		with_system(frame, SystemColumn::UpdatedAt, datetimes(&[4_000_000_000, 5_000_000_000, 6_000_000_000]));
	let frame = with_system(frame, SystemColumn::Time, datetimes(&[7_000_000_000, 8_000_000_000, 9_000_000_000]));
	round_trip(frame);
}

#[test]
fn multi_frame() {
	let frame1 = frame_of(vec![("a", data(ValueType::Int4, Int32Array::from(vec![1, 2])))]);
	let frame2 = frame_of(vec![(
		"b",
		data(ValueType::Utf8, LargeStringArray::from(vec!["x".to_string(), "y".to_string()])),
	)]);
	let encoded =
		encode_frames(&[frame1.clone(), frame2.clone()], &EncodeOptions::default()).expect("encode failed");
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), 2);
	assert_frame_eq(&frame1, &decoded[0]);
	assert_frame_eq(&frame2, &decoded[1]);
}

#[test]
fn empty_columns() {
	let frame = frame_of(vec![
		("empty_ints", data(ValueType::Int4, Int32Array::from(Vec::<i32>::new()))),
		("empty_strings", data(ValueType::Utf8, LargeStringArray::from(Vec::<String>::new()))),
	]);
	round_trip(frame);
}

#[test]
fn empty_frame_keeps_the_row_numbers_flag() {
	// A client must see #rownum on an empty answer too, so the flag cannot be derived from a non-empty list.
	let frame = frame_of(vec![("x", data(ValueType::Int4, Int32Array::from(Vec::<i32>::new())))]);
	let frame = with_system(frame, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(Vec::<u64>::new())));

	let encoded = encode_frames(from_ref(&frame), &EncodeOptions::default()).expect("encode failed");
	let decoded = decode_frames(&encoded).expect("decode failed");

	assert_eq!(decoded.len(), 1);
	assert!(
		system_column(&decoded[0].batch, SystemColumn::RowNumbers).is_some(),
		"a 0-row frame with row numbers must decode with the flag set"
	);
	assert!(row_numbers(&decoded[0].batch).unwrap().is_empty());
	let user_only = Frame::from(keep_system_columns(&frame.batch, &[]).unwrap());
	let without = decode_frames(&encode_frames(&[user_only], &EncodeOptions::default()).expect("encode failed"))
		.expect("decode failed");
	assert!(
		system_column(&without[0].batch, SystemColumn::RowNumbers).is_none(),
		"a frame without row numbers must not gain the flag"
	);
}

#[test]
fn invalid_magic() {
	let mut data = encode_frames(&[frame_without_columns()], &EncodeOptions::default()).expect("encode failed");
	data[0] = 0xFF; // corrupt magic
	let result = decode_frames(&data);
	assert!(matches!(result, Err(DecodeError::InvalidMagic(_))));
}

#[test]
fn column_decode_error_includes_name() {
	let frame = frame_of(vec![(
		"test_col",
		data(
			ValueType::Date,
			date_array(vec![
				Date::from_days_since_epoch(0).unwrap(),
				Date::from_days_since_epoch(1).unwrap(),
				Date::from_days_since_epoch(2).unwrap(),
			]),
		),
	)]);
	let encoded = encode_frames(&[frame], &EncodeOptions::default()).expect("encode failed");

	// Byte 40 is the column descriptor's nones_len: msg header 16 + frame header 12 + descriptor
	// offset 12. An impossible length there must be reported against the column, not swallowed.
	let mut corrupted = encoded.clone();
	corrupted[40..44].copy_from_slice(&9999u32.to_le_bytes());

	let err = decode_frames(&corrupted).unwrap_err();
	match err {
		DecodeError::ColumnDecodeFailed {
			column_name,
			..
		} => {
			assert_eq!(column_name, "test_col");
		}
		_ => panic!("expected ColumnDecodeFailed error"),
	}
}

#[test]
fn unsupported_version() {
	let mut data = encode_frames(&[frame_without_columns()], &EncodeOptions::default()).expect("encode failed");
	// The version is the u16 at bytes 4..6, right after the magic.
	data[4] = 0xFE;
	data[5] = 0xCA;
	let result = decode_frames(&data);
	assert!(matches!(result, Err(DecodeError::UnsupportedVersion(_))));
}

#[test]
fn unexpected_eof_msg_header() {
	let data = encode_frames(&[frame_without_columns()], &EncodeOptions::default()).expect("encode failed");
	for i in 1..16 {
		let result = decode_frames(&data[..i]);
		assert!(matches!(result, Err(DecodeError::UnexpectedEof { .. })));
	}
}

#[test]
fn metadata_combinations() {
	// Each metadata array is independently present, so the flag byte has to be read per array
	// rather than as all-or-nothing.
	let frame1 = frame_of(vec![("v", data(ValueType::Int4, Int32Array::from(vec![10])))]);
	let frame1 = with_system(frame1, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1])));
	round_trip(frame1);

	let frame2 = frame_of(vec![("v", data(ValueType::Int4, Int32Array::from(vec![10])))]);
	let frame2 = with_system(frame2, SystemColumn::CreatedAt, datetimes(&[100]));
	let frame2 = with_system(frame2, SystemColumn::UpdatedAt, datetimes(&[200]));
	let frame2 = with_system(frame2, SystemColumn::Time, datetimes(&[300]));
	round_trip(frame2);
}

#[test]
fn empty_column_name() {
	let frame = frame_of(vec![("", data(ValueType::Int4, Int32Array::from(vec![1, 2, 3])))]);
	round_trip(frame);
}

#[test]
fn mixed_types_frame() {
	let frame = frame_of(vec![
		("id", data(ValueType::Int8, Int64Array::from(vec![1, 2, 3]))),
		(
			"name",
			data(
				ValueType::Utf8,
				LargeStringArray::from(vec![
					"alice".to_string(),
					"bob".to_string(),
					"charlie".to_string(),
				]),
			),
		),
		("active", data(ValueType::Boolean, BooleanArray::from(vec![true, false, true]))),
		(
			"email",
			optional(
				data(
					ValueType::Utf8,
					LargeStringArray::from(vec![
						"a@b.com".to_string(),
						"".to_string(),
						"c@d.com".to_string(),
					]),
				),
				&[true, false, true],
			),
		),
	]);
	round_trip(frame);
}

#[test]
fn heuristics_threshold_small_columns() {
	// Below MIN_ROWS the heuristic refuses every compressed encoding, since the per-encoding
	// overhead would exceed the saving.
	let values: Vec<i32> = (1..=3).collect();
	let frame = frame_of(vec![("small", data(ValueType::Int4, Int32Array::from(values)))]);
	let encoded = encode_frames(&[frame], &EncodeOptions::default()).expect("encode failed");
	// Byte 29 is the first column's encoding byte: msg header 16 + frame header 12 + type code 1.
	assert_eq!(encoded[29], Encoding::Plain as u8);
}

#[test]
fn compression_none_forces_plain() {
	let values: Vec<i32> = (1..=500).collect();
	let frame = frame_of(vec![("seq", data(ValueType::Int4, Int32Array::from(values)))]);
	let encoded = encode_frames(from_ref(&frame), &EncodeOptions::none()).expect("encode failed");
	// A sequence this regular would delta-encode to a fraction of its size, so exceeding the raw
	// 500 * 4 bytes proves compression really was disabled.
	assert!(encoded.len() > 2000, "expected plain (no compression), got {} bytes", encoded.len());
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), 1);
	assert_frame_eq(&frame, &decoded[0]);
}

#[test]
fn compression_max_round_trip() {
	let values: Vec<i32> = (1..=500).collect();
	let frame = frame_of(vec![("seq", data(ValueType::Int4, Int32Array::from(values)))]);
	let encoded = encode_frames(from_ref(&frame), &EncodeOptions::max()).expect("encode failed");
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), 1);
	assert_frame_eq(&frame, &decoded[0]);
}
