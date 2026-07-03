// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Multi-frame round-trip regression tests for RBCF.
//!
//! Reproduces the bug where multi-statement RQL responses containing a
//! `sort | take {N}` frame become undecodable: the decoder reads garbage
//! at byte 1 of a column descriptor (observed encoding tags 64, 212, 125
//! across runs - classic byte-misalignment signature). Single-statement
//! sort+take and multi-statement-without-sort both decode cleanly.
//!
//! These tests focus on multi-frame buffers where one or more frames
//! carry populated `row_numbers` / `created_at` / `updated_at` metadata
//! arrays - the shape an operator like `sort | take` produces.

use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions};
use reifydb_value::value::{
	container::{number::NumberContainer, utf8::Utf8Container},
	datetime::DateTime,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	row_number::RowNumber,
};

fn assert_col_data_eq(a: &FrameColumnData, b: &FrameColumnData) {
	assert_eq!(a.len(), b.len(), "column length mismatch");
	for i in 0..a.len() {
		let va = a.get_value(i);
		let vb = b.get_value(i);
		assert_eq!(va, vb, "mismatch at index {}: {:?} != {:?}", i, va, vb);
	}
}

fn assert_frame_eq(a: &Frame, b: &Frame) {
	assert_eq!(a.row_numbers.len(), b.row_numbers.len(), "row_numbers length mismatch");
	for (i, (ra, rb)) in a.row_numbers.iter().zip(&b.row_numbers).enumerate() {
		assert_eq!(ra.value(), rb.value(), "row_number mismatch at {}", i);
	}
	assert_eq!(a.created_at.len(), b.created_at.len(), "created_at length mismatch");
	for (i, (ca, cb)) in a.created_at.iter().zip(&b.created_at).enumerate() {
		assert_eq!(ca.to_nanos(), cb.to_nanos(), "created_at mismatch at {}", i);
	}
	assert_eq!(a.updated_at.len(), b.updated_at.len(), "updated_at length mismatch");
	for (i, (ua, ub)) in a.updated_at.iter().zip(&b.updated_at).enumerate() {
		assert_eq!(ua.to_nanos(), ub.to_nanos(), "updated_at mismatch at {}", i);
	}
	assert_eq!(a.columns.len(), b.columns.len(), "column count mismatch");
	for (ca, cb) in a.columns.iter().zip(&b.columns) {
		assert_eq!(ca.name, cb.name);
		assert_col_data_eq(&ca.data, &cb.data);
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
	assert_eq!(a.columns.len(), b.columns.len(), "frame[{idx}] column count mismatch");
	for (ca, cb) in a.columns.iter().zip(&b.columns) {
		assert_eq!(ca.name, cb.name, "frame[{idx}] column name mismatch");
	}
	assert_frame_eq(a, b);
}

fn frame_int4(name: &str, values: Vec<i32>) -> Frame {
	Frame::new(vec![FrameColumn {
		name: name.to_string(),
		data: FrameColumnData::Int4(NumberContainer::new(values)),
	}])
}

fn frame_with_metadata(name: &str, values: Vec<i32>) -> Frame {
	let n = values.len();
	Frame {
		row_numbers: (0..n).map(|i| RowNumber::new((i as u64) + 1)).collect(),
		created_at: (0..n).map(|i| DateTime::from_nanos((i as u64) * 1_000_000)).collect(),
		updated_at: (0..n).map(|i| DateTime::from_nanos((i as u64) * 2_000_000)).collect(),
		columns: vec![FrameColumn {
			name: name.to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(values)),
		}],
	}
}

#[test]
fn two_frames_no_metadata() {
	// Sanity: matches the existing `multi_frame` test in metadata.rs.
	round_trip_multi(vec![frame_int4("a", vec![1, 2]), frame_int4("b", vec![10, 20])]);
}

#[test]
fn two_frames_both_with_metadata() {
	// Both frames have populated row_numbers + created_at + updated_at,
	// length matching their single column. This is the shape produced by
	// two `sort | take {N}` statements in a multi-statement RQL.
	round_trip_multi(vec![frame_with_metadata("a", vec![1, 2, 3]), frame_with_metadata("b", vec![10, 20, 30])]);
}

#[test]
fn metadata_then_no_metadata() {
	// First frame is `sort+take`-shaped, second is `aggregate`-shaped.
	round_trip_multi(vec![frame_with_metadata("a", vec![1, 2, 3]), frame_int4("b", vec![10, 20, 30])]);
}

#[test]
fn no_metadata_then_metadata() {
	// First frame is `aggregate`-shaped, second is `sort+take`-shaped.
	round_trip_multi(vec![frame_int4("a", vec![1, 2, 3]), frame_with_metadata("b", vec![10, 20, 30])]);
}

#[test]
fn three_frames_alternating_metadata() {
	// Mimics the token_overview handler shape: a sort+take frame, then
	// an aggregate frame, then another sort+take frame.
	round_trip_multi(vec![
		frame_with_metadata("a", vec![1]),
		frame_int4("b", vec![100, 200]),
		frame_with_metadata("c", vec![3, 4, 5]),
	]);
}

#[test]
fn two_frames_only_row_numbers() {
	let frame1 = Frame {
		row_numbers: vec![RowNumber::new(1), RowNumber::new(2)],
		created_at: vec![],
		updated_at: vec![],
		columns: vec![FrameColumn {
			name: "v".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![10, 20])),
		}],
	};
	let frame2 = Frame {
		row_numbers: vec![RowNumber::new(3)],
		created_at: vec![],
		updated_at: vec![],
		columns: vec![FrameColumn {
			name: "w".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![30])),
		}],
	};
	round_trip_multi(vec![frame1, frame2]);
}

#[test]
fn two_frames_only_created_at() {
	let frame1 = Frame {
		row_numbers: vec![],
		created_at: vec![DateTime::from_nanos(100), DateTime::from_nanos(200)],
		updated_at: vec![],
		columns: vec![FrameColumn {
			name: "v".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![1, 2])),
		}],
	};
	let frame2 = Frame {
		row_numbers: vec![],
		created_at: vec![DateTime::from_nanos(300)],
		updated_at: vec![],
		columns: vec![FrameColumn {
			name: "w".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![3])),
		}],
	};
	round_trip_multi(vec![frame1, frame2]);
}

#[test]
fn frame_with_only_metadata_take_one_then_aggregate() {
	// The minimal reproducer of the observed birdeye bug: the first
	// frame has exactly one row plus row_numbers/created_at/updated_at
	// populated to length 1 (the `sort | take {1}` shape), the second
	// is the markets-count aggregate (no metadata, multi-row).
	let sort_take_frame = Frame {
		row_numbers: vec![RowNumber::new(42)],
		created_at: vec![DateTime::from_nanos(1_777_056_096_000_000_000u64)],
		updated_at: vec![DateTime::from_nanos(1_777_056_096_000_000_000u64)],
		columns: vec![
			FrameColumn {
				name: "base_mint".to_string(),
				data: FrameColumnData::Utf8(Utf8Container::new(vec![
					"So11111111111111111111111111111111111111112".to_string(),
				])),
			},
			FrameColumn {
				name: "close_usd".to_string(),
				data: FrameColumnData::Int4(NumberContainer::new(vec![86])),
			},
		],
	};
	let aggregate_frame = Frame::new(vec![
		FrameColumn {
			name: "quote_mint".to_string(),
			data: FrameColumnData::Utf8(Utf8Container::new(vec![
				"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
				"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".to_string(),
			])),
		},
		FrameColumn {
			name: "c".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![19, 21])),
		},
	]);
	round_trip_multi(vec![sort_take_frame, aggregate_frame]);
}
