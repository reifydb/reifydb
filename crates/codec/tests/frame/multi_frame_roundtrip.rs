// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Multi-frame RBCF round trips where only some frames carry populated system columns. A frame
//! whose metadata arrays are present shifts the byte offsets of everything after it, so a decoder
//! that mistakes their length reads the next column descriptor misaligned.

use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions};
use reifydb_value::value::{
	container::{number::NumberContainer, utf8::Utf8Container},
	datetime::DateTime,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	row_number::RowNumber,
	system_columns::SystemColumns,
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
	assert_eq!(a.row_numbers().len(), b.row_numbers().len(), "row_numbers length mismatch");
	for (i, (ra, rb)) in a.row_numbers().iter().zip(b.row_numbers()).enumerate() {
		assert_eq!(ra.value(), rb.value(), "row_number mismatch at {}", i);
	}
	assert_eq!(a.created_at().len(), b.created_at().len(), "created_at length mismatch");
	for (i, (ca, cb)) in a.created_at().iter().zip(b.created_at()).enumerate() {
		assert_eq!(ca.to_nanos(), cb.to_nanos(), "created_at mismatch at {}", i);
	}
	assert_eq!(a.updated_at().len(), b.updated_at().len(), "updated_at length mismatch");
	for (i, (ua, ub)) in a.updated_at().iter().zip(b.updated_at()).enumerate() {
		assert_eq!(ua.to_nanos(), ub.to_nanos(), "updated_at mismatch at {}", i);
	}
	assert_eq!(a.time().len(), b.time().len(), "time length mismatch");
	for (i, (ta, tb)) in a.time().iter().zip(b.time()).enumerate() {
		assert_eq!(ta.to_nanos(), tb.to_nanos(), "time mismatch at {}", i);
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
		system: SystemColumns::new(
			(0..n).map(|i| RowNumber::new((i as u64) + 1)).collect(),
			Vec::new(),
			(0..n).map(|i| DateTime::from_nanos((i as u64) * 1_000_000)).collect(),
			(0..n).map(|i| DateTime::from_nanos((i as u64) * 2_000_000)).collect(),
			(0..n).map(|i| DateTime::from_nanos((i as u64) * 3_000_000)).collect(),
		),
		op: None,
		columns: vec![FrameColumn {
			name: name.to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(values)),
		}],
	}
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
	let frame1 = Frame {
		system: SystemColumns::new(
			vec![RowNumber::new(1), RowNumber::new(2)],
			Vec::new(),
			vec![],
			vec![],
			vec![],
		),
		op: None,
		columns: vec![FrameColumn {
			name: "v".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![10, 20])),
		}],
	};
	let frame2 = Frame {
		system: SystemColumns::new(vec![RowNumber::new(3)], Vec::new(), vec![], vec![], vec![]),
		op: None,
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
		system: SystemColumns::new(
			vec![],
			Vec::new(),
			vec![DateTime::from_nanos(100), DateTime::from_nanos(200)],
			vec![],
			vec![],
		),
		op: None,
		columns: vec![FrameColumn {
			name: "v".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![1, 2])),
		}],
	};
	let frame2 = Frame {
		system: SystemColumns::new(vec![], Vec::new(), vec![DateTime::from_nanos(300)], vec![], vec![]),
		op: None,
		columns: vec![FrameColumn {
			name: "w".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![3])),
		}],
	};
	round_trip_multi(vec![frame1, frame2]);
}

#[test]
fn frame_with_only_metadata_take_one_then_aggregate() {
	// The narrowest case: a single-row frame whose metadata arrays are length 1, followed by a
	// multi-row frame with none, so a decoder that reuses the first frame's lengths misaligns.
	let sort_take_frame = Frame {
		system: SystemColumns::new(
			vec![RowNumber::new(42)],
			Vec::new(),
			vec![DateTime::from_nanos(1_777_056_096_000_000_000u64)],
			vec![DateTime::from_nanos(1_777_056_096_000_000_000u64)],
			vec![DateTime::from_nanos(1_777_056_096_000_000_000u64)],
		),
		op: None,
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
