// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	util::bitvec::BitVec,
	value::{
		container::{
			bool::BoolContainer, number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container,
		},
		date::Date,
		datetime::DateTime,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		row_number::RowNumber,
	},
};
use reifydb_wire_format::{
	decode::decode_frames, encode::encode_frames, error::DecodeError, format::Encoding, options::EncodeOptions,
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
	assert_eq!(a.row_numbers.len(), b.row_numbers.len());
	for (i, (ra, rb)) in a.row_numbers.iter().zip(&b.row_numbers).enumerate() {
		assert_eq!(ra.value(), rb.value(), "row_number mismatch at {}", i);
	}
	assert_eq!(a.created_at.len(), b.created_at.len());
	assert_eq!(a.updated_at.len(), b.updated_at.len());
	assert_eq!(a.columns.len(), b.columns.len());
	for (ca, cb) in a.columns.iter().zip(&b.columns) {
		assert_eq!(ca.name, cb.name);
		assert_col_data_eq(&ca.data, &cb.data);
	}
}

fn round_trip(frame: Frame) {
	let encoded = encode_frames(&[frame.clone()], &EncodeOptions::default()).expect("encode failed");
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), 1);
	assert_frame_eq(&frame, &decoded[0]);
}

#[test]
fn empty_frame() {
	let frame = Frame::new(vec![]);
	round_trip(frame);
}

#[test]
fn frame_with_metadata() {
	let frame = Frame {
		row_numbers: vec![RowNumber::new(1), RowNumber::new(2), RowNumber::new(3)],
		created_at: vec![
			DateTime::from_nanos(1_000_000_000),
			DateTime::from_nanos(2_000_000_000),
			DateTime::from_nanos(3_000_000_000),
		],
		updated_at: vec![
			DateTime::from_nanos(4_000_000_000),
			DateTime::from_nanos(5_000_000_000),
			DateTime::from_nanos(6_000_000_000),
		],
		columns: vec![FrameColumn {
			name: "x".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![10, 20, 30])),
		}],
	};
	round_trip(frame);
}

#[test]
fn multi_frame() {
	let frame1 = Frame::new(vec![FrameColumn {
		name: "a".to_string(),
		data: FrameColumnData::Int4(NumberContainer::new(vec![1, 2])),
	}]);
	let frame2 = Frame::new(vec![FrameColumn {
		name: "b".to_string(),
		data: FrameColumnData::Utf8(Utf8Container::new(vec!["x".to_string(), "y".to_string()])),
	}]);
	let encoded =
		encode_frames(&[frame1.clone(), frame2.clone()], &EncodeOptions::default()).expect("encode failed");
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), 2);
	assert_frame_eq(&frame1, &decoded[0]);
	assert_frame_eq(&frame2, &decoded[1]);
}

#[test]
fn empty_columns() {
	let frame = Frame::new(vec![
		FrameColumn {
			name: "empty_ints".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![])),
		},
		FrameColumn {
			name: "empty_strings".to_string(),
			data: FrameColumnData::Utf8(Utf8Container::new(vec![])),
		},
	]);
	round_trip(frame);
}

#[test]
fn invalid_magic() {
	let mut data = encode_frames(&[Frame::new(vec![])], &EncodeOptions::default()).expect("encode failed");
	data[0] = 0xFF; // corrupt magic
	let result = decode_frames(&data);
	assert!(matches!(result, Err(DecodeError::InvalidMagic(_))));
}

#[test]
fn column_decode_error_includes_name() {
	let frame = Frame::new(vec![FrameColumn {
		name: "test_col".to_string(),
		data: FrameColumnData::Date(TemporalContainer::new(vec![
			Date::from_days_since_epoch(0).unwrap(),
			Date::from_days_since_epoch(1).unwrap(),
			Date::from_days_since_epoch(2).unwrap(),
		])),
	}]);
	let encoded = encode_frames(&[frame], &EncodeOptions::default()).expect("encode failed");

	// data_len is at offset 16 (msg header) + 12 (frame header) + 12 (offset in col descriptor) = 40
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
	let mut data = encode_frames(&[Frame::new(vec![])], &EncodeOptions::default()).expect("encode failed");
	// version is at offset 4
	data[4] = 0xFE;
	data[5] = 0xCA;
	let result = decode_frames(&data);
	assert!(matches!(result, Err(DecodeError::UnsupportedVersion(_))));
}

#[test]
fn unexpected_eof_msg_header() {
	let data = encode_frames(&[Frame::new(vec![])], &EncodeOptions::default()).expect("encode failed");
	for i in 1..16 {
		let result = decode_frames(&data[..i]);
		assert!(matches!(result, Err(DecodeError::UnexpectedEof { .. })));
	}
}

#[test]
fn metadata_combinations() {
	// Only row numbers
	let frame1 = Frame {
		row_numbers: vec![RowNumber::new(1)],
		created_at: vec![],
		updated_at: vec![],
		columns: vec![FrameColumn {
			name: "v".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![10])),
		}],
	};
	round_trip(frame1);

	// Only timestamps
	let frame2 = Frame {
		row_numbers: vec![],
		created_at: vec![DateTime::from_nanos(100)],
		updated_at: vec![DateTime::from_nanos(200)],
		columns: vec![FrameColumn {
			name: "v".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![10])),
		}],
	};
	round_trip(frame2);
}

#[test]
fn empty_column_name() {
	let frame = Frame::new(vec![FrameColumn {
		name: "".to_string(),
		data: FrameColumnData::Int4(NumberContainer::new(vec![1, 2, 3])),
	}]);
	round_trip(frame);
}

#[test]
fn mixed_types_frame() {
	let frame = Frame::new(vec![
		FrameColumn {
			name: "id".to_string(),
			data: FrameColumnData::Int8(NumberContainer::new(vec![1, 2, 3])),
		},
		FrameColumn {
			name: "name".to_string(),
			data: FrameColumnData::Utf8(Utf8Container::new(vec![
				"alice".to_string(),
				"bob".to_string(),
				"charlie".to_string(),
			])),
		},
		FrameColumn {
			name: "active".to_string(),
			data: FrameColumnData::Bool(BoolContainer::new(vec![true, false, true])),
		},
		FrameColumn {
			name: "email".to_string(),
			data: FrameColumnData::Option {
				inner: Box::new(FrameColumnData::Utf8(Utf8Container::new(vec![
					"a@b.com".to_string(),
					"".to_string(),
					"c@d.com".to_string(),
				]))),
				bitvec: BitVec::from_slice(&[true, false, true]),
			},
		},
	]);
	round_trip(frame);
}

#[test]
fn heuristics_threshold_small_columns() {
	// < 4 rows should always be Plain
	let values: Vec<i32> = (1..=3).collect();
	let frame = Frame::new(vec![FrameColumn {
		name: "small".to_string(),
		data: FrameColumnData::Int4(NumberContainer::new(values)),
	}]);
	let encoded = encode_frames(&[frame], &EncodeOptions::default()).expect("encode failed");
	// offset 29 is encoding byte for first column
	// msg(16) + frame(12) + type(1) = 29
	assert_eq!(encoded[29], Encoding::Plain as u8);
}

#[test]
fn compression_none_forces_plain() {
	let values: Vec<i32> = (1..=500).collect();
	let frame = Frame::new(vec![FrameColumn {
		name: "seq".to_string(),
		data: FrameColumnData::Int4(NumberContainer::new(values)),
	}]);
	let encoded = encode_frames(&[frame.clone()], &EncodeOptions::none()).expect("encode failed");
	// With CompressionLevel::None, data should be plain: 500 * 4 = 2000 bytes of data + overhead
	assert!(encoded.len() > 2000, "expected plain (no compression), got {} bytes", encoded.len());
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), 1);
	assert_frame_eq(&frame, &decoded[0]);
}

#[test]
fn compression_max_round_trip() {
	let values: Vec<i32> = (1..=500).collect();
	let frame = Frame::new(vec![FrameColumn {
		name: "seq".to_string(),
		data: FrameColumnData::Int4(NumberContainer::new(values)),
	}]);
	let encoded = encode_frames(&[frame.clone()], &EncodeOptions::max()).expect("encode failed");
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), 1);
	assert_frame_eq(&frame, &decoded[0]);
}
