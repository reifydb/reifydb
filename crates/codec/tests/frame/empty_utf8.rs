// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions};
use reifydb_value::{
	util::bitvec::BitVec,
	value::{
		Value,
		container::utf8::Utf8Container,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		value_type::ValueType,
	},
};

// An empty string is a value, not an absent one. The binary frame carries presence in the none bitmap,
// so a Some("") and a None must come back apart however the column was encoded.

fn column(values: Vec<&str>, defined: &[bool]) -> FrameColumnData {
	FrameColumnData::Option {
		inner: Box::new(FrameColumnData::Utf8(Utf8Container::new(
			values.into_iter().map(|v| v.to_string()).collect(),
		))),
		bitvec: BitVec::from_slice(defined),
	}
}

fn round_trip(data: FrameColumnData, options: &EncodeOptions) -> FrameColumnData {
	let frame = Frame::new(vec![FrameColumn {
		name: "value".to_string(),
		data,
	}]);
	let bytes = encode_frames(&[frame], options).expect("encode failed");
	let mut frames = decode_frames(&bytes).expect("decode failed");
	assert_eq!(frames.len(), 1);
	frames.remove(0).columns.remove(0).data
}

#[test]
fn an_empty_string_and_a_none_stay_apart_through_a_round_trip() {
	let decoded = round_trip(column(vec!["", ""], &[true, false]), &EncodeOptions::none());

	assert_eq!(decoded.get_value(0), Value::Utf8(String::new()));
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Utf8));
}

#[test]
fn an_empty_string_and_a_none_stay_apart_under_the_fast_encoding() {
	// The fast options let the encoder pick dictionary or run-length coding, where an empty string and
	// an absent value can share a slot unless presence is carried separately.
	let decoded = round_trip(column(vec!["", "", "a"], &[true, false, true]), &EncodeOptions::fast());

	assert_eq!(decoded.get_value(0), Value::Utf8(String::new()));
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Utf8));
	assert_eq!(decoded.get_value(2), Value::Utf8("a".to_string()));
}

#[test]
fn a_column_of_only_empty_strings_carries_no_none() {
	let decoded = round_trip(column(vec!["", ""], &[true, true]), &EncodeOptions::none());

	assert_eq!(decoded.get_value(0), Value::Utf8(String::new()));
	assert_eq!(decoded.get_value(1), Value::Utf8(String::new()));
}
