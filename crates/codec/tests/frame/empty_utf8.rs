// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::LargeStringArray;
use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions};
use reifydb_value::value::{Value, frame::frame::Frame, value_type::ValueType};

use crate::common::{ColumnData, data, frame_of, optional, view_at};

// An empty string is a value, not an absent one. The binary frame carries presence in the none bitmap,
// so a Some("") and a None must come back apart however the column was encoded.

fn column(values: Vec<&str>, defined: &[bool]) -> ColumnData {
	optional(
		data(
			ValueType::Utf8,
			LargeStringArray::from(values.into_iter().map(|v| v.to_string()).collect::<Vec<String>>()),
		),
		defined,
	)
}

fn round_trip(input: ColumnData, options: &EncodeOptions) -> Frame {
	let frame = frame_of(vec![("value", input)]);
	let bytes = encode_frames(&[frame], options).expect("encode failed");
	let mut frames = decode_frames(&bytes).expect("decode failed");
	assert_eq!(frames.len(), 1);
	frames.remove(0)
}

#[test]
fn an_empty_string_and_a_none_stay_apart_through_a_round_trip() {
	let frame = round_trip(column(vec!["", ""], &[true, false]), &EncodeOptions::none());
	let decoded = view_at(&frame, 0);

	assert_eq!(decoded.get_value(0), Value::Utf8(String::new()));
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Utf8));
}

#[test]
fn an_empty_string_and_a_none_stay_apart_under_the_fast_encoding() {
	// The fast options let the encoder pick dictionary or run-length coding, where an empty string and
	// an absent value can share a slot unless presence is carried separately.
	let frame = round_trip(column(vec!["", "", "a"], &[true, false, true]), &EncodeOptions::fast());
	let decoded = view_at(&frame, 0);

	assert_eq!(decoded.get_value(0), Value::Utf8(String::new()));
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Utf8));
	assert_eq!(decoded.get_value(2), Value::Utf8("a".to_string()));
}

#[test]
fn a_column_of_only_empty_strings_carries_no_none() {
	let frame = round_trip(column(vec!["", ""], &[true, true]), &EncodeOptions::none());
	let decoded = view_at(&frame, 0);

	assert_eq!(decoded.get_value(0), Value::Utf8(String::new()));
	assert_eq!(decoded.get_value(1), Value::Utf8(String::new()));
}
