// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::json::{from::parse_value, none_marker, to::convert_frames};
use reifydb_value::{
	util::bitvec::BitVec,
	value::{
		Value,
		container::utf8::Utf8Container,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		value_type::ValueType,
	},
};

// An empty string is a value. A column holding one must stay distinguishable from a column holding
// nothing, on the wire and back, or every empty cell silently reads as absent.

fn option_utf8() -> ValueType {
	ValueType::Option(Box::new(ValueType::Utf8))
}

fn frame_with(values: Vec<&str>, defined: &[bool]) -> Frame {
	Frame::new(vec![FrameColumn {
		name: "value".to_string(),
		data: FrameColumnData::Option {
			inner: Box::new(FrameColumnData::Utf8(Utf8Container::new(
				values.into_iter().map(|v| v.to_string()).collect(),
			))),
			bitvec: BitVec::from_slice(defined),
		},
	}])
}

#[test]
fn an_empty_payload_on_an_option_utf8_parses_as_the_empty_string() {
	assert_eq!(parse_value(&option_utf8(), ""), Ok(Value::Utf8(String::new())));
}

#[test]
fn only_the_none_marker_parses_as_none_on_an_option_utf8() {
	assert_eq!(parse_value(&option_utf8(), &none_marker(0)), Ok(Value::none_of(ValueType::Utf8)));
}

#[test]
fn an_empty_string_and_a_none_render_as_different_payloads() {
	let frames = convert_frames(&[frame_with(vec!["", ""], &[true, false])]);

	let payload = &frames[0].columns[0].payload;
	assert_eq!(payload[0], "", "a present empty string must render as an empty payload");
	assert_eq!(payload[1], none_marker(0), "an absent value must render as the none marker");
	assert_ne!(payload[0], payload[1]);
}

#[test]
fn an_empty_string_and_a_none_survive_the_render_and_parse_round_trip() {
	let frames = convert_frames(&[frame_with(vec!["", ""], &[true, false])]);
	let column = &frames[0].columns[0];

	assert_eq!(parse_value(&column.r#type.0, &column.payload[0]), Ok(Value::Utf8(String::new())));
	assert_eq!(parse_value(&column.r#type.0, &column.payload[1]), Ok(Value::none_of(ValueType::Utf8)));
}
