// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::json::{from::frames_from_json, to::convert_frames};
use reifydb_value::{
	util::bitvec::BitVec,
	value::{
		Value,
		container::{number::NumberContainer, utf8::Utf8Container},
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		value_type::ValueType,
	},
};
use serde_json::{json, to_string, to_value};

fn option(inner: FrameColumnData, defined: &[bool]) -> FrameColumnData {
	FrameColumnData::Option {
		inner: Box::new(inner),
		bitvec: BitVec::from_slice(defined),
	}
}

fn int4(values: Vec<i32>) -> FrameColumnData {
	FrameColumnData::Int4(NumberContainer::new(values))
}

fn frame(data: FrameColumnData) -> Frame {
	Frame::new(vec![FrameColumn {
		name: "c".to_string(),
		data,
	}])
}

fn layers(data: &FrameColumnData) -> Vec<Vec<bool>> {
	let mut out = Vec::new();
	let mut cur = data;
	while let FrameColumnData::Option {
		inner,
		bitvec,
	} = cur
	{
		out.push(bitvec.to_vec());
		cur = inner;
	}
	out
}

fn decode_single(json: &str) -> FrameColumnData {
	let mut frames = frames_from_json(json).expect("from_json failed");
	assert_eq!(frames.len(), 1);
	frames.remove(0).columns.remove(0).data
}

#[test]
fn option_int4_writes_the_bare_marker_at_the_outer_layer() {
	let response = convert_frames(&[frame(option(int4(vec![1, 0, 3]), &[true, false, true]))]);
	let column = &response[0].columns[0];
	assert_eq!(to_value(&column.r#type).unwrap(), json!({"id": "Option", "underlying": {"id": "Int4"}}));
	assert_eq!(column.payload, vec!["1", "\u{27EA}none\u{27EB}", "3"]);

	let decoded = decode_single(&to_string(&response).unwrap());
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert_eq!(layers(&decoded), vec![vec![true, false, true]]);
	assert_eq!(decoded.get_value(0), Value::Int4(1));
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Int4));
	assert_eq!(decoded.get_value(2), Value::Int4(3));
}

#[test]
fn option_option_int4_names_the_some_layers_above_each_none() {
	let column = option(option(int4(vec![0, 0, 7]), &[false, false, true]), &[false, true, true]);
	let response = convert_frames(&[frame(column)]);
	let column = &response[0].columns[0];
	assert_eq!(
		to_value(&column.r#type).unwrap(),
		json!({"id": "Option", "underlying": {"id": "Option", "underlying": {"id": "Int4"}}})
	);
	assert_eq!(column.payload, vec!["\u{27EA}none\u{27EB}", "\u{27EA}none:1\u{27EB}", "7"]);

	let decoded = decode_single(&to_string(&response).unwrap());
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Int4)))));
	assert_eq!(layers(&decoded), vec![vec![false, true, true], vec![false, false, true]]);
	assert_eq!(decoded.get_value(0), Value::none_of(ValueType::Option(Box::new(ValueType::Int4))));
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Int4));
	assert_eq!(decoded.get_value(2), Value::Int4(7));
}

#[test]
fn hand_written_markers_parse_into_one_bitmap_per_layer() {
	let json = r#"[{"columns":[{"name":"c","type":{"id":"Option","underlying":{"id":"Option","underlying":{"id":"Int4"}}},"payload":["⟪none⟫","⟪none:1⟫","7","⟪none⟫"]}]}]"#;
	let decoded = decode_single(json);
	assert_eq!(layers(&decoded), vec![vec![false, true, true, false], vec![false, false, true, false]]);
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Int4));
	assert_eq!(decoded.get_value(2), Value::Int4(7));
}

#[test]
fn a_marker_deeper_than_the_column_stays_a_none_at_the_innermost_layer() {
	let json = r#"[{"columns":[{"name":"c","type":{"id":"Option","underlying":{"id":"Int4"}},"payload":["⟪none:3⟫","5"]}]}]"#;
	let decoded = decode_single(json);
	assert_eq!(layers(&decoded), vec![vec![false, true]]);
	assert_eq!(decoded.get_value(0), Value::none_of(ValueType::Int4));
	assert_eq!(decoded.get_value(1), Value::Int4(5));
}

#[test]
fn a_fully_populated_option_column_keeps_its_option_type() {
	let response = convert_frames(&[frame(option(int4(vec![1, 2, 3]), &[true, true, true]))]);
	assert_eq!(response[0].columns[0].payload, vec!["1", "2", "3"]);

	let decoded = decode_single(&to_string(&response).unwrap());
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert_eq!(layers(&decoded), vec![vec![true, true, true]]);

	let nested = option(option(int4(vec![1, 2]), &[true, true]), &[true, true]);
	let decoded = decode_single(&to_string(&convert_frames(&[frame(nested)])).unwrap());
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Int4)))));
	assert_eq!(layers(&decoded), vec![vec![true, true], vec![true, true]]);
}

#[test]
fn option_option_utf8_round_trips_every_state() {
	let strings = FrameColumnData::Utf8(Utf8Container::new(vec![
		String::new(),
		String::new(),
		"seven".to_string(),
		"\u{27EA}none\u{27EB}".to_string(),
	]));
	let column = option(option(strings, &[false, false, true, true]), &[false, true, true, true]);
	let response = convert_frames(&[frame(column)]);
	let column = &response[0].columns[0];
	assert_eq!(
		to_value(&column.r#type).unwrap(),
		json!({"id": "Option", "underlying": {"id": "Option", "underlying": {"id": "Utf8"}}})
	);
	assert_eq!(
		column.payload,
		vec!["\u{27EA}none\u{27EB}", "\u{27EA}none:1\u{27EB}", "seven", "\u{27EA}none\u{27EB}"]
	);

	let decoded = decode_single(&to_string(&response).unwrap());
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Utf8)))));
	assert_eq!(decoded.get_value(0), Value::none_of(ValueType::Option(Box::new(ValueType::Utf8))));
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Utf8));
	assert_eq!(decoded.get_value(2), Value::Utf8("seven".to_string()));
	// a string that spells the marker is indistinguishable from a none on the wire
	assert_eq!(decoded.get_value(3), Value::none_of(ValueType::Option(Box::new(ValueType::Utf8))));
}
