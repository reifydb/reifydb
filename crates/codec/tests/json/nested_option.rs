// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::Schema;
use reifydb_codec::json::{from::frames_from_json, to::convert_frames};
use reifydb_value::value::{
	Value,
	column_view::ColumnView,
	frame::frame::Frame,
	value_type::{ValueType, field::named},
};
use serde_json::{json, to_string, to_value};

fn option_int4(values: Vec<Option<i32>>) -> Frame {
	let (field, array) =
		named("c", ValueType::Option(Box::new(ValueType::Int4)).into(), Arc::new(Int32Array::from(values)));
	Frame::from(RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap())
}

fn layers(view: &ColumnView<'_>) -> Vec<Vec<bool>> {
	match view.is_nullable() {
		true => vec![(0..view.len()).map(|i| !view.none_at(i)).collect()],
		false => Vec::new(),
	}
}

fn decode_single(json: &str) -> Frame {
	let mut frames = frames_from_json(json).expect("from_json failed");
	assert_eq!(frames.len(), 1);
	frames.remove(0)
}

fn view(frame: &Frame) -> ColumnView<'_> {
	ColumnView::try_from((frame.batch.column(0), frame.batch.schema_ref().field(0))).unwrap()
}

fn assert_depth_two_rejected(json: &str) {
	let message =
		frames_from_json(json).map(|frames| frames.len()).expect_err("depth two must not decode").to_string();
	assert!(message.contains("has 2 option layers, but a column holds at most one"), "unexpected error: {message}");
}

#[test]
fn option_int4_writes_the_bare_marker_at_the_outer_layer() {
	let response = convert_frames(&[option_int4(vec![Some(1), None, Some(3)])]).unwrap();
	let column = &response[0].columns[0];
	assert_eq!(to_value(&column.r#type).unwrap(), json!({"id": "Option", "underlying": {"id": "Int4"}}));
	assert_eq!(column.payload, vec!["1", "\u{27EA}none\u{27EB}", "3"]);

	let frame = decode_single(&to_string(&response).unwrap());
	let decoded = view(&frame);
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert_eq!(layers(&decoded), vec![vec![true, false, true]]);
	assert_eq!(decoded.get_value(0), Value::Int4(1));
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Int4));
	assert_eq!(decoded.get_value(2), Value::Int4(3));
}

#[test]
fn option_option_int4_markers_are_rejected() {
	// A column holds at most one option layer, so depth two markers must never decode.
	let json = r#"[{"columns":[{"name":"c","type":{"id":"Option","underlying":{"id":"Option","underlying":{"id":"Int4"}}},"payload":["⟪none⟫","⟪none:1⟫","7"]}]}]"#;
	assert_depth_two_rejected(json);
}

#[test]
fn hand_written_depth_two_markers_are_rejected() {
	// A second option layer must never decode, since no column can hold that nesting.
	let json = r#"[{"columns":[{"name":"c","type":{"id":"Option","underlying":{"id":"Option","underlying":{"id":"Int4"}}},"payload":["⟪none⟫","⟪none:1⟫","7","⟪none⟫"]}]}]"#;
	assert_depth_two_rejected(json);
}

#[test]
fn a_marker_deeper_than_the_column_is_a_decode_error() {
	// Clamping a deeper marker to the innermost none would hand back a value the server never sent.
	let json = r#"[{"columns":[{"name":"c","type":{"id":"Option","underlying":{"id":"Int4"}},"payload":["⟪none:3⟫","5"]}]}]"#;
	let result = frames_from_json(json).map(|frames| frames.len());
	let message = result.expect_err("a marker deeper than the column must not decode").to_string();
	assert!(
		message.contains("none marker depth 3 exceeds the 1 Option layers of type Option(Int4)"),
		"unexpected error: {message}"
	);
	assert!(
		message.contains("column 'c'") && message.contains("at row 0"),
		"error must name column c row 0: {message}"
	);
}

#[test]
fn a_fully_populated_option_column_keeps_its_option_type() {
	let response = convert_frames(&[option_int4(vec![Some(1), Some(2), Some(3)])]).unwrap();
	assert_eq!(response[0].columns[0].payload, vec!["1", "2", "3"]);

	let frame = decode_single(&to_string(&response).unwrap());
	let decoded = view(&frame);
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert_eq!(layers(&decoded), vec![vec![true, true, true]]);

	// A fully populated depth two column must be rejected too, never flattened to one layer.
	let nested = r#"[{"columns":[{"name":"c","type":{"id":"Option","underlying":{"id":"Option","underlying":{"id":"Int4"}}},"payload":["1","2"]}]}]"#;
	assert_depth_two_rejected(nested);
}

#[test]
fn option_option_utf8_is_rejected_in_every_state() {
	// A column holds at most one option layer, so every depth two utf8 state must fail to decode.
	let json = r#"[{"columns":[{"name":"c","type":{"id":"Option","underlying":{"id":"Option","underlying":{"id":"Utf8"}}},"payload":["⟪none⟫","⟪none:1⟫","seven","⟪none⟫"]}]}]"#;
	assert_depth_two_rejected(json);
}
