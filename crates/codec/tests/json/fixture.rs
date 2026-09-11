// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::json::{fixture::frames_from_fixture_json, from::frames_from_json};
use reifydb_value::value::{frame::data::FrameColumnData, value_type::ValueType};

fn fixture(ty: &str) -> String {
	format!(
		r#"[{{"columns":[{{"name":"val","type":{ty},"payload":["1"]}}],"row_numbers":[],"created_at":[],"updated_at":[]}}]"#
	)
}

#[test]
fn a_scalar_in_the_fixture_notation_reads_as_the_same_frame_as_the_wire_notation() {
	let from_fixture = frames_from_fixture_json(&fixture(r#""Int4""#)).expect("fixture notation must parse");
	let from_wire = frames_from_json(&fixture(r#"{"id":"Int4"}"#)).expect("wire notation must parse");
	assert_eq!(from_fixture, from_wire);
}

#[test]
fn an_option_in_the_fixture_notation_reads_as_the_same_frame_as_the_wire_notation() {
	let from_fixture =
		frames_from_fixture_json(&fixture(r#"{"Option":"Int4"}"#)).expect("fixture notation must parse");
	let from_wire = frames_from_json(&fixture(r#"{"id":"Option","underlying":{"id":"Int4"}}"#))
		.expect("wire notation must parse");
	assert_eq!(from_fixture, from_wire);
}

#[test]
fn the_fixture_notation_carries_the_option_layer_into_the_column_data() {
	// The notation is only a spelling: what it decodes to still has to be an optional column, or
	// the corpus would be pinning the wrong thing.
	let frames = frames_from_fixture_json(&fixture(r#"{"Option":"Int4"}"#)).expect("fixture notation must parse");
	let data = &frames[0].columns[0].data;
	assert_eq!(data.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert!(matches!(data, FrameColumnData::Option { .. }), "an Option type must decode into optional column data");
}

#[test]
fn the_fixture_reader_rejects_the_wire_descriptor() {
	// The two notations are deliberately separate: the corpus is anchored on ValueType's serde so
	// that it survives changes to the wire's rendering. A reader that accepted both would let the
	// corpus drift back onto the wire without anyone noticing.
	let err = frames_from_fixture_json(&fixture(r#"{"id":"Int4"}"#))
		.expect_err("the wire descriptor must not be readable as the fixture notation");
	assert!(err.to_string().contains("Int4") || err.to_string().contains("unknown"), "got: {err}");
}

#[test]
fn the_wire_reader_rejects_the_fixture_notation() {
	let err = frames_from_json(&fixture(r#""Int4""#))
		.expect_err("the fixture notation must not be readable as the wire descriptor");
	assert!(err.to_string().contains("expected a type descriptor object"), "got: {err}");
}
