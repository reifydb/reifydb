// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::json::{
	NONE_MARKER,
	from::{frames_from_envelope, frames_from_json},
	none_marker,
};
use reifydb_value::value::{Value, frame::frame::Frame};
use serde_json::{Value as JsonValue, json, to_string};

fn one_column(ty: JsonValue, payload: Vec<String>) -> JsonValue {
	json!([{ "columns": [{ "name": "a", "type": ty, "payload": payload }] }])
}

fn decode(json: JsonValue) -> Result<Vec<Frame>, String> {
	frames_from_json(&to_string(&json).unwrap()).map_err(|error| error.to_string())
}

#[test]
fn a_none_marker_in_a_non_option_column_is_a_decode_error_not_a_placeholder() {
	// A plain Int4 column has no none, so reading the marker as 0 invents a value the server never sent.
	let result = decode(one_column(json!({"id": "Int4"}), vec![NONE_MARKER.to_string()]));

	assert!(result.is_err(), "expected a decode error, got {result:?}");
}

#[test]
fn a_none_marker_deeper_than_the_option_column_is_a_decode_error_not_clamped() {
	// Clamping a marker the column cannot hold turns a malformed cell into an outer none without a trace.
	let ty = json!({"id": "Option", "underlying": {"id": "Int4"}});

	let result = decode(one_column(ty, vec![none_marker(1)]));

	assert!(result.is_err(), "expected a decode error, got {result:?}");
}

#[test]
fn an_unknown_frame_op_is_a_decode_error_not_an_absent_op() {
	// An absent op reads as a plain query frame, so an unknown op must not quietly lose the change kind.
	let mut json = one_column(json!({"id": "Int4"}), vec!["7".to_string()]);
	json[0]["op"] = json!(9);

	let result = decode(json).map(|frames| frames[0].op);

	assert!(result.is_err(), "expected a decode error, got {result:?}");
}

#[test]
fn an_envelope_without_a_frames_key_is_a_decode_error_not_zero_frames() {
	// Zero frames is a valid answer, so an envelope that lost its frames must never look like one.
	let result = frames_from_envelope(json!({ "rows": [] })).map(|frames| frames.len());

	assert!(result.is_err(), "expected a decode error, got {result:?}");
}

#[test]
fn an_envelope_that_is_not_an_object_is_a_decode_error_not_zero_frames() {
	// A bare list or string body is not the frames format, so it must not decode as an empty result.
	for body in [json!([]), json!("frames"), JsonValue::Null] {
		let result = frames_from_envelope(body.clone()).map(|frames| frames.len());

		assert!(result.is_err(), "expected a decode error for {body}, got {result:?}");
	}
}

#[test]
fn a_decode_error_names_the_column_row_type_and_a_truncated_cell() {
	// Without the column, row and type the caller cannot find the bad cell; an untruncated cell floods the log.
	let long = "x".repeat(10_000);

	let error = decode(one_column(json!({"id": "Int4"}), vec!["7".to_string(), long.clone()])).unwrap_err();

	assert!(error.contains("'a'"), "column missing: {error}");
	assert!(error.contains("row 1"), "row missing: {error}");
	assert!(error.contains("Int4"), "type missing: {error}");
	assert!(error.len() < 400, "cell not truncated, {} bytes", error.len());
}

#[test]
fn a_system_timestamp_error_names_the_system_column_and_row() {
	// A user column may be called created_at, so the error must name the system column unambiguously.
	let mut json = one_column(json!({"id": "Int4"}), vec!["7".to_string()]);
	json[0]["updated_at"] = json!(["not a datetime"]);

	let error = decode(json).unwrap_err();

	assert!(error.contains("#updated_at") && error.contains("row 0"), "{error}");
}

#[test]
fn a_bare_digest_column_still_reads_an_empty_slot_marker_as_none() {
	// RBCF keeps empty slots in a bare digest column, so rejecting the JSON marker splits the formats.
	let ty = json!({"id": "Digest", "underlying": {"id": "Float8"}, "accuracy": 10000});

	let frames = decode(one_column(ty, vec![NONE_MARKER.to_string()])).expect("an empty digest slot must decode");

	assert!(matches!(frames[0].columns[0].data.get_value(0), Value::None { .. }));
}
