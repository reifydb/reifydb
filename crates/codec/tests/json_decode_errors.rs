// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::catch_unwind;

use reifydb_codec::json::{
	from::{frames_from_envelope, frames_from_json},
	to::frames_to_json,
};
use reifydb_value::value::{
	Value,
	container::{digest::DigestContainer, number::NumberContainer},
	datetime::DateTime,
	digest::Digest,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	row_number::RowNumber,
	system_columns::SystemColumns,
	value_type::ValueType,
};
use serde_json::{Value as JsonValue, from_str, json, to_string};

fn digest_frame() -> Frame {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	digest.add_value(&Value::float8(1.0)).unwrap();
	let mut container = DigestContainer::with_capacity(1);
	container.push(Box::new(digest));
	Frame::new(vec![FrameColumn {
		name: "d".to_string(),
		data: FrameColumnData::Digest {
			container,
			inner: ValueType::Float8,
			accuracy: 10_000,
		},
	}])
}

fn int4_frame() -> Frame {
	Frame::new(vec![FrameColumn {
		name: "a".to_string(),
		data: FrameColumnData::Int4(NumberContainer::new(vec![7])),
	}])
}

fn with_first_cell(frame: Frame, cell: &str) -> String {
	let mut json: JsonValue = from_str(&frames_to_json(&[frame]).unwrap()).unwrap();
	json[0]["columns"][0]["payload"][0] = JsonValue::String(cell.to_string());
	to_string(&json).unwrap()
}

#[test]
fn a_digest_cell_that_is_not_a_digest_is_a_decode_error_not_a_panic() {
	// The decoder already returns a Result, so a bad server payload must come back through it.
	let json = with_first_cell(digest_frame(), "0xzz");

	let result = catch_unwind(|| frames_from_json(&json).map(|frames| frames.len()));

	assert!(matches!(result, Ok(Err(_))), "expected a decode error, got {result:?}");
}

#[test]
fn a_scalar_cell_that_does_not_parse_is_a_decode_error_not_a_default_value() {
	// Replacing an unparsable int4 with 0 hands the caller a value the server never sent.
	let json = with_first_cell(int4_frame(), "not a number");

	let result = frames_from_json(&json).map(|frames| frames[0].columns[0].data.get_value(0));

	assert!(result.is_err(), "expected a decode error, got {result:?}");
}

#[test]
fn a_system_timestamp_that_does_not_parse_is_a_decode_error_not_a_dropped_entry() {
	// Dropping one created_at entry shifts every later timestamp onto the wrong row.
	let mut frame = int4_frame();
	let at = DateTime::from_nanos(1_000);
	frame.system = SystemColumns::new(vec![RowNumber(1)], Vec::new(), vec![at], vec![at], vec![at], Vec::new());
	let mut json: JsonValue = from_str(&frames_to_json(&[frame]).unwrap()).unwrap();
	json[0]["created_at"][0] = JsonValue::String("not a datetime".to_string());

	let result = frames_from_json(&to_string(&json).unwrap()).map(|frames| frames[0].system.created_at().len());

	assert!(result.is_err(), "expected a decode error, got {result:?}");
}

#[test]
fn a_malformed_frames_envelope_is_not_read_as_zero_frames() {
	// An empty result is a valid answer, so a payload that fails to decode must never look like one.
	let body = json!({ "frames": [{ "columns": "not a column list" }] });

	let result = frames_from_envelope(body).map(|frames| frames.len());

	assert!(result.is_err(), "expected a decode error, got {result:?}");
}
